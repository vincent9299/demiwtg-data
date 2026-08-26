"""collect_v2 基础设施层：按源限速、有界重试、并行控制。

契约（用户拍板，见 .qoder/handoff_collect_v2.md §2.3 / §4.5）：
- 按源限速，尽量快但避免被封：两档初值——官方 API 档 2 req/s 并发 2；
  爬虫档 0.5 req/s 并发 1；全局并发上限 8。
- 分类重试：确定性失败（400/401/403/404/410、域名非法）不重试直接抛出；
  瞬态失败（超时/连接重置/429/5xx）重试 3 次、固定间隔 1s，不做指数退避。
- 零业务逻辑：不出现 instance/候选/blobs 概念，只提供通用机制。

对外原语：
- request(source, method, url, ...)   限速 + 分类重试的 HTTP 请求
- stream(source, method, url, ...)    流式版 request（下载算子用，字节封顶在调用方）
- WorkPool(limit)                     全局工作池（并发任务数封顶）
- SourceGate / RateLimiter            供算子按源取用的限流原语
"""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx

# ---------------------------------------------------------------------------
# 配置（两档初值，跑起来按封禁反馈再调）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceLimits:
    rate: float        # 每秒请求数
    concurrency: int   # 该源最大在途请求数
    proxy: bool = False  # 是否走外网代理（2026-08-20 拍板：海外源走 109 代理，国内直连）


# 官方 API 档：尽量快，按文档/礼仪限制
_API_SOURCES = ("wikimedia", "wikimedia_zh", "inaturalist", "fandom",
                "anilist", "deviantart")
# 搜索爬虫档：反爬源，保守
_CRAWLER_SOURCES = ("baidu", "bing", "toutiao", "so360", "huaban_api",
                    "bing_images", "pixiv", "yandex_images", "mal")

# 外网代理（§7 网络约定：直连优先，直连不通才走代理；宕机旧代理禁用）
PROXY_URL = "http://192.168.10.109:10808"
# 必须走代理的源名单：仅收实测直连不通的（2026-08-22 拍板规范：外网源能直连就直连，
# 减少代理流量）。2026-08-22 实测：mal/bing_images/yandex_images 直连可通（走直连），
# wikimedia(_zh)/anilist/pixiv/deviantart 直连超时（留本名单）。
_PROXY_SOURCES = ("wikimedia", "wikimedia_zh", "inaturalist", "fandom",
                  "anilist", "deviantart", "pixiv")

SOURCE_LIMITS: dict[str, SourceLimits] = {
    **{s: SourceLimits(rate=2.0, concurrency=2) for s in _API_SOURCES},
    # 检索档沿革：1.0→2.0（2026-08-21 探测全源 200 无封禁）→4.0（2026-08-22 开闸换吞吐，
    # 全日志 0 条风控证据）→10.0（2026-08-22 用户拍板再开大；若出现按源封禁再降回并对该源上 IP 池）
    **{s: SourceLimits(rate=10.0, concurrency=16) for s in _CRAWLER_SOURCES},
}
# 下载独立闸门（2026-08-21 实测定案）：图片下载与检索共用源闸门时把检索
# 配额挤死（对照实验：纯检索 4.1 对/s → 加共享下载 1.3 对/s，链内同款
# 症状）。图片下载打的是 CDN，与搜索端点是独立限流桶，拆 dl:<源> 键
# 给宽预算，检索速率不再受下载量牵连。
SOURCE_LIMITS.update({
    **{f"dl:{s}": SourceLimits(rate=6.0, concurrency=8) for s in _API_SOURCES},
    # 下载档沿革：3.0→6.0（2026-08-22 开闸，风控证据 0）→15.0（2026-08-22 用户拍板再开大，
    # 并发同步放宽到 32 避免槽位成新约束；出现按源 403/429 即回退）
    **{f"dl:{s}": SourceLimits(rate=15.0, concurrency=32) for s in _CRAWLER_SOURCES},
})
for _s in _PROXY_SOURCES:
    for _key in (_s, f"dl:{_s}"):
        if _key in SOURCE_LIMITS:
            _lim = SOURCE_LIMITS[_key]
            SOURCE_LIMITS[_key] = SourceLimits(
                rate=_lim.rate, concurrency=_lim.concurrency, proxy=True)

GLOBAL_CONCURRENCY = 8   # 全局工作池并发上限
MAX_RETRIES = 3          # 瞬态失败重试次数（不含首次）
RETRY_INTERVAL = 1.0     # 重试固定间隔（秒），不做指数退避

DEFAULT_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=10.0)


# ---------------------------------------------------------------------------
# 异常与失败分类
# ---------------------------------------------------------------------------

class InfraError(Exception):
    """基础设施层异常基类。"""


class DeterministicError(InfraError):
    """确定性失败（403/404/域名非法等）：不重试，调用方认缺。"""


class TransientExhaustedError(InfraError):
    """瞬态失败且有界重试已用尽。"""


def classify_status(status: int) -> str:
    """HTTP 状态码分类：ok / transient / deterministic。"""
    if status < 400:
        return "ok"
    if status == 429 or status >= 500:
        return "transient"
    return "deterministic"


def _in_chain(exc: BaseException, target: type) -> bool:
    cur: Optional[BaseException] = exc
    while cur is not None:
        if isinstance(cur, target):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def classify_network_error(exc: Exception) -> Optional[str]:
    """网络异常分类：deterministic / transient；不认识返回 None（原样抛出）。"""
    if isinstance(exc, httpx.TimeoutException):
        return "transient"
    if isinstance(exc, httpx.ConnectError):
        # 域名解析失败 = 域名非法，确定性失败
        if _in_chain(exc, socket.gaierror):
            return "deterministic"
        return "transient"
    if isinstance(exc, httpx.NetworkError):
        return "transient"
    return None


# ---------------------------------------------------------------------------
# 限速原语
# ---------------------------------------------------------------------------

class RateLimiter:
    """最小间隔限速器：同源请求按 1/rate 秒最小间隔串行放行。"""

    def __init__(self, rate: float):
        self._interval = 1.0 / rate
        self._lock = asyncio.Lock()
        self._next_at = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            wait = self._next_at - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = loop.time()
            self._next_at = now + self._interval


class SourceGate:
    """每源闸门：并发信号量 + 限速器。slot() 内发请求。"""

    def __init__(self, limits: SourceLimits):
        self.limits = limits
        self._sem = asyncio.Semaphore(limits.concurrency)
        self._rl = RateLimiter(limits.rate)

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        async with self._sem:
            await self._rl.acquire()
            yield


_gates: dict[str, SourceGate] = {}


def gate_for(source: str) -> SourceGate:
    """按源名取闸门（惰性创建）。未登记源直接报错，不给默认限速。"""
    gate = _gates.get(source)
    if gate is None:
        limits = SOURCE_LIMITS.get(source)
        if limits is None:
            raise ValueError(f"源 {source!r} 未在限速表 SOURCE_LIMITS 登记")
        gate = _gates[source] = SourceGate(limits)
    return gate


# ---------------------------------------------------------------------------
# 全局工作池
# ---------------------------------------------------------------------------

class WorkPool:
    """全局工作池：在途任务总数封顶；submit 返回 task，join 等全部完成。"""

    def __init__(self, limit: int = GLOBAL_CONCURRENCY):
        self._sem = asyncio.Semaphore(limit)
        self._tasks: set[asyncio.Task] = set()

    def submit(self, coro) -> asyncio.Task:
        task = asyncio.create_task(self._guarded(coro))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _guarded(self, coro):
        async with self._sem:
            return await coro

    async def join(self) -> None:
        # 循环以覆盖等待期间新提交的任务
        while self._tasks:
            await asyncio.wait(list(self._tasks))


# ---------------------------------------------------------------------------
# 带限速与分类重试的 HTTP 请求
# ---------------------------------------------------------------------------

_client_direct: Optional[httpx.AsyncClient] = None
_client_proxy: Optional[httpx.AsyncClient] = None
_dl_client_direct: Optional[httpx.AsyncClient] = None
_dl_client_proxy: Optional[httpx.AsyncClient] = None

# 下载专用池参数（2026-08-22 拍板恢复连接复用，显式推翻 2026-08-21 keepalive=0 定案，
# 沿革见 get_download_client 文档串）；回退 = max_keepalive_connections 改回 0。
DOWNLOAD_LIMITS = httpx.Limits(max_connections=128, max_keepalive_connections=64)


def get_client(source: str = "") -> httpx.AsyncClient:
    """按源取进程级共享 HTTP 客户端（双池：直连 / 代理，惰性创建）。
    检索侧专用：下载侧（dl: 流量）另走 get_download_client。

    两池均 max_keepalive_connections=0（2026-08-21 定案）：三次夜跑卡死
    同一签名——半读状态的连接（CLOSE-WAIT 且接收缓冲有未读字节）被池
    保留，下次复用读流永久阻塞，全链路静默停摆。任务取消是半读连接
    的主要来源，无法从池层根治，故直接禁用复用：每次请求新连接，
    代价（每请求一次 TCP+TLS 握手）远小于停摆风险。

    冒烟可先 set_client 注入（无 proxy 需求的源注 direct 池，
    proxy 源注 proxy 池）。
    """
    global _client_direct, _client_proxy
    no_keepalive = httpx.Limits(max_keepalive_connections=0)
    base = source.removeprefix("dl:")   # 下载闸门键映射回源级代理归属
    need_proxy = bool(base) and (SOURCE_LIMITS.get(base) is not None
                                 and SOURCE_LIMITS[base].proxy)
    if need_proxy:
        if _client_proxy is None:
            _client_proxy = httpx.AsyncClient(
                timeout=DEFAULT_TIMEOUT, follow_redirects=True,
                proxy=PROXY_URL, limits=no_keepalive)
        return _client_proxy
    if _client_direct is None:
        _client_direct = httpx.AsyncClient(
            timeout=DEFAULT_TIMEOUT, follow_redirects=True,
            limits=no_keepalive)
    return _client_direct


def get_download_client(source: str) -> httpx.AsyncClient:
    """下载专用客户端（双池：直连 / 代理，惰性创建）：开启连接复用。

    沿革：2026-08-21 三次夜跑停摆后全链路禁复用（keepalive=0）；2026-08-22
    拍板仅下载侧恢复复用——病根（stream yield-in-retry 制造半读连接）已修，
    全链零任务取消源，停摆有 supervise 12 分钟自愈兜底，且下载打的是 CDN、
    每图一次全新 TCP+TLS 握手已成实测主瓶颈（py-spy 实锤）。
    配套三层防线：① 仅本池开复用、检索侧维持禁用；② 调用方（op_download）
    每请求硬超时 90s，超时取消任务经 stream 的 finally 关响应——没读完的
    响应关闭即销毁连接不入池；③ read=30s 读超时与看门狗不动。
    回退条件：任何停摆/风控迹象 → 上面 DOWNLOAD_LIMITS 的
    max_keepalive_connections 改回 0，重启即恢复旧行为。
    """
    global _dl_client_direct, _dl_client_proxy
    base = source.removeprefix("dl:")
    need_proxy = bool(base) and (SOURCE_LIMITS.get(base) is not None
                                 and SOURCE_LIMITS[base].proxy)
    if need_proxy:
        if _dl_client_proxy is None:
            _dl_client_proxy = httpx.AsyncClient(
                timeout=DEFAULT_TIMEOUT, follow_redirects=True,
                proxy=PROXY_URL, limits=DOWNLOAD_LIMITS)
        return _dl_client_proxy
    if _dl_client_direct is None:
        _dl_client_direct = httpx.AsyncClient(
            timeout=DEFAULT_TIMEOUT, follow_redirects=True,
            limits=DOWNLOAD_LIMITS)
    return _dl_client_direct


def set_client(client: httpx.AsyncClient, *, proxy: bool = False) -> None:
    global _client_direct, _client_proxy
    if proxy:
        _client_proxy = client
    else:
        _client_direct = client


async def close_client() -> None:
    global _client_direct, _client_proxy, _dl_client_direct, _dl_client_proxy
    for _c in (_client_direct, _client_proxy, _dl_client_direct, _dl_client_proxy):
        if _c is not None:
            await _c.aclose()
    _client_direct = _client_proxy = None
    _dl_client_direct = _dl_client_proxy = None


async def request(
    source: str,
    method: str,
    url: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    **kwargs,
) -> httpx.Response:
    """对 source 发一次受限速、带分类重试的请求，成功返回响应。

    - 确定性失败：抛 DeterministicError，不重试；
    - 瞬态失败：固定间隔重试 MAX_RETRIES 次后用尽抛 TransientExhaustedError；
    - 未识别异常：原样抛出，不归类。
    """
    gate = gate_for(source)
    http = client or get_client(source)
    last_exc: Optional[Exception] = None
    last_status: Optional[int] = None

    for attempt in range(MAX_RETRIES + 1):
        async with gate.slot():
            try:
                resp = await http.request(method, url, **kwargs)
            except Exception as exc:  # noqa: BLE001 - 需要分类后决定重试与否
                verdict = classify_network_error(exc)
                if verdict is None:
                    raise
                if verdict == "deterministic":
                    raise DeterministicError(f"{source} {url}: 域名/连接确定性失败") from exc
                last_exc, last_status = exc, None
            else:
                verdict = classify_status(resp.status_code)
                if verdict == "ok":
                    return resp
                if verdict == "deterministic":
                    raise DeterministicError(f"{source} {url}: HTTP {resp.status_code}")
                last_exc, last_status = None, resp.status_code
        # 瞬态失败：固定间隔后重试
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_INTERVAL)

    detail = f"HTTP {last_status}" if last_status else repr(last_exc)
    raise TransientExhaustedError(
        f"{source} {url}: 瞬态失败重试用尽（{MAX_RETRIES} 次）最后状态 {detail}"
    ) from last_exc


@asynccontextmanager
async def stream(
    source: str,
    method: str,
    url: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    **kwargs,
) -> AsyncIterator[httpx.Response]:
    """流式版 request：分类/重试只作用于建流与首包状态码。

    建流成功后把响应交给调用方流式读取（调用方负责字节封顶），
    读出阶段的网络异常原样上抛，不重试（下载重头再来代价大，认缺即可）。

    坑修（2026-08-21 夜跑实测）：yield 曾写在重试 for 循环内，调用方在读流
    阶段抛的异常会被 throw 进 yield 处，随后无守卫地进入下一轮重试，
    把读流错误转成 TransientExhaustedError 重抛——违反「读出阶段异常
    原样上抛」契约；且读流期 gate slot 早已释放，重试也无限速保护。
    现改为建流/重试循环与 yield 彻底分离。
    """
    gate = gate_for(source)
    # dl: 流量（下载专用，op_download 是唯一消费者）走复用池客户端；
    # 其余（仅冒烟可能）维持禁用复用的共享客户端。
    if client is None:
        client = (get_download_client(source) if source.startswith("dl:")
                  else get_client(source))
    http = client
    last_exc: Optional[Exception] = None
    last_status: Optional[int] = None
    resp: Optional[httpx.Response] = None

    for attempt in range(MAX_RETRIES + 1):
        async with gate.slot():
            try:
                req = http.build_request(method, url, **kwargs)
                resp = await http.send(req, stream=True)
            except Exception as exc:  # noqa: BLE001 - 需要分类后决定重试与否
                verdict = classify_network_error(exc)
                if verdict is None:
                    raise
                if verdict == "deterministic":
                    raise DeterministicError(f"{source} {url}: 域名/连接确定性失败") from exc
                last_exc, last_status = exc, None
                continue
            verdict = classify_status(resp.status_code)
            if verdict == "ok":
                break                       # 建流成功，跳出循环后交给调用方
            status = resp.status_code
            await resp.aclose()
            resp = None
            if verdict == "deterministic":
                raise DeterministicError(f"{source} {url}: HTTP {status}")
            last_exc, last_status = None, status
        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_INTERVAL)
    else:
        detail = f"HTTP {last_status}" if last_status else repr(last_exc)
        raise TransientExhaustedError(
            f"{source} {url}: 瞬态失败重试用尽（{MAX_RETRIES} 次）最后状态 {detail}"
        ) from last_exc

    # 建流成功：读流阶段异常原样上抛不重试（调用方认缺），finally 只关响应
    try:
        yield resp
    finally:
        await resp.aclose()
