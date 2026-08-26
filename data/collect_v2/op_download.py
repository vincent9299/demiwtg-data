"""collect_v2 下载算子：输入 Item（检索产出）→ 在同一 Item 上追加下载产出字段。

契约（.qoder/handoff_collect_v2.md §3.2 + 用户拍板更新）：
- **下载算子不做过滤**（结构过滤、分辨率门均不做，用户拍板）；
  解码仅为提取实测元数据（宽高/mime/ext），拒收仅限「不是图」（解码失败），
  这不是质量筛选而是正确性验证；
- 候选 URL 轮转（2026-08-21 拍板）：对 item.content_urls（档位大到小）按序
  依次试，首个成功即停，获胜链接记回 item.content_url；确定性失败
  （404/403 等，典型为档位不存在）换下一候选；非图/超限拒收不轮转
  （同一逻辑图的各档位内容一致，一档不是图其余也不必试）；
- 字节封顶 MAX_BYTES，流式读取，超限按拒收（认缺，不重试）；
- 不碰盘：blobs 落盘是 op_sink 的唯一落点，本算子只产字节；
- 数据算子流：输入输出都是 Item，追加 content_url/data/sha256/ext/
  actual_width/actual_height/size_bytes 字段，不改写上游字段；
- 下载头按源登记（baidu 防盗链 Referer；wikimedia 系 bot UA 带真实联系方式，
  实测 example.com 占位会被 robot policy 拦截 403）；
- 失败语义沿用 infra：候选全败后，瞬态用尽 TransientExhaustedError 上抛
  由链层认缺；无候选/非图/超限拒收返回 None（正常流转，不是异常）。
"""

from __future__ import annotations

import asyncio
import hashlib
import io
from typing import Optional

import httpx
from PIL import Image

from collect_v2 import infra
from collect_v2.op_search import API_UA, BROWSER_UA, Item

MAX_BYTES = 20 * 1024 * 1024    # 单图字节上限（用户拍板 20MB）
CHUNK = 64 * 1024
# 每请求硬超时（2026-08-22 连接复用三层防线第②层）：read=30s 只管单次读，
# 慢渗连接可以一直通过单读超时；包总时长封顶把永久阻塞降级成丢一张图。
# 超时取消内层任务，infra.stream 的 finally 关响应：未读完的连接销毁不入池。
DOWNLOAD_HARD_TIMEOUT = 90.0

# Pillow 解码格式 → mime / ext（与存量 images.jsonl 的 ext 口径一致，不带点）
_FORMAT_MIME = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "GIF": "image/gif",
    "WEBP": "image/webp",
    "BMP": "image/bmp",
    "TIFF": "image/tiff",
}
_MIME_EXT = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/webp": "webp",
    "image/bmp": "bmp",
    "image/tiff": "tiff",
}

# 下载头按源登记（纯业务经验来自旧系统 _reference/old_repo/collect/sources/）：
# - baidu CDN 有防盗链，必须带来源 Referer；
# - wikimedia 系按礼仪带可识别 API UA；
# - 未登记源用浏览器 UA 兜底。
DOWNLOAD_HEADERS: dict[str, dict] = {
    "baidu": {
        "User-Agent": BROWSER_UA,
        "Referer": "https://image.baidu.com/",
        "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
    },
    "wikimedia": {"User-Agent": API_UA},
    "wikimedia_zh": {"User-Agent": API_UA},
    # pixiv i.pximg.net 防盗链：必须带站内 Referer
    "pixiv": {
        "User-Agent": BROWSER_UA,
        "Referer": "https://www.pixiv.net/",
        "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
    },
    # 国内爬虫档三源：CDN 有防盗链，Referer 带各站搜索页（旧系统实证）
    "huaban_api": {
        "User-Agent": BROWSER_UA,
        "Referer": "https://huaban.com/",
        "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
    },
    "toutiao": {
        "User-Agent": BROWSER_UA,
        "Referer": "https://so.toutiao.com/",
        "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
    },
    "so360": {
        "User-Agent": BROWSER_UA,
        "Referer": "https://image.so.com/",
        "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
    },
}
_DEFAULT_HEADERS = {"User-Agent": BROWSER_UA, "Accept": "image/*,*/*;q=0.8"}


def _headers_for(source: str) -> dict:
    return DOWNLOAD_HEADERS.get(source, _DEFAULT_HEADERS)


def _decode(data: bytes) -> Optional[tuple]:
    """Pillow 完整解码：失败/截断返回 None；成功返回 (format, width, height)。
    解码是提取实测元数据的手段，不承担质量筛选职责。"""
    try:
        with Image.open(io.BytesIO(data)) as im:
            im.load()   # 强制全量解码，截断/损坏在此暴露
            return im.format, im.width, im.height
    except Exception:  # noqa: BLE001 - 解码失败一律按质量拒收
        return None


async def _fetch_one(source: str, url: str, *, client, max_bytes: int
                     ) -> Optional[bytes]:
    """拉单个 URL 的字节；超限返回 None，网络异常按 infra 分类上抛。

    闸门走独立的 dl:<源> 键（2026-08-21 提速定案）：图片下载与检索
    共用源闸门会互挤配额，下载打的是 CDN，与搜索端点是独立限流桶。"""
    buf = bytearray()
    capped = False
    async with infra.stream(
        f"dl:{source}", "GET", url,
        client=client, headers=_headers_for(source),
    ) as resp:
        async for chunk in resp.aiter_bytes(CHUNK):
            buf.extend(chunk)
            if len(buf) > max_bytes:
                capped = True
                break
    return None if capped else bytes(buf)


async def download(
    item: Item,
    *,
    client: Optional[httpx.AsyncClient] = None,
    max_bytes: int = MAX_BYTES,
) -> Optional[Item]:
    """下载单条 Item：按 content_urls 档位序依次试，首个成功即停，
    成功时在同一 Item 上追加下载产出字段（content_url 记获胜链接）并返回。
    返回 None = 无候选/非图/超限（链层继续下一条）；
    确定性失败（404/403）换下一候选；候选全败后瞬态用尽异常上抛（链层认缺）。"""
    if not item.content_urls:
        return None

    data: Optional[bytes] = None
    winner: Optional[str] = None
    fmt: Optional[str] = None
    width = height = 0
    for url in item.content_urls:
        try:
            got = await asyncio.wait_for(
                _fetch_one(item.source, url, client=client,
                           max_bytes=max_bytes),
                timeout=DOWNLOAD_HARD_TIMEOUT)
        except infra.DeterministicError:
            continue    # 档位确定性不可得（404/403）：试下一档
        except asyncio.TimeoutError:
            continue    # 硬超时：该链接不可得（同档位确定性失败，换下一档）
        if got is None:
            return None     # 超字节上限：认缺，不轮转不重试
        decoded = _decode(got)
        if decoded is None:
            return None     # 解码失败 = 不是图：拒收（正确性验证，非质量筛选）
        data, winner = got, url
        fmt, width, height = decoded
        break
    if data is None:
        raise infra.DeterministicError(
            f"{item.source}: 全部 {len(item.content_urls)} 个候选链接确定性失败")

    # 追加下载产出字段（不改写上游字段）
    item.content_url = winner
    item.data = data
    item.sha256 = hashlib.sha256(data).hexdigest()
    item.mime = _FORMAT_MIME.get(fmt or "", "application/octet-stream")
    item.ext = _MIME_EXT.get(item.mime, "bin")
    item.actual_width = width
    item.actual_height = height
    item.size_bytes = len(data)
    return item
