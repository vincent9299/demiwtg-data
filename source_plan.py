"""collect_v2 分支粒度 LLM 选源（2026-08-29 拍板：运行时动态决策按分支分组，
一组一次决策全组复用——逐实例决策没必要，分支级拿到九成价值）。

架构（仿视觉知识卡 search_kb 的「注册表+规划器+健康账本」三件套，粒度降一档）：
- IMG_REGISTRY：图片源注册表（只收 op_search 已有连接器的源；新源=探针验证后
  在此登记一格）；规划器不裸发明源 id，清单外 id 一律剔除（防幻觉）；
- BranchPlanner：分支 → 图源集合。LLM（glm-5.3-flash，Galaxy，key 只从
  modelhub/.env 读不落盘）一次决策，落盘缓存 state/collect/branch_routes_
  <lang>.json（含 note 供人工审计），同分支后续实例全部查表零 LLM；
  失败/空结果不落缓存、回退默认路由 FALLBACK_SOURCES，下次重判；
- 消费端：chain --source-plan（蕴含 --source-agent）。带 plan 时规划结果
  整体替换路由表输出（关键词条件路由与 deviantart 复挂不再叠加——两机制
  互斥，避免重复投递）；不带 plan 时维持关键词条件路由不变。

分组键 = 主分支（挂载路径的深度 2 祖先：根/域/二级；未挂载实例不规划，
走默认路由）。分支索引由 load_branch_index 从树现算（消费者现场聚合模式）。
"""

from __future__ import annotations

import asyncio
import json
import os
import re

# 图片源注册表：id → 规划器可读的能力描述（zh 源待中文链 adopt 时补入）
IMG_REGISTRY: dict[str, str] = {
    "wikimedia": "维基共享资源：真实世界实体（地标/生物/器物/人物/事件）的实拍与官方图，全类目通用",
    "bing_images": "必应图片搜索：全类目通用引擎，真实实体与概念均可用",
    "yandex_images": "Yandex 图片搜索：全类目通用引擎，欧美/俄语区内容强",
    "pixiv": "pixiv 插画社区：日系插画/同人图，虚拟角色与 ACG 强；真实实体照片弱",
    "deviantart": "DeviantArt 艺术社区：手绘/设计/艺术创作；真实实体照片弱（历史上半死源）",
    "anilist": "AniList：日本动画/漫画角色与作品官方图，仅 ACG 实体有效",
    "mal": "MyAnimeList：日本动画/漫画角色与作品官方图，仅 ACG 实体有效",
}

# 规划失败/空结果回退：默认 latin 路由（通用引擎保底）
FALLBACK_SOURCES = ("wikimedia", "bing_images", "yandex_images")

PLAN_SYSTEM = """你是图片检索源规划器。给定一个标签树分支（路径）与该分支下的实例名样例，从可用图源清单中选出对这个分支的实体值得查询的图源。

规则：
1. 每源请求有成本，只选真正可能命中的源（通常 1~4 个）。
2. 按分支实体类型选源：真实世界实体（地标/生物/器物/品牌/人物）优先通用引擎与维基共享；插画/同人价值高的分支（虚拟角色、ACG）可加 pixiv/anilist/mal；纯概念/抽象分支只留通用引擎。
3. 清单之外的源 id 不要写；不需要的源不要写。
只输出 JSON：{"sources": ["源id", ...], "note": "一句话理由"}"""

PLAN_USER_TPL = "分支：{branch}\n实例样例：{samples}\n可用图源清单：\n{registry}\n请规划。"

GLM_MODEL = "glm-5.3-flash"
GLM_MAX_TOKENS = 8192     # thinking 模型下限（项目记忆：reasoning 短输出 8192 够）
GLM_CONCURRENCY = 8
GLM_RETRIES = 3
GLM_429_PAUSE = 15.0      # kcard 实测：配额是账户级令牌桶，全局冷却优于单请求退避


def load_glm_conf() -> tuple:
    """GLM 接入参数只从 modelhub/.env 读（GLM_API_BASE/GLM_API_KEY），不落盘。"""
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), "modelhub", ".env")
    base = key = ""
    if os.path.exists(env_path):
        for line in open(env_path, encoding="utf-8"):
            if line.startswith("GLM_API_BASE="):
                base = line.strip().split("=", 1)[1]
            elif line.startswith("GLM_API_KEY="):
                key = line.strip().split("=", 1)[1]
    if not key:
        raise SystemExit("modelhub/.env 缺 GLM_API_KEY（--source-plan 需要）")
    return base, key


def load_branch_index(tree_path: str) -> tuple:
    """读树现算 (实例名 → 主分支路径, 分支 → 样例名列表)。

    主分支 = 挂载路径的深度 2 祖先（根/域/二级）；直接挂在更浅节点的实例
    以其自身路径为分支。一实例多挂取先遇者（树序稳定）。样例封顶 12 个
    供规划器窥斑见豹。纯内存不落盘。
    """
    doc = json.loads(open(tree_path, encoding="utf-8").read())
    inst_branch: dict[str, str] = {}

    def walk(node: dict, branch: str) -> None:
        br = node.get("path", "") if node.get("depth", 0) <= 2 else branch
        for inst in node.get("instances") or []:
            inst_branch.setdefault(inst, br)
        for ch in node.get("children") or []:
            walk(ch, br)

    tree = doc.get("tree") or {}
    if tree:
        walk(tree, "")
    samples: dict[str, list] = {}
    for inst, br in inst_branch.items():
        samples.setdefault(br, []).append(inst)
    return inst_branch, {br: v[:12] for br, v in samples.items()}


class BranchPlanner:
    """分支 → 图源集合的懒规划 + 落盘缓存。单事件循环内用，无锁。"""

    def __init__(self, cache_path: str, base: str, key: str):
        import httpx
        self.path = cache_path
        self.base = base
        self.key = key
        self.routes: dict[str, dict] = {}   # branch -> {"sources": [...], "note"}
        self._inflight: dict[str, asyncio.Future] = {}
        self._sem = asyncio.Semaphore(GLM_CONCURRENCY)
        self._pause_until = 0.0
        self._client = httpx.AsyncClient(
            trust_env=False,
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=10.0))

    # ------------------------------------------------------------------
    # 缓存
    # ------------------------------------------------------------------

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            doc = json.loads(open(self.path, encoding="utf-8").read())
        except (json.JSONDecodeError, OSError):
            return
        # 注册表收缩时剔除已失效 id；剔空的重判
        self.routes = {}
        for br, rec in (doc or {}).items():
            srcs = [s for s in (rec.get("sources") or [])
                    if s in IMG_REGISTRY]
            if srcs:
                self.routes[br] = {"sources": srcs, "note": rec.get("note", "")}

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.routes, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.path)

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------
    # 规划（缓存优先；同分支并发去重）
    # ------------------------------------------------------------------

    async def sources_for(self, branch: str, samples: list) -> tuple:
        """返回该分支的图源元组；缓存命中零 LLM。异常上抛由调用方回退。"""
        rec = self.routes.get(branch)
        if rec:
            return tuple(rec["sources"])
        fut = self._inflight.get(branch)
        if fut is None:
            fut = asyncio.get_running_loop().create_future()
            self._inflight[branch] = fut
            try:
                srcs = await self._plan(branch, samples)
            except Exception as exc:  # noqa: BLE001 - 规划失败回退，不打断链
                fut.set_exception(exc)
            else:
                fut.set_result(srcs)
            finally:
                self._inflight.pop(branch, None)
        return await asyncio.shield(fut) if not fut.done() else fut.result()

    async def _plan(self, branch: str, samples: list) -> tuple:
        registry_txt = "\n".join(f"- {sid}: {desc}"
                                 for sid, desc in IMG_REGISTRY.items())
        payload = {
            "model": GLM_MODEL,
            "max_tokens": GLM_MAX_TOKENS,
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": PLAN_SYSTEM},
                {"role": "user", "content": PLAN_USER_TPL.format(
                    branch=branch, samples="、".join(samples[:12]),
                    registry=registry_txt)},
            ],
        }
        headers = {"Authorization": f"Bearer {self.key}"}
        last_exc: Exception | None = None
        for attempt in range(GLM_RETRIES + 1):
            while asyncio.get_running_loop().time() < self._pause_until:
                await asyncio.sleep(1.0)
            async with self._sem:
                try:
                    r = await self._client.post(
                        f"{self.base}/chat/completions",
                        json=payload, headers=headers)
                    if r.status_code == 429:
                        self._pause_until = (asyncio.get_running_loop().time()
                                             + GLM_429_PAUSE)
                        last_exc = RuntimeError("GLM 429")
                        continue
                    r.raise_for_status()
                    content = r.json()["choices"][0]["message"]["content"] or ""
                    m = re.search(r"\{[^{}]*\"sources\"[^{}]*\}", content, re.S)
                    if not m:
                        last_exc = RuntimeError("规划输出无 JSON")
                        continue
                    picked = [s for s in json.loads(m.group(0)).get("sources")
                              or [] if s in IMG_REGISTRY]
                    picked = list(dict.fromkeys(picked))   # 去重保序
                    if not picked:
                        return FALLBACK_SOURCES           # 合理判空：回退不落缓存
                    note = json.loads(m.group(0)).get("note", "")
                    self.routes[branch] = {"sources": picked, "note": note}
                    self.save()
                    return tuple(picked)
                except Exception as exc:  # noqa: BLE001 - 网络/服务端错误重试
                    last_exc = exc
            if attempt < GLM_RETRIES:
                await asyncio.sleep(2.0)
        raise RuntimeError(f"分支规划重试用尽: {last_exc!r}")
