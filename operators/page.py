"""data_pipeline docs 线算子：页面抓取（图文一体）→ 内嵌图下载 → docs 落盘。

行契约：
- 页面候选行（读）：{name, page_url, title, authority, query}
- 页面产物行（PageFetchStage 产）：+ {page_sha, passages[]（段落+绑定图）,
  path}；markdown 原文落 pages/<aa>/<sha256(url)>.md（内容寻址，跨概念
  共享去重——抽取只做一次）
- docs 清单行（DocsSinkStage）：{page_sha, url, concepts, authority,
  title, path, n_passages, n_images, fetched_at}（分片单写者，与图像线
  同款幂等追加）
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time

from demiflow.collect import net
from demiflow.collect.crawl import PageCrawler
from demiflow.collect.fetch import fetch_tiers
from demiflow.collect.images import verify_image
from demiflow.collect.store import (AppendManifestStore,
                                    atomic_write_bytes)
from demiflow.data.plan import StreamStage

# 内嵌图下载闸（自声明：任意站点图，礼貌限速 + 代理归属与图像线同判——
# 这里按直连登记，受限网络由 env DEMIFLOW_PROXY_URL 兜底）
net.register_limits({
    "inline": net.SourceLimits(rate=4.0, concurrency=8),
    "dl:inline": net.SourceLimits(rate=6.0, concurrency=8),
})

# 段落切分：按标题/空行聚块，块目标 200-800 字（过长按句号硬切）
_SPLIT_RE = re.compile(r"\n\s*\n")
_MIN_PASSAGE = 120          # 短于此并入下一段
_MAX_PASSAGE = 900
_INLINE_IMG_RE = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)[^)]*\)")
_LINK_MD_RE = re.compile(r"\[[^\]]*\]\([^)]*\)")   # [text](url) 链接语法
_JUNK_RE = re.compile(
    r"\.(svg|ico)(\?|$)|/logo|/icon|/avatar|/spacer|/blank|badge|sprite",
    re.IGNORECASE)


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def extract_passages(markdown: str) -> list:
    """markdown → 段落集（含内嵌图绑定）。

    块内 `![alt](src)` 原位摘出为该段绑定图（文本里保留占位符），
    过滤明显垃圾图（logo/icon/svg/头像）；<MIN_PASSAGE 的块并入前段。
    """
    blocks = [b.strip() for b in _SPLIT_RE.split(markdown) if b.strip()]
    passages, buf = [], ""
    for b in blocks:
        buf = f"{buf}\n\n{b}" if buf else b
        if len(buf) >= _MIN_PASSAGE:
            passages.append(buf)
            buf = ""
    if buf:
        if passages and len(buf) < _MIN_PASSAGE:
            passages[-1] += "\n\n" + buf
        else:
            passages.append(buf)
    out = []
    for i, p in enumerate(passages):
        if len(p) > _MAX_PASSAGE:                     # 过长按句切两半
            mid = p.find("。", len(p) // 2)
            mid = mid if mid > 0 else len(p) // 2
            p = p[:mid + 1]
        images = []
        def _keep(m):
            src, alt = m.group(2), m.group(1)
            if not _JUNK_RE.search(src):
                images.append({"src": src, "alt": alt})
            return f"[图:{alt or '图'}]"
        text = _INLINE_IMG_RE.sub(_keep, p)
        # 链密度过滤（2026-09-06：导航栏/菜单链列表治理）——剥掉链接语法
        # 后的净文本占比 <35% 或链接数 >15 的块按导航垃圾丢弃
        plain = _LINK_MD_RE.sub("", text)
        plain = re.sub(r"\s+", "", plain)
        total = len(re.sub(r"\s+", "", text))
        n_links = len(_LINK_MD_RE.findall(text))
        if total > 0 and (len(plain) / total < 0.35 or n_links > 15):
            continue
        out.append({"id": f"p{len(out)}", "text": text.strip(),
                    "images": images})
    return out


_WIKI_URL_RE = re.compile(
    r"https://([a-z\-]+)\.wikipedia\.org/wiki/([^?#]+)")


async def _wiki_extract(url: str):
    """wikipedia 页直取纯文本（REST extracts，redirects=1 自动跟随重定向）。

    浏览器抓 wiki 页的两大痛点一并绕开：导航栏污染 markdown、重定向页
    内容壳。失败返回 None 回退浏览器路径。"""
    m = _WIKI_URL_RE.match(url)
    if not m:
        return None
    lang, title = m.group(1), m.group(2)
    from urllib.parse import unquote
    title = unquote(title)
    try:
        from operators.search import API_UA
        resp = await net.request(
            "wiki_entity", "GET",
            f"https://{lang}.wikipedia.org/w/api.php",
            params={"action": "query", "prop": "extracts",
                    "explaintext": "1", "redirects": "1", "format": "json",
                    "titles": title},
            headers={"User-Agent": API_UA})
        pages = (resp.json().get("query") or {}).get("pages") or {}
        for _, p in pages.items():
            extract = p.get("extract")
            if not extract or len(extract.strip()) <= 300:
                continue
            head = extract[:300]
            if ("may also refer to" in head or "可以指" in head
                    or "可以是指" in head):
                continue               # 消歧义页：列表壳非知识，丢弃
            return extract
    except Exception:  # noqa: BLE001 - wiki 直取失败回退浏览器
        pass
    return None


def quality_gate(passages: list):
    """共享质量门（在线采集与离线导入同款）。

    壳页判定双通道：有效段落 >=2；或单段但净文本 >=150 字（中文致密
    段落合并后常为一段——总文本量达标的单段知识页不误杀）。
    """
    if len(passages) >= 2:
        return passages
    if len(passages) == 1 and len(passages[0]["text"]) >= 150:
        return passages
    return None


class BaseIngestStage(StreamStage):
    """离线/外部文本导入算子：{concepts, title, url, text} 行 → docs 落盘行。

    与在线采集复用同一段抽取-过滤-质量门链路（extract_passages 的链密度
    过滤/垃圾图过滤 + quality_gate 壳页门）——base 层（wikipedia dump 等）
    与 delta 层（在线采集）经过完全相同的清洗，落到同一张 docs 清单、
    同一 pages/ 内容寻址池，authority 溯源区分来源。
    """

    label = "base_ingest"
    concurrency = 8

    def __init__(self, store_root: str):
        self.root = store_root

    async def __call__(self, row: dict):
        import hashlib as _h
        url = row.get("url") or f"offline:{row.get('title', '')}"
        sha = _h.sha256(url.encode("utf-8")).hexdigest()
        md_path = os.path.join(self.root, "pages", sha[:2], f"{sha}.md")
        if not os.path.exists(md_path):
            await asyncio.to_thread(
                atomic_write_bytes, md_path,
                row["text"].encode("utf-8"))
        passages = quality_gate(extract_passages(row["text"]))
        if passages is None:
            return None
        return {**row, "page_sha": sha,
                "passages": passages,
                "path": f"pages/{sha[:2]}/{sha}.md",
                "authority": row.get("authority", "offline-dump"),
                "n_images": sum(len(p["images"]) for p in passages)}


class PageFetchStage(StreamStage):
    """页面抓取算子（图文一体）：候选行 → 页面产物行。

    - 每概念页预算（默认 8，按权威度先到先得）；URL 内容寻址缓存：
      已抓页面直接复用（跨概念/跨轮零重复抓取）；
    - 页面正文 + 抽取段落缓存落共享存储（pages/ 与 pages-extract/），
      行内不携 markdown 只携 page_sha 与段落集；
    - 抓取失败认缺（None），单页失败不断链。
    """

    label = "pages"
    concurrency = 4            # 浏览器 tab 预算
    queue_depth = 8
    catch = ()

    def __init__(self, store_root: str, *, proxy=None,
                 max_pages_per_concept: int = 20, page_timeout: float = 40.0):
        self.root = store_root            # 共享存储数据集根（pages/ 落此）
        self._proxy = proxy
        self._max_pages = max_pages_per_concept
        self._timeout = page_timeout
        self._crawler: PageCrawler | None = None
        self._fetched: dict = {}          # {概念: 已抓页数}（本 run 内预算）
        self.pages = 0

    async def __call__(self, row: dict):
        concept = row["name"]
        if self._fetched.get(concept, 0) >= self._max_pages:
            return None                   # 概念页预算用尽
        url = row["page_url"]
        sha = _sha(url)
        md_path = os.path.join(self.root, "pages", sha[:2], f"{sha}.md")
        if os.path.exists(md_path):
            markdown = open(md_path, encoding="utf-8", errors="replace").read()
        else:
            wiki_md = await _wiki_extract(url)
            if wiki_md is not None:
                markdown = wiki_md        # wiki 直取纯文本（无导航栏）
            else:
                if self._crawler is None:
                    self._crawler = PageCrawler(proxy=self._proxy,
                                                page_timeout=self._timeout)
                    await self._crawler.__aenter__()
                page = await self._crawler.fetch(url)
                if page is None:
                    return None           # 抓取认缺
                markdown = page["markdown"]
                if page.get("title") and not row.get("title"):
                    row["title"] = page["title"]
            await asyncio.to_thread(atomic_write_bytes, md_path,
                                    markdown.encode("utf-8"))
            self.pages += 1
        passages = quality_gate(extract_passages(markdown))
        if passages is None:
            return None               # 壳页质量门：导航/空壳/登录墙拒收
        self._fetched[concept] = self._fetched.get(concept, 0) + 1
        n_imgs = sum(len(p["images"]) for p in passages)
        return {**row, "page_sha": sha,
                "passages": passages,
                "path": f"pages/{sha[:2]}/{sha}.md",
                "n_images": n_imgs}

    async def aclose(self):
        if self._crawler is not None:
            await self._crawler.__aexit__(None, None, None)
            self._crawler = None


class InlineImageStage(StreamStage):
    """内嵌图下载算子：段落绑定图 → blob（行内换 sha/path 引用）。

    单档下载（fetch_tiers 单元素）+ verify 解码；垃圾图（小于
    min_side 的图标/装饰）认缺剔除；行内图失败不断链（段落保留文本）。
    """

    label = "inline"
    concurrency = 8
    queue_depth = 16
    catch = (net.InfraError, __import__("httpx").HTTPError)

    def __init__(self, blob_root: str, *, min_side: int = 200,
                 referrer: str = ""):
        self.root = blob_root
        self._min_side = min_side
        self._referrer = referrer

    async def __call__(self, row: dict):
        for p in row.get("passages") or []:
            keep = []
            for img in p.get("images") or []:
                src = img["src"]
                if not src.startswith(("http://", "https://")):
                    continue
                headers = {"User-Agent": net.BROWSER_UA}
                if self._referrer:
                    headers["Referer"] = self._referrer
                try:
                    got = await fetch_tiers(
                        [src], source="inline", headers=headers,
                        verify=lambda d: (
                            {"sha": None, "w": 0} if False else _verify_min(
                                d, self._min_side)))
                except Exception:  # noqa: BLE001 - 单图失败认缺
                    continue
                if got is None:
                    continue
                rel = f"blobs/{got.sha256[:2]}/{got.sha256}.{got.extra['ext']}"
                await asyncio.to_thread(
                    atomic_write_bytes, os.path.join(self.root, rel), got.data)
                keep.append({**img, "sha256": got.sha256, "blob_path": rel})
            p["images"] = keep
        return row


def _verify_min(data: bytes, min_side: int):
    """verify 钩子：解码 + 最小边过滤（图标/装饰图剔除）。"""
    from demiflow.collect.images import verify_image
    m = verify_image(data)
    if m is None:
        return None
    if (m["width"] or 0) < min_side or (m["height"] or 0) < min_side:
        return None
    return m


class DocsSinkStage(StreamStage):
    """docs 清单落盘算子：页面产物行 → docs 分片清单幂等追加。

    键 (page_sha, concept)；同页跨概念为合法多行（docs↔concepts 多对多，
    与图像清单同款语义）。
    """

    label = "docs_sink"
    concurrency = 4

    def __init__(self, dataset_dir: str, manifest_name: str = "docs.jsonl"):
        self.root_dir = dataset_dir       # pages/ 与清单同根（清单本地分片，
        self.manifest = os.path.join(dataset_dir, "meta", manifest_name)  # 页面在共享根由调用方保证）
        os.makedirs(os.path.dirname(self.manifest), exist_ok=True)
        self._store = AppendManifestStore(
            manifest=self.manifest,
            lock_path=os.path.join(os.path.dirname(self.manifest),
                                   f".{manifest_name}.lock"))
        self._store.load_index(
            key_of=lambda rec: [(rec.get("page_sha"), c)
                                for c in rec.get("concepts") or [""]])
        self.sunk = 0

    async def __call__(self, row: dict):
        sha = row.get("page_sha")
        if not sha:
            return None
        record = {
            "page_sha": sha, "url": row.get("page_url"),
            "concepts": row.get("concepts") or [row["name"]],
            "authority": row.get("authority"),
            "title": row.get("title"), "path": row.get("path"),
            "n_passages": len(row.get("passages") or []),
            "n_images": sum(len(p.get("images") or [])
                            for p in row.get("passages") or []),
            "query": row.get("query"), "fetched_at": time.time(),
        }
        done = await self._store.write(
            data=b"", blob_path=os.path.join(self.root_dir, row.get("path") or ""),
            key=(sha, ",".join(row.get("concepts") or [row.get("name", "")])),
            record=record)
        if done:
            self.sunk += 1
            return row
        return None
