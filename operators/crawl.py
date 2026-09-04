"""data_pipeline 抽取算子：CrawlStage 抓取 + PersistStage 落盘（自包含）。

CrawlStage 行契约：
- 读键：url
- 追加键：title、markdown（正文；失败认缺返回 None，不断链）

PersistStage 行契约：
- 读键：url、markdown、title?
- 动作：内容寻址落盘 state 下 pages/<sha前2>/<sha256(url)>.md +
  index.jsonl 幂等追加（复用 demiflow.collect.store 机制：原子写/
  跨进程锁/吸收式尾扫）；重复 URL 跳过（幂等续跑），落盘成功行继续流转。
"""

from __future__ import annotations

import hashlib
import os
import time

from demiflow.collect.crawl import PageCrawler
from demiflow.collect.store import AppendManifestStore
from demiflow.data.plan import StreamStage


class CrawlStage(StreamStage):
    """抓取算子：URL 行 → 页面正文行（Crawl4AI 进程内浏览器，惰性启动）。

    策略：concurrency 为页级并发（浏览器每页一 tab，4-8 为资源安全区）；
    代理显式传参（不读 env——编排启动即清代理残留）。浏览器经平台
    run_stages 退出期统一 aclose。"""

    label = "crawl"
    concurrency = 4

    def __init__(self, *, proxy=None, page_timeout: float = 40.0):
        self._proxy = proxy
        self._timeout = page_timeout
        self._crawler: PageCrawler | None = None

    async def __call__(self, row: dict):
        if self._crawler is None:
            self._crawler = PageCrawler(proxy=self._proxy,
                                        page_timeout=self._timeout)
            await self._crawler.__aenter__()
        page = await self._crawler.fetch(row["url"])
        if page is None:
            return None                  # 网络/渲染/超时认缺
        return {**row, "title": page["title"], "markdown": page["markdown"]}

    async def aclose(self):
        if self._crawler is not None:
            await self._crawler.__aexit__(None, None, None)
            self._crawler = None


class PersistStage(StreamStage):
    """落盘算子：正文行 → 内容寻址页文件 + index.jsonl 幂等追加。

    失败页（无 markdown）不写行——重跑自动重试（ok 集即续跑索引）。"""

    label = "persist"
    concurrency = 8

    def __init__(self, state_dir: str):
        self.root = state_dir
        os.makedirs(state_dir, exist_ok=True)
        self.store = AppendManifestStore(
            manifest=os.path.join(state_dir, "index.jsonl"),
            lock_path=os.path.join(state_dir, ".index.lock"))
        self.load_index()

    def load_index(self) -> int:
        return self.store.load_index(
            key_of=lambda rec: [(rec.get("sha"),)])

    def contains_url(self, url: str) -> bool:
        sha = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self.store.contains((sha,))

    async def __call__(self, row: dict):
        markdown = (row.get("markdown") or "").strip()
        if not markdown:
            return None
        url = row["url"]
        sha = hashlib.sha256(url.encode("utf-8")).hexdigest()
        rel = f"pages/{sha[:2]}/{sha}.md"
        record = {"url": url, "sha": sha, "status": "ok",
                  "title": row.get("title"), "path": rel,
                  "fetched_at": time.time()}
        done = await self.store.write(
            data=markdown.encode("utf-8"),
            blob_path=os.path.join(self.root, rel),
            key=(sha,), record=record)
        return row if done else None
