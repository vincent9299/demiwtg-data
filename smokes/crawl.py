"""data_pipeline/operators/crawl.py 最小冒烟：CrawlStage 真网抓取 +
PersistStage 落盘幂等。运行：python3 -m smokes.crawl
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import tempfile


async def main() -> None:
    try:
        import crawl4ai  # noqa: F401
    except ImportError:
        print("[SKIP] crawl4ai 未安装（pip install demiflow[crawl]）")
        return

    from operators.crawl import CrawlStage, PersistStage

    tmp = tempfile.mkdtemp(prefix="smoke_crawl_")
    try:
        # 1) CrawlStage 真网抓取（example.com 主、baidu 备，antibot 波动容错）
        stage = CrawlStage(page_timeout=30.0)
        try:
            row = await stage({"url": "https://example.com"})
            if row is None:
                row = await stage({"url": "https://www.baidu.com"})
        except Exception as exc:  # noqa: BLE001
            print(f"[SKIP] 浏览器不可用（{type(exc).__name__}: {exc}；"
                  f"运行 crawl4ai-setup 安装 playwright 浏览器）")
            return
        assert row is not None and row["markdown"].strip()
        assert row["url"] == "https://example.com" or "baidu" in row["url"]
        print(f"[PASS] CrawlStage 抓取（title={row['title']!r}，"
              f"正文 {len(row['markdown'])} 字符）")

        # 2) 认缺：不可达页面返回 None
        assert await stage({"url": "https://no-such-9x7q.invalid"}) is None
        print("[PASS] 不可达页面认缺")

        # 3) PersistStage：落盘 + 内容寻址 + 幂等
        persist = PersistStage(os.path.join(tmp, "crawl"))
        assert await persist(dict(row)) is not None
        assert await persist(dict(row)) is None            # 同 URL 幂等跳过
        sha = hashlib.sha256(row["url"].encode()).hexdigest()
        rec = json.loads(open(os.path.join(tmp, "crawl", "index.jsonl"),
                              encoding="utf-8").readline())
        assert rec["sha"] == sha and rec["status"] == "ok"
        page = os.path.join(tmp, "crawl", rec["path"])
        assert open(page, encoding="utf-8").read() == row["markdown"].strip()
        assert persist.contains_url(row["url"])
        print("[PASS] PersistStage 落盘/内容寻址/幂等续跑索引")

        # 4) 算子生命周期：aclose 可重入
        await stage.aclose()
        await stage.aclose()
        print("[PASS] aclose 可重入")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
