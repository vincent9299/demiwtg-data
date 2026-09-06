"""docs 线最小冒烟：文本引擎检索→页面图文一体抽取→内嵌图下载→docs 落盘。
全 mock（检索/浏览器/下载）。运行：python3 -m smokes.docs
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile

import httpx
from PIL import Image

import flow
import operators.page as page_mod


def png(w: int, h: int) -> bytes:
    im = Image.new("RGB", (w, h), (7, 77, 177))
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()


PAGE_MD = """# 抛光不锈钢

抛光不锈钢是经机械/化学抛光处理的不锈钢材料，表面呈镜面效果，广泛用于
建筑装饰、厨具与雕塑。其耐腐蚀性与反光特性使它兼具工程与美学价值。

## 工艺

抛光分粗磨、中磨、精抛三步，依次用更细的磨料去除表面划痕。
![镜面效果示例](https://mock.cdn/mirror.png)
![logo图标](https://mock.cdn/logo.svg)

精抛后表面粗糙度可降至 Ra0.05 微米以下，形成接近镜面的反射。
![工艺流程图](https://mock.cdn/process.png?x=1)
"""

BYTES = {"https://mock.cdn/mirror.png": png(600, 400),
         "https://mock.cdn/process.png": png(500, 350)}


def install_mocks(tmp_share: str):
    from demiflow.collect import net
    net.RETRY_INTERVAL = 0.05
    net._gates.clear()
    net._client_direct = net._client_proxy = None
    net._dl_client_direct = net._dl_client_proxy = None

    def search_handler(req):
        url = str(req.url)
        if "wikipedia.org" in url:
            key = "Polished_stainless_steel"
            return httpx.Response(200, json={"pages": [
                {"key": key, "title": "Polished stainless steel",
                 "excerpt": "mirror-finish <b>steel</b>"}]})
        if "127.0.0.1:8080" in url:      # searxng general
            return httpx.Response(200, json={"results": [
                {"url": "https://mock.page/intro", "title": "抛光不锈钢介绍",
                 "content": "镜面不锈钢的工艺与用途"}]})
        return httpx.Response(404)

    mock = httpx.AsyncClient(transport=httpx.MockTransport(search_handler))
    net.set_client(mock)
    net.set_client(mock, proxy=True)
    dl = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(
            200, content=BYTES.get(str(req.url).split("?")[0]))
        if BYTES.get(str(req.url).split("?")[0]) else httpx.Response(404)))
    net.set_download_client(dl)
    net.set_download_client(dl, proxy=True)

    # 浏览器 mock：替换 PageCrawler（markdown + 内嵌图）
    class FakeCrawler:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def fetch(self, url):
            if "mock.page" not in url and "wikipedia" not in url:
                return None
            return {"url": url, "title": "页面标题",
                    "markdown": PAGE_MD, "images": []}

    page_mod.PageCrawler = FakeCrawler


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="smoke_docs_")
    try:
        share = os.path.join(tmp, "share")      # 共享根（pages/blobs）
        lake = os.path.join(tmp, "lake")        # 本地清单根
        doc = {"schema_version": "2.0.0", "concepts": [
            {"name": "抛光不锈钢",
             "aliases": ["polished stainless steel"],
             "carriers": "text"},               # text-only：只走 docs 线
            {"name": "黄水晶", "aliases": ["citrine"],
             "carriers": "image"},              # image-only：不走 docs 线
        ]}
        cpath = os.path.join(tmp, "batch.json")
        json.dump(doc, open(cpath, "w", encoding="utf-8"), ensure_ascii=False)
        install_mocks(share)
        sys.argv = ["flow", "--concepts", cpath, "--dataset", lake,
                    "--alias-cache", os.path.join(tmp, "a.json"),
                    "--blob-root", share, "--quota-passes", "1",
                    "--log-every", "100", "--vlm-concurrency", "2",
                    "--search-concurrency", "2", "--download-concurrency", "2",
                    "--instance-concurrency", "2"]
        flow.main()

        docs = os.path.join(lake, "meta", "docs.jsonl")
        rows = [json.loads(l) for l in open(docs, encoding="utf-8")]
        assert rows, "docs 线未落盘"
        assert {r["concepts"][0] for r in rows} == {"抛光不锈钢"}
        wiki_rows = [r for r in rows if r["authority"] == "wiki"]
        serp_rows = [r for r in rows if r["authority"] == "serp"]
        assert wiki_rows and serp_rows, rows
        assert all(r["n_passages"] >= 1 for r in rows)
        # 页面正文内容寻址落共享根
        for r in rows:
            p = os.path.join(share, r["path"])
            assert os.path.exists(p)
        print(f"[PASS] docs 线：{len(rows)} 页（wiki {len(wiki_rows)}/serp "
              f"{len(serp_rows)}），正文内容寻址落共享根")

        # 段落绑定 + 内嵌图（svg 垃圾图剔除、query 串剥离后下载成功 2 张）
        rec = rows[0]
        assert rec["n_images"] == 2, rec
        sha = rows[0]["page_sha"]
        page_file = os.path.join(share, "pages", sha[:2], f"{sha}.md")
        md = open(page_file, encoding="utf-8").read()
        ps = page_mod.extract_passages(md)
        bound = [i for p in ps for i in p["images"]]
        assert len(bound) == 2 and all(
            i["src"].split("?")[0] in BYTES for i in bound)
        blob_files = []
        for _, _, files in os.walk(os.path.join(share, "blobs")):
            blob_files += files
        assert len(blob_files) == 2, blob_files
        print("[PASS] 段落绑定 2 图（svg 垃圾剔除）、内嵌图下载落 blob")

        # 幂等：重跑 docs 线零追加
        install_mocks(share)
        flow.main()
        rows2 = [json.loads(l) for l in open(docs, encoding="utf-8")]
        assert len(rows2) == len(rows)
        print("[PASS] docs 幂等重跑零追加")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
