"""collect_v2/search.py 最小冒烟：SearchStage 全 mock 端到端（引擎网络
MockTransport），验证 dict 行契约、路由、每源切片、认缺语义。
运行：python3 -m data_pipeline.smoke_search
"""

from __future__ import annotations

import asyncio
import json

import httpx

from data_pipeline.operators import search
from demiflow.collect import net
from demiflow.collect.search import get_engine


async def main() -> None:
    net.RETRY_INTERVAL = 0.05

    def wikimedia_handler(req):
        assert "commons.wikimedia.org" in str(req.url)
        title = json.dumps({"query": {"pages": {
            "11": {"index": 1, "imageinfo": [{
                "url": "https://upload.wikimedia.org/w/a.png",
                "width": 800, "height": 600,
                "extmetadata": {
                    "LicenseShortName": {"value": "CC0"},
                    "Artist": {"value": "张三"}}}],
                "title": "File:a.png",
                "pageprops": {
                    "canonicalurl": "https://commons.wikimedia.org/wiki/File:a.png"}}
        }}}, ensure_ascii=False)
        return httpx.Response(200, text=title)

    def baidu_handler(req):
        items = []
        for i in range(3):
            items.append({"middleURL": f"https://t.bdimg.com/i{i}.jpg?w=640&h=480",
                          "thumbURL": f"https://t.bdimg.com/t{i}.jpg",
                          "width": 640, "height": 480})
        return httpx.Response(200, json={"data": items})

    def handler(req):
        url = str(req.url)
        if "wikimedia" in url:
            return wikimedia_handler(req)
        if "baidu" in url:
            return baidu_handler(req)
        return httpx.Response(404)

    mock = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    net.set_client(mock)
    net.set_client(mock, proxy=True)      # wikimedia 系走代理池，两池都注

    # 1) zh 种子：路由含 baidu/wikimedia_zh/...（其余源 404 认缺）
    stage = search.SearchStage(top_n=1, k=2)
    rows = await stage({"name": "慕田峪长城", "query": "慕田峪长城", "lang": "zh"})
    assert rows, "应至少有 baidu/wikimedia_zh 召回"
    by_src = {}
    for r in rows:
        by_src.setdefault(r["source"], []).append(r)
    assert "baidu" in by_src and len(by_src["baidu"]) == 1   # 每源 top_n 切片
    b = by_src["baidu"][0]
    assert b["tiers"][0].startswith("https://t.bdimg.com/i")
    assert b["name"] == "慕田峪长城" and b["query"] == "慕田峪长城"
    assert b["width"] == 640 and b["height"] == 480         # 声明尺寸取 URL 参数（口径）
    if "wikimedia_zh" in by_src:
        w = by_src["wikimedia_zh"][0]
        assert w["license"] == "CC0" and w["author"] == "张三"
    print(f"[PASS] zh 路由/每源切片/dict 行契约（源: {sorted(by_src)}）")

    # 2) latin 种子：路由含 wikimedia，不含 baidu
    rows = await stage({"name": "x", "query": "Great Wall", "lang": "latin"})
    srcs = {r["source"] for r in rows}
    assert "wikimedia" in srcs and "baidu" not in srcs
    assert all(r["query"] == "Great Wall" for r in rows)
    print("[PASS] latin 语言对位路由")

    # 3) 全源失败 → 认缺 None；引擎自声明限速已注册
    def dead(req):
        return httpx.Response(403)
    dead = httpx.AsyncClient(transport=httpx.MockTransport(dead))
    net.set_client(dead)
    net.set_client(dead, proxy=True)
    assert await stage({"name": "x", "query": "q", "lang": "latin"}) is None
    assert net.SOURCE_LIMITS["baidu"].rate == 10.0
    assert net.SOURCE_LIMITS["wikimedia"].proxy is True
    print("[PASS] 全源认缺与限速自声明")

    # 4) 网关 fail-fast 语义保留（引擎层件）
    assert get_engine("searxng").k_cap == search.K_SEMANTIC
    print("[PASS] 引擎注册表可达")
    await net.close_client()
    print("冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
