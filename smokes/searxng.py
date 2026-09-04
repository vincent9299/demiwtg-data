"""collect_v2/search.py SearxngEngine 最小冒烟：dict 结果契约、参数对位、
fail-fast。运行：python3 -m smoke_searxng
"""

from __future__ import annotations

import asyncio

import httpx

from operators import search
from demiflow.collect import net


async def main() -> None:
    net.RETRY_INTERVAL = 0.05
    eng = search.SearxngEngine()

    # 1) zh 参数对位 + tiers 档位 + native.engine 溯源
    payload = {"results": [
        {"img_src": "https://cdn.example/a_full.jpg",
         "thumbnail_src": "https://cdn.example/a_thumb.jpg",
         "url": "https://page.example/a", "engine": "bing images",
         "title": "甲", "resolution": "1920 x 1080"},
        {"img_src": "https://cdn.example/b.png",      # 无缩图单档
         "url": "https://page.example/b", "engine": "google cse images"},
        {"url": "https://page.example/c"},            # 无 img_src 跳过
        {"img_src": "https://cdn.example/a_full.jpg",
         "url": "https://page.example/d", "engine": "x"},   # 重复去重
    ]}

    def handler(req):
        assert req.url.host == "127.0.0.1" and req.url.path == "/search"
        assert req.url.params["categories"] == "images"
        assert req.url.params["format"] == "json"
        assert req.url.params["language"] == "zh-CN"
        return httpx.Response(200, json=payload)

    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    rows = await eng.search("慕田峪长城", 5, lang="zh")
    assert len(rows) == 2
    assert rows[0]["tiers"] == ["https://cdn.example/a_full.jpg",
                                "https://cdn.example/a_thumb.jpg"]
    assert rows[0]["landing"] == "https://page.example/a"
    assert rows[0]["native"]["engine"] == "bing images"
    assert (rows[0]["width"], rows[0]["height"]) == (1920, 1080)
    print("[PASS] dict 契约/两档候选/引擎溯源/去重")

    # 2) latin 语言对位 + 空认缺
    def h2(req):
        assert req.url.params["language"] == "en"
        return httpx.Response(200, json={"results": []})
    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h2)))
    assert await eng.search("Mutianyu", 5, lang="latin") == []
    print("[PASS] latin 对位与空认缺")

    # 3) K 封顶
    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(200, json={"results": [
            {"img_src": f"https://c/{i}.jpg"} for i in range(50)]}))))
    assert len(await eng.search("q", 99, lang="zh")) == eng.k_cap
    print("[PASS] K 封顶")

    # 4) 网关不可达 fail-fast（配置错误终止，不静默认缺）
    net.set_client(httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: (_ for _ in ()).throw(httpx.ConnectError("refused")))))
    stage = search.SearchStage(top_n=2, k=2)
    try:
        await stage({"name": "x", "query": "q", "lang": "zh"})
        raise AssertionError("应 fail-fast")
    except RuntimeError as e:
        assert "webgate/start.sh" in str(e)
    print("[PASS] 网关不可达 fail-fast")

    # 5) 双闸自声明
    assert net.SOURCE_LIMITS["searxng"].proxy is False
    assert net.SOURCE_LIMITS["dl:searxng"].proxy is True
    print("[PASS] 双闸登记")
    await net.close_client()
    print("冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
