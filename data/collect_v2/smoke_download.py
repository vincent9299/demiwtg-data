"""collect_v2/op_download.py 最小冒烟：MockTransport 验证下载、质量门与失败语义。

运行：python3 -m collect_v2.smoke_download
"""

from __future__ import annotations

import asyncio
import hashlib
import io

import httpx
from PIL import Image

from collect_v2 import infra, op_download, op_search


def make_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def png_bytes(w: int, h: int, color=(200, 30, 30)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (w, h), color).save(buf, "PNG")
    return buf.getvalue()


def item(source: str, urls: list[str] = None) -> op_search.Item:
    return op_search.Item(instance="测试实例", query="测试实例",
                          source=source, rank=0,
                          content_urls=urls if urls is not None
                          else ["https://img.example/a.png"])


async def main() -> None:
    infra.RETRY_INTERVAL = 0.05
    good = png_bytes(1024, 800)

    # 1) 正常下载：字节透传、sha256、解码实测元数据；baidu 带防盗链 Referer
    seen = {}

    def ok_handler(req):
        seen["referer"] = req.headers.get("Referer")
        return httpx.Response(200, content=good)

    r = await op_download.download(item("baidu"), client=make_client(ok_handler))
    assert r is not None
    assert r.data == good and r.sha256 == hashlib.sha256(good).hexdigest()
    assert (r.ext, r.mime) == ("png", "image/png")
    assert (r.actual_width, r.actual_height, r.size_bytes) == (1024, 800, len(good))
    assert r.instance == "测试实例" and r.query == "测试实例"   # 上游字段透传不回落
    assert seen["referer"] == "https://image.baidu.com/"
    print("[PASS] 正常下载与 baidu 防盗链头")

    # 2) wikimedia_zh 下载头为 API UA（礼仪）
    def wm_handler(req):
        assert req.headers["User-Agent"].startswith("collect-v2/")
        return httpx.Response(200, content=good)

    r = await op_download.download(item("wikimedia_zh"), client=make_client(wm_handler))
    assert r is not None
    print("[PASS] wikimedia_zh API UA 头")

    # 3) 非图字节（HTML 反爬页）→ 解码失败拒收（正确性验证，非质量筛选）
    r = await op_download.download(item("baidu"), client=make_client(
        lambda req: httpx.Response(200, content=b"<html>verify</html>")))
    assert r is None
    # 小图不过滤（下载算子不做质量过滤，用户拍板）
    small = png_bytes(64, 48)
    r = await op_download.download(item("wikimedia_zh"), client=make_client(
        lambda req: httpx.Response(200, content=small)))
    assert r is not None and (r.actual_width, r.actual_height) == (64, 48)
    print("[PASS] 非图拒收且小图放行（下载算子无质量过滤）")

    # 4) 字节封顶：超限拒收，不重试
    r = await op_download.download(item("wikimedia_zh"), max_bytes=1024,
                                   client=make_client(
                                       lambda req: httpx.Response(200, content=good)))
    assert r is None
    print("[PASS] 字节封顶拒收")

    # 5) 无候选 → None，不发请求
    r = await op_download.download(item("wikimedia_zh", urls=[]),
                                   client=make_client(lambda req: (_ for _ in ()).throw(
                                       AssertionError("不应发请求"))))
    assert r is None
    print("[PASS] 无候选直接拒收")

    # 6) 候选轮转：首档 404 确定性失败换下一档，获胜链接记回 content_url
    hit_urls = []

    def rotate_handler(req):
        hit_urls.append(str(req.url))
        if str(req.url).endswith("orig.jpg"):
            return httpx.Response(404)
        return httpx.Response(200, content=good)

    r = await op_download.download(
        item("pixiv", urls=["https://img.example/orig.jpg",
                            "https://img.example/orig.png",
                            "https://img.example/master.jpg"]),
        client=make_client(rotate_handler))
    assert r is not None and r.content_url == "https://img.example/orig.png"
    assert len(hit_urls) == 2 and "orig.png" in hit_urls[1]  # 首个成功即停，不试第三档
    print("[PASS] 候选轮转：404 换下一档、获胜即停、获胜链接记回")

    # 6.5) 候选全败 → DeterministicError 上抛（链层认缺）
    try:
        await op_download.download(
            item("pixiv", urls=["https://img.example/a.jpg", "https://img.example/a.png"]),
            client=make_client(lambda req: httpx.Response(404)))
        raise AssertionError("候选全败应抛 DeterministicError")
    except infra.DeterministicError:
        pass
    print("[PASS] 候选全败按确定性失败上抛")

    # 7) 单档 404 → DeterministicError 上抛（认缺，不重试）
    try:
        await op_download.download(item("wikimedia_zh"), client=make_client(
            lambda req: httpx.Response(404)))
        raise AssertionError("404 应抛 DeterministicError")
    except infra.DeterministicError:
        pass
    print("[PASS] 404 确定性失败上抛")

    # 8) 5xx → 重试用尽 TransientExhaustedError（不轮转：瞬态非档位问题）
    hits = {"n": 0}

    def flaky(req):
        hits["n"] += 1
        return httpx.Response(503)

    try:
        await op_download.download(item("wikimedia_zh"), client=make_client(flaky))
        raise AssertionError("5xx 重试用尽应抛 TransientExhaustedError")
    except infra.TransientExhaustedError:
        pass
    assert hits["n"] == infra.MAX_RETRIES + 1
    print("[PASS] 5xx 有界重试后上抛")

    print("冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
