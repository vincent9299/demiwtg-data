"""collect_v2/op_search.py 最小冒烟：MockTransport 验证两个代表源的解析与契约。

运行：python3 -m collect_v2.smoke_search
"""

from __future__ import annotations

import asyncio
import json

import httpx

from collect_v2 import infra, op_search


def make_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def main() -> None:
    infra.RETRY_INTERVAL = 0.05

    # 1) wikimedia_zh：乱序 pages 按 index 排回相关度序，字段提取正确
    wm_payload = {
        "query": {
            "pages": {
                "101": {"pageid": 101, "index": 2, "title": "File:乙.jpg",
                        "imageinfo": [{"url": "https://up.example/乙.jpg", "width": 800,
                                       "height": 600, "mime": "image/jpeg",
                                       "extmetadata": {"LicenseShortName": {"value": "CC-BY"}}}],
                        "pageprops": {"canonicalurl": "https://zh.wikipedia.org/wiki/File:乙.jpg"}},
                "100": {"pageid": 100, "index": 1, "title": "File:甲.png",
                        "imageinfo": [{"url": "https://up.example/甲.png", "width": 1024,
                                       "height": 768, "mime": "image/png", "extmetadata": {}}]},
            }
        }
    }

    def wm_handler(req):
        assert "commons.wikimedia.org" in str(req.url)
        assert req.url.params["gsrsearch"] == "慕田峪长城"
        return httpx.Response(200, json=wm_payload)

    seed = op_search.Seed(name="慕田峪长城")
    cands = await op_search.search(seed, "wikimedia_zh", client=make_client(wm_handler))
    assert [c.native["page_title"] for c in cands] == ["File:甲.png", "File:乙.jpg"], cands
    assert cands[0].rank == 0 and cands[0].query == "慕田峪长城"
    assert cands[0].content_urls == ["https://up.example/甲.png"]   # commons 直出即原图，单档
    assert cands[1].license == "CC-BY"
    assert cands[1].landing_url == "https://zh.wikipedia.org/wiki/File:乙.jpg"
    print("[PASS] wikimedia_zh 排序与字段提取")

    # 2) baidu：middleURL 优先（不用加密 objURL）、尺寸取 URL 查询串、空壳剔除、去重
    bd_payload = {"data": [
        {"objURL": "ipprf_z2C$q加密串", "middleURL": "https://img0.baidu.com/it/u=1&fm=253?w=640&h=480",
         "width": "3000", "height": "2000", "fromURL": "https://p.example", "di": 12345},
        {"width": "1", "height": "1"},  # 空壳
        {"thumbURL": "https://t.example/b.jpg"},  # 无尺寸查询串
        {"middleURL": "https://img0.baidu.com/it/u=1&fm=253?w=640&h=480"},  # 与首条重复
    ]}

    def bd_handler(req):
        assert "baidu.com" in str(req.url)
        if req.url.path != "/search/acjson":
            return httpx.Response(200, text="<html>home</html>")  # 预热请求
        assert req.url.params["word"] == "菠萝包"
        return httpx.Response(200, json=bd_payload)

    cands = await op_search.search(op_search.Seed(name="菠萝包"), "baidu",
                                   client=make_client(bd_handler))
    assert len(cands) == 2, cands
    assert cands[0].content_urls == ["https://img0.baidu.com/it/u=1&fm=253?w=640&h=480"]  # middleURL 单档候选，不碰 objURL
    assert (cands[0].declared_width, cands[0].declared_height) == (640, 480)  # 尺寸取 URL 查询串而非原图字段
    assert cands[0].native["orig_width"] == 3000                      # 原图尺寸留 native
    assert cands[1].rank == 1 and cands[1].declared_width is None     # 无查询串放行为 None
    assert cands[0].instance == "菠萝包"                              # 种子实例名随行透传
    print("[PASS] baidu middleURL 优先、URL 尺寸提取、空壳剔除与去重")

    # 3) baidu 反爬页（非 JSON）→ TransientExhaustedError
    def bd_anti(req):
        if req.url.path != "/search/acjson":
            return httpx.Response(200, text="<html>home</html>")
        return httpx.Response(200, text="<html>verify</html>")

    try:
        await op_search.search(op_search.Seed(name="x"), "baidu",
                               client=make_client(bd_anti))
        raise AssertionError("非 JSON 应答应抛 TransientExhaustedError")
    except infra.TransientExhaustedError:
        pass
    print("[PASS] baidu 反爬页按瞬态失败上抛")

    # 4) baidu 反爬明确拦截（antiFlag）→ DeterministicError
    def bd_antiflag(req):
        if req.url.path != "/search/acjson":
            return httpx.Response(200, text="<html>home</html>")
        return httpx.Response(200, json={"antiFlag": 1, "message": "Forbid spider access"})

    try:
        await op_search.search(op_search.Seed(name="x"), "baidu",
                               client=make_client(bd_antiflag))
        raise AssertionError("antiFlag 应抛 DeterministicError")
    except infra.DeterministicError:
        pass
    print("[PASS] baidu antiFlag 拦截按确定性失败认缺")

    # 5) K 封顶与认缺：gsrlimit 不超过 k_cap；空结果原样返回
    def wm_empty(req):
        assert int(req.url.params["gsrlimit"]) <= op_search.K_SEMANTIC
        return httpx.Response(200, json={"query": {"pages": {}}})

    cands = await op_search.search(op_search.Seed(name="不存在的东西"), "wikimedia_zh", k=99,
                                   client=make_client(wm_empty))
    assert cands == []
    print("[PASS] K 封顶与空列表认缺")

    # 6) huaban_api：file.key 拼直链、去重、无 key 跳过、宽高取自 file
    hb_payload = {"pins": [
        {"pin_id": 1, "file": {"key": "abc123", "width": 1080, "height": 1920},
         "board": {"title": "角色画集"}},
        {"pin_id": 2, "file": {}},                                   # 无 key 跳过
        {"pin_id": 3, "file": {"key": "abc123", "width": 1, "height": 1}},  # 重复
        {"pin_id": 4, "file": {"key": "def456"}},
    ]}

    def hb_handler(req):
        assert "api.huaban.com" in str(req.url)
        assert req.url.params["q"] == "初音未来"
        assert req.headers.get("Referer") == "https://huaban.com/"
        return httpx.Response(200, json=hb_payload)

    cands = await op_search.search(op_search.Seed(name="初音未来"), "huaban_api",
                                   client=make_client(hb_handler))
    assert [c.content_urls for c in cands] == [
        ["https://hbimg.huaban.com/abc123"],
        ["https://hbimg.huaban.com/def456"]], cands
    assert (cands[0].declared_width, cands[0].declared_height) == (1080, 1920)
    assert cands[1].declared_width is None
    print("[PASS] huaban_api 直链拼接、去重与字段提取")

    # 7) toutiao：toutiaoimg 剔除、byteimg 优先、http 升级 https、UI 噪声过滤
    tt_html = """
    <img src="https://p3.toutiaoimg.com/origin/aaa.jpg">
    <img src="http://p9-byteimg.com/tos/bbb.png">
    <img src="https://p1.douyinpic.com/img/ccc.webp">
    <img src="https://sf1-fe.toutiao.com/static/logo.png">
    """

    def tt_handler(req):
        assert "so.toutiao.com" in str(req.url)
        assert req.url.params["keyword"] == "皮卡丘"
        return httpx.Response(200, text=tt_html)

    cands = await op_search.search(op_search.Seed(name="皮卡丘"), "toutiao",
                                   client=make_client(tt_handler))
    # toutiaoimg/logo 剔除；byteimg 优先（http 升级）；douyinpic 次之
    assert [c.content_urls for c in cands] == [
        ["https://p9-byteimg.com/tos/bbb.png"],
        ["https://p1.douyinpic.com/img/ccc.webp"]], cands
    print("[PASS] toutiao 签名图床剔除、host 优先与 https 升级")

    # 8) so360：多档候选大到小（imgurl > middle > thumb）、landing_url 取 url 字段、去重
    so_payload = {"list": [
        {"imgurl": "https://p0.qhimg.com/t01/aaa.jpg",
         "middle": "https://p1.qhimg.com/mid/aaa_m.png",
         "thumb": "https://p1.qhimg.com/t/aaa_t.png",
         "width": 800, "height": 600,
         "url": "https://page.example/a", "title": "图一"},
        {"middle": "https://p1.qhimg.com/mid/bbb.png"},
        {"imgurl": ""},                                              # 空壳
        {"imgurl": "https://p0.qhimg.com/t01/aaa.jpg"},             # 首档重复整条跳过
    ]}

    def so_handler(req):
        assert "image.so.com" in str(req.url)
        assert req.url.params["q"] == "绫波丽"
        return httpx.Response(200, json=so_payload)

    cands = await op_search.search(op_search.Seed(name="绫波丽"), "so360",
                                   client=make_client(so_handler))
    assert [c.content_urls for c in cands] == [
        ["https://p0.qhimg.com/t01/aaa.jpg",
         "https://p1.qhimg.com/mid/aaa_m.png",
         "https://p1.qhimg.com/t/aaa_t.png"],
        ["https://p1.qhimg.com/mid/bbb.png"]], cands
    assert cands[0].landing_url == "https://page.example/a"
    assert (cands[0].declared_width, cands[0].declared_height) == (800, 600)
    assert cands[0].native["title"] == "图一"
    print("[PASS] so360 多档候选大到小、去重与字段提取")

    # 8.5) pixiv：原图候选大到小（jpg/png 试错 + master1200 殿后）、ugoira 单档、R18 剔除
    px_payload = {"error": False, "body": {"illustManga": {"data": [
        {"id": "111", "title": "普通作", "url":
            "https://i.pximg.net/c/250x250_80_a2/img-master/img/2026/08/20/00/00/00/111_p0_square1200.jpg",
         "illustType": 1, "xRestrict": 0, "width": 2016, "height": 1118,
         "userName": "画师甲", "userId": 1},
        {"id": "222", "title": "多页作", "url":
            "https://i.pximg.net/c/250x250_80_a2/img-master/img/2026/08/20/00/00/00/222_p0_square1200.jpg",
         "illustType": 1, "xRestrict": 0, "width": 800, "height": 600,
         "userName": "画师乙", "userId": 2},
        {"id": "333", "title": "动图", "url":
            "https://i.pximg.net/c/250x250_80_a2/img-master/img/2026/08/20/00/00/00/333_p0_square1200.jpg",
         "illustType": 2, "xRestrict": 0, "width": 640, "height": 360,
         "userName": "画师丙", "userId": 3},
        {"id": "555", "title": "AI作", "url":
            "https://i.pximg.net/custom-thumb/img/2026/08/20/00/00/00/555_p0_custom1200.jpg",
         "illustType": 1, "xRestrict": 0, "width": 1200, "height": 1200,
         "userName": "画师戊", "userId": 5},
        {"id": "444", "title": "R18", "url":
            "https://i.pximg.net/c/250x250_80_a2/img-master/img/2026/08/20/00/00/00/444_p0_square1200.jpg",
         "illustType": 1, "xRestrict": 1, "width": 100, "height": 100,
         "userName": "画师丁", "userId": 4},
    ]}}}

    def px_handler(req):
        assert "pixiv.net/ajax/search/artworks" in str(req.url)
        assert req.headers.get("Referer") == "https://www.pixiv.net/"
        return httpx.Response(200, json=px_payload)

    cands = await op_search.search(op_search.Seed(name="初音ミク"), "pixiv",
                                   client=make_client(px_handler))
    assert [c.native["artwork_id"] for c in cands] == ["111", "222", "333", "555"]  # R18 剔除
    _BASE = "https://i.pximg.net/img-original/img/2026/08/20/00/00/00/"
    assert cands[0].content_urls == [                      # 原图 jpg/png 试错 + master 殿后
        _BASE + "111_p0.jpg", _BASE + "111_p0.png",
        "https://i.pximg.net/img-master/img/2026/08/20/00/00/00/111_p0_master1200.jpg"]
    assert len(cands[1].content_urls) == 3                 # 多页作取首页 p0 同规则
    assert cands[2].content_urls == [                      # ugoira 无静态原图，只给 master 首帧
        "https://i.pximg.net/img-master/img/2026/08/20/00/00/00/333_p0_master1200.jpg"]
    assert cands[3].content_urls == [                      # custom-thumb 无原图档，单元素
        "https://i.pximg.net/custom-thumb/img/2026/08/20/00/00/00/555_p0_custom1200.jpg"]
    assert (cands[0].declared_width, cands[0].declared_height) == (2016, 1118)
    print("[PASS] pixiv 原图候选大到小、ugoira/custom-thumb 单档与 R18 剔除")

    # 9) 未注册源报错
    try:
        await op_search.search(op_search.Seed(name="x"), "no_such_source")
        raise AssertionError("未注册源应报错")
    except ValueError:
        pass
    print("[PASS] 未注册源拒绝")

    print("冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
