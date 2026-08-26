"""collect_v2/op_annotate.py 最小冒烟：MockTransport 模拟 vLLM 端点验证契约。

运行：python3 -m collect_v2.smoke_annotate
"""

from __future__ import annotations

import asyncio
import io
import json

import httpx
from PIL import Image

from collect_v2 import op_annotate, op_search

KB = {
    "慕田峪长城": {
        "desc": "慕田峪长城位于北京市怀柔区，是明长城的精华段落，"
                "以敌楼密集、植被葱郁著称，为国家级文物保护单位。",
        "aliases": ["Mutianyu", "慕田峪"],
    },
}


def make_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def png_bytes(w: int = 64, h: int = 48) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (w, h), (10, 120, 30)).save(buf, "PNG")
    return buf.getvalue()


def item(data: bytes | None) -> op_search.Item:
    it = op_search.Item(instance="慕田峪长城", query="慕田峪长城",
                        source="wikimedia_zh", rank=0)
    it.data = data
    return it


def vlm_reply(content: str):
    def handler(req):
        body = json.loads(req.content)
        assert body["messages"][0]["role"] == "system"
        assert "identity" in body["messages"][0]["content"]      # 新增字段已进 prompt
        assert "focus" in body["messages"][0]["content"]         # focus 转正已进 prompt
        assert "慕田峪长城" in body["messages"][1]["content"][1]["text"]
        return httpx.Response(200, json={
            "choices": [{"message": {"content": content}}]})
    return handler


GOOD = json.dumps({"kb_match": 9, "richness": 8, "identity": True, "focus": 10,
                   "caption": "这是一张长城城墙沿山脊蜿蜒延伸的照片，敌楼清晰可见，"
                              "两侧植被茂密，远景层峦叠嶂，为自然光下的实景摄影。"},
                  ensure_ascii=False)


async def main() -> None:
    op_annotate.RETRY_INTERVAL = 0.05

    # 1) 正常打标：字段追加到 Item，上游字段不动；quality 为算子派生
    it = item(png_bytes())
    r = await op_annotate.annotate(it, KB, client=make_client(vlm_reply(GOOD)))
    assert r is it                                   # 同一 Item 原地追加
    assert (r.kb_match, r.richness, r.identity, r.focus) == (9, 8, True, 10)
    # quality = 0.4*kb + 0.4*focus + 0.2*richness = 3.6+4.0+1.6 = 9.2
    assert r.quality == 9.2
    assert r.caption.startswith("这是一张长城")
    assert r.instance == "慕田峪长城" and r.query == "慕田峪长城"
    print("[PASS] 正常打标与字段追加（含 focus/quality 派生）")

    # 2) VLM 应答解析失败（caption 过短）→ 重试用尽 → 无标注放行（不弃图）
    hits = {"n": 0}
    bad = json.dumps({"kb_match": 5, "richness": 5, "identity": True,
                      "caption": "太短"}, ensure_ascii=False)

    def bad_handler(req):
        hits["n"] += 1
        return httpx.Response(200, json={"choices": [{"message": {"content": bad}}]})

    it = item(png_bytes())
    r = await op_annotate.annotate(it, KB, client=make_client(bad_handler))
    assert r is it and r.kb_match is None and r.caption is None   # 无标注放行
    assert hits["n"] == op_annotate.RETRIES                        # 有界重试
    print("[PASS] 解析失败有界重试后无标注放行")

    # 3) VLM 端点 500 → 重试用尽 → 无标注放行
    def err_handler(req):
        return httpx.Response(500)

    it = item(png_bytes())
    r = await op_annotate.annotate(it, KB, client=make_client(err_handler))
    assert r is it and r.kb_match is None
    print("[PASS] VLM 网络失败无标注放行")

    # 4) identity 非布尔 → 视为解析失败
    assert op_annotate.parse_annotation(
        '{"kb_match":9,"richness":8,"identity":"yes","focus":9,'
        '"caption":"' + "字" * 50 + '"}') is None
    print("[PASS] identity 非布尔判为解析失败")

    # 4.5) focus 缺失 → 视为解析失败（focus 已是契约必选字段）
    assert op_annotate.parse_annotation(
        '{"kb_match":9,"richness":8,"identity":true,'
        '"caption":"' + "字" * 50 + '"}') is None
    print("[PASS] focus 缺失判为解析失败")

    # 5) 知识块构造：别名前 5、desc 截断 250、无知识实例兜底
    blk = op_annotate.build_block("慕田峪长城", KB)
    assert "别名：Mutianyu、慕田峪" in blk and "知识：" in blk
    blk2 = op_annotate.build_block("不存在实体", KB)
    assert "仅凭实体名称判断" in blk2
    print("[PASS] 知识块构造")

    # 6) 未下载 Item（data=None）原样流转不报错
    r = await op_annotate.annotate(item(None), KB,
                                   client=make_client(lambda req: (_ for _ in ()).throw(
                                       AssertionError("不应发请求"))))
    assert r.kb_match is None
    print("[PASS] 未下载 Item 原样流转")

    print("冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
