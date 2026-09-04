"""collect_v2/annotate.py 最小冒烟：打标口径金测（请求载荷逐字段断言，
31 万存量可比性红线）+ ManifestSink 幂等。运行：python3 -m smoke_annotate
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import tempfile

import httpx

from operators import annotate
from demiflow.collect.llm import (AsyncLLMClient, close_all_llm,
                                  inject_endpoint_client)

GOOD = json.dumps({
    "kb_match": 8, "richness": 7, "identity": True, "focus": 8,
    "caption": "一段足够长的中文描述，用来满足 caption 最小字数校验要求，确保解析成功。",
}, ensure_ascii=False)


def image_bytes() -> bytes:
    import io
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (120, 90), (5, 5, 5)).save(buf, "PNG")
    return buf.getvalue()


async def main() -> None:
    tmp = tempfile.mkdtemp(prefix="smoke_annotate_")
    try:
        data = image_bytes()
        row = {"name": "慕田峪长城", "query": "慕田峪长城", "lang": "zh",
               "source": "baidu", "tiers": ["https://x/a.png"],
               "data": data, "sha256": hashlib.sha256(data).hexdigest(),
               "ext": "png", "content_url": "https://x/a.png"}

        # 1) 口径金测：捕获请求载荷，逐字段断言与旧实现完全一致
        captured = {}

        def capture(req):
            captured["payload"] = json.loads(req.content)
            return httpx.Response(200, json={
                "choices": [{"message": {"content": GOOD}}]})

        inject_endpoint_client("demiwtg_vlm", AsyncLLMClient(
            base_url="http://mock/v1", model="mock-model",
            http=httpx.AsyncClient(transport=httpx.MockTransport(capture))))
        out = await annotate.annotate(dict(row), {"慕田峪长城": {"desc": "长城。", "aliases": []}})
        pl = captured["payload"]
        assert pl["model"] == "mock-model"
        assert pl["max_tokens"] == annotate.MAX_TOKENS
        assert pl["temperature"] == 0.0
        assert pl["response_format"] == {"type": "json_object"}
        assert pl["chat_template_kwargs"] == {"enable_thinking": False}
        assert pl["messages"][0]["role"] == "system"
        u = pl["messages"][1]["content"]
        assert u[0]["type"] == "image_url" and u[0]["image_url"]["url"].startswith(
            "data:image/jpeg;base64,")
        assert u[1]["type"] == "text"
        # 标注键追加 + quality 派生
        assert out["kb_match"] == 8 and out["identity"] is True
        assert out["quality"] == round(
            annotate.QUALITY_WEIGHTS[0] * 8 + annotate.QUALITY_WEIGHTS[1] * 8
            + annotate.QUALITY_WEIGHTS[2] * 7, 1)
        print("[PASS] 口径金测：载荷参数/消息结构/派生分与旧实现一致")

        # 2) 无 data 行原样流转
        assert await annotate.annotate({"name": "x"}, {}) == {"name": "x"}
        print("[PASS] 未下载行原样流转")

        # 3) AnnotateSinkStage：落盘 + 撞车跳过
        ds = os.path.join(tmp, "demiwtg")
        sink = annotate.ManifestSink(ds)
        assert sink.load_index() == 0
        stage = annotate.AnnotateSinkStage(sink, {})
        assert await stage(dict(row)) is not None
        assert stage.annotated == 1
        assert await stage(dict(row)) is None            # 同键幂等跳过
        other = dict(row, name="八达岭长城")              # 同图跨实例追加
        assert await stage(other) is not None
        lines = [json.loads(l) for l in open(sink.manifest, encoding="utf-8")]
        assert len(lines) == 2
        assert lines[0]["kb_match"] == 8 and lines[0]["instances"] == ["慕田峪长城"]
        blob = os.path.join(ds, lines[0]["path"])
        assert hashlib.sha256(open(blob, "rb").read()).hexdigest() == row["sha256"]
        print("[PASS] 算子级落盘/幂等/跨实例追加")
        await close_all_llm()
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
