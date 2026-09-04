"""collect_v2/seed.py 最小冒烟：MockTransport 模拟 LLM 判定，验证三态词表
与 dict 种子行契约。运行：python3 -m data_pipeline.smoke_seed
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile

import httpx

from data_pipeline.operators import seed
from demiflow.collect.llm import (AsyncLLMClient, inject_endpoint_client,
                                  register_endpoint)


def llm_client(handler) -> AsyncLLMClient:
    return AsyncLLMClient(base_url="http://mock/v1", model="mock",
                          http=httpx.AsyncClient(
                              transport=httpx.MockTransport(handler)))


def reply(picked):
    return httpx.Response(200, json={"choices": [{"message": {"content":
            json.dumps({"picked": picked}, ensure_ascii=False)}}]})


async def main() -> None:
    tmp = tempfile.mkdtemp(prefix="smoke_seed_")
    try:
        cache_path = os.path.join(tmp, "alias.json")

        # 1) 判定合格：两 seed（zh + latin）
        cache = seed.SeedCache(cache_path)
        inject_endpoint_client("demiwtg_vlm", llm_client(
            lambda req: reply("Tai Chi Chuan")))
        seeds = await seed.project("太极拳", ["Tai Chi Chuan", "shadow boxing"],
                                   cache, desc="中国传统武术。")
        assert [s["lang"] for s in seeds] == ["zh", "latin"], seeds
        assert seeds[1]["query"] == "Tai Chi Chuan"
        assert seeds[0]["name"] == "太极拳"
        print("[PASS] 判定合格产双 seed（dict 行契约）")

        # 2) 防幻觉：LLM 自造名（不在候选内）→ 判定失败不落词表
        cache2 = seed.SeedCache(cache_path)
        inject_endpoint_client("demiwtg_vlm", llm_client(
            lambda req: reply("Taichi")))
        seeds = await seed.project("太极拳", ["Tai Chi Chuan"], cache2)
        assert len(seeds) == 1 and seeds[0]["lang"] == "zh"
        judged, _ = cache2.get("太极拳")
        assert not judged
        print("[PASS] 防幻觉拒绝自造名")

        # 3) 无候选：直接认缺落词表（不调 LLM）
        calls = {"n": 0}

        def h(req):
            calls["n"] += 1
            return reply("x")
        inject_endpoint_client("demiwtg_vlm", llm_client(h))
        cache3 = seed.SeedCache(cache_path)
        seeds = await seed.project("跳绳", None, cache3)
        assert len(seeds) == 1 and calls["n"] == 0
        judged, v = cache3.get("跳绳")
        assert judged and v is None
        print("[PASS] 无候选直接认缺落词表")

        # 4) 词表落盘与重载
        cache.save()
        re = seed.SeedCache(cache_path)
        assert re.get("太极拳") == (True, "Tai Chi Chuan")
        print("[PASS] 词表落盘与重载")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
