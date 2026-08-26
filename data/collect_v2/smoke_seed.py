"""collect_v2/op_seed.py + getsource.py 最小冒烟：MockTransport 模拟 LLM 判定。

运行：python3 -m collect_v2.smoke_seed
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile

import httpx

from collect_v2 import getsource, op_seed
from collect_v2.op_search import Seed


def llm_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def vlm_reply(picked) -> httpx.Response:
    body = {"choices": [{"message": {"content": json.dumps({"picked": picked})}}]}
    return httpx.Response(200, json=body)


async def main() -> None:
    op_seed.RETRY_INTERVAL = 0.05
    tmp = tempfile.mkdtemp(prefix="seed_smoke_")
    try:
        cache_path = os.path.join(tmp, "alias_western.json")
        cache = op_seed.SeedCache(cache_path)

        # 1) 合格西文投影：LLM 选中候选之一 → 产两个 seed（zh + latin）
        client = llm_client(lambda req: vlm_reply("Mutianyu Great Wall"))
        seeds = await op_seed.project(
            "慕田峪长城", ["慕田峪", "Mutianyu Great Wall", "Mutianyu"],
            cache, client=client)
        assert len(seeds) == 2
        assert seeds[0] == Seed("慕田峪长城", "慕田峪长城", lang="zh")
        assert seeds[1].query == "Mutianyu Great Wall" and seeds[1].lang == "latin"
        assert cache.table["慕田峪长城"] == "Mutianyu Great Wall"
        print("[PASS] 合格西文投影产双 seed")

        # 2) 词表命中：不再调 LLM（client 抛错也进不去）
        poisoned = llm_client(lambda req: (_ for _ in ()).throw(
            AssertionError("词表命中不应调 LLM")))
        seeds = await op_seed.project("慕田峪长城", ["whatever"], cache,
                                      client=poisoned)
        assert len(seeds) == 2 and seeds[1].query == "Mutianyu Great Wall"
        print("[PASS] 词表命中零 LLM（增量补判）")

        # 3) LLM 判 null（类目泛词全拒）→ 只有中文 seed，落词表认缺不重判
        client = llm_client(lambda req: vlm_reply(None))
        seeds = await op_seed.project("跳绳", ["jump rope fitness", "跳绳运动"],
                                      cache, client=client)
        assert len(seeds) == 1 and seeds[0].lang == "zh"
        assert cache.table["跳绳"] is None
        seeds = await op_seed.project("跳绳", None, cache, client=llm_client(
            lambda req: (_ for _ in ()).throw(AssertionError("认缺不应重判"))))
        assert len(seeds) == 1
        print("[PASS] 类目泛词拒绝 + 认缺落表不重判")

        # 4) 防幻觉：LLM 自造候选外的名字 → 视同解析失败，重试耗尽不落表
        client = llm_client(lambda req: vlm_reply("Fabricated Name"))
        seeds = await op_seed.project("太极拳", ["Tai Chi"], cache, client=client)
        assert len(seeds) == 1            # 宁缺毋滥：不产西文 seed
        assert "太极拳" not in cache.table   # 失败不落表，下次重判
        print("[PASS] 防幻觉（自造名拒收）+ 失败不落表")

        # 5) 无西文候选（纯中文别名）→ 不费 LLM，直接认缺落表
        client = llm_client(lambda req: (_ for _ in ()).throw(
            AssertionError("无西文候选不应调 LLM")))
        seeds = await op_seed.project("笛子", None, cache, client=client)
        assert len(seeds) == 1 and cache.table["笛子"] is None
        print("[PASS] 无西文候选直接认缺")

        # 6) 词表原子落盘 + 重新加载
        cache.save()
        again = op_seed.SeedCache(cache_path)
        assert again.table == cache.table
        print("[PASS] 词表落盘与重载")

        # 7) getsource 域路由：zh/latin 均投递新源（所有 seed 都去，拍板）
        zh, latin = seeds[0], Seed("慕田峪长城", "Mutianyu", lang="latin")
        assert [s for _, s in getsource.route(zh)][:2] == ["wikimedia_zh", "baidu"]
        assert [s for _, s in getsource.route(latin)][0] == "wikimedia"
        assert "anilist" in [s for _, s in getsource.route(zh)]      # 新源全量投递
        assert "pixiv" in [s for _, s in getsource.route(latin)]
        assert getsource.route(Seed("x", "x", lang="jp")) == []   # 不回落不放宽
        assert all(r_seed is zh for r_seed, _ in getsource.route(zh))  # seed 原样透传
        print("[PASS] getsource 路由表（含新源全量投递）")

        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
