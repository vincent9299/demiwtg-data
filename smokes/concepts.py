"""concepts 批任务模式最小冒烟：加载器校验/配额兜底/query 展开/top_n_hint/
text-only 路由/配额循环收敛。运行：python3 -m smokes.concepts
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


def png(tag: int) -> bytes:
    im = Image.new("RGB", (64 + tag, 48), (tag * 7 % 255, 30, 60))
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()


GOOD_ANN = json.dumps({
    "kb_match": 8, "richness": 7, "identity": True, "focus": 8,
    "caption": "一段足够长的中文描述，用来满足 caption 最小字数校验要求，确保解析成功。",
}, ensure_ascii=False)

BYTES: dict = {}


def searxng_handler(req):
    q = str(req.url.params.get("q", ""))
    results = []
    for i in range(5):                       # 每 query 引擎返 5 条
        data = png(hash(q) % 200 + i)
        sha = hashlib.sha256(data).hexdigest()
        BYTES[f"https://mock.cdn/{sha}.png"] = data
        results.append({"img_src": f"https://mock.cdn/{sha}.png",
                        "url": f"https://mock.page/{i}", "engine": "mock"})
    return httpx.Response(200, json={"results": results})


def install_mocks():
    from demiflow.collect import net
    from demiflow.collect.llm import AsyncLLMClient, inject_endpoint_client
    net.RETRY_INTERVAL = 0.05
    net._gates.clear()
    net._client_direct = net._client_proxy = None
    net._dl_client_direct = net._dl_client_proxy = None
    mock = httpx.AsyncClient(transport=httpx.MockTransport(searxng_handler))
    dl = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(200, content=BYTES.get(str(req.url)))
        if BYTES.get(str(req.url)) else httpx.Response(404)))
    net.set_client(mock)
    net.set_client(mock, proxy=True)
    net.set_download_client(dl)
    net.set_download_client(dl, proxy=True)
    inject_endpoint_client("demiwtg_vlm", AsyncLLMClient(
        base_url="http://mock/v1", model="m",
        http=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda req: httpx.Response(200, json={
                "choices": [{"message": {"content": GOOD_ANN}}]})))))
    from operators import search
    search.ROUTE_TABLE = {"zh": ["searxng"], "latin": ["searxng"]}


def main() -> None:
    from operators.concepts import ConceptSeedStage, load_concepts
    tmp = tempfile.mkdtemp(prefix="smoke_concepts_")
    try:
        # 1) 加载器：3.0 原生（plan 配额）+ 2.0 适配 + 种子展开
        doc3 = {"schema_version": "3.0.0", "concepts": [
            {"name": "测试甲", "aliases": ["A thing", "甲物"],
             "carriers": "image+text"}],
            "plan": {"by_gate": {"strict": 40}, "default": {"quota_images": 20},
                     "per_concept": {"测试甲": {"quota_images": 40}}}}
        p3 = os.path.join(tmp, "v3.json")
        json.dump(doc3, open(p3, "w", encoding="utf-8"), ensure_ascii=False)
        rows3, plan3 = load_concepts(p3)
        assert rows3[0]["min_images"] == 40            # per_concept 覆盖
        doc2 = {"schema_version": "2.0.0", "concepts": [
            {"name": "测试甲", "desc": "d", "aliases": ["A thing", "甲物"],
             "query": ["A thing"], "gate": "strict", "carriers": "image+text"},
            {"name": "测试乙", "query": ["B"], "gate": "relevance",
             "collect": {"min_images": 10}, "carriers": "image+text"},
            {"name": "纯文", "query": ["x"], "carriers": "text"},
            {"name": "测试甲", "query": ["dup"]}]}
        cpath = os.path.join(tmp, "batch.json")
        json.dump(doc2, open(cpath, "w", encoding="utf-8"), ensure_ascii=False)
        rows = load_concepts(cpath)[0]
        assert len(rows) == 3 and rows[0]["min_images"] == 40
        assert rows[1]["min_images"] == 10
        assert sorted(rows[0].keys()) == ["aliases", "carriers", "min_images", "name"]
        seeds = asyncio.run(ConceptSeedStage()(rows[0]))
        assert [s["lang"] for s in seeds] == ["zh", "latin", "zh"]   # 名+双别名
        assert seeds[0]["top_n_hint"] == 2
        print("[PASS] 加载器（3.0 plan/2.0 适配）/三字段/种子展开/top_n_hint")

        # 2) 端到端：flow --concepts（配额循环收敛；text-only 排除）
        install_mocks()
        ds = os.path.join(tmp, "demiwtg")
        sys.argv = ["flow", "--concepts", cpath, "--dataset", ds,
                    "--alias-cache", os.path.join(tmp, "alias.json"),
                    "--quota-passes", "2", "--log-every", "100",
                    "--vlm-concurrency", "4", "--search-concurrency", "2",
                    "--download-concurrency", "4", "--instance-concurrency", "3"]
        flow.main()
        manifest = os.path.join(ds, "meta", "image.jsonl")
        rows_out = [json.loads(l) for l in open(manifest, encoding="utf-8")]
        by_name = {}
        for r in rows_out:
            by_name.setdefault(r["concepts"][0], []).append(r)
        assert set(by_name) == {"测试甲", "测试乙"}, by_name.keys()   # 纯文未进图像线
        # 新模型数学：甲 = 3 种子(name+2别名)×hint2 = 6；乙无别名 = 1 种子×hint1 = 1
        assert len(by_name["测试甲"]) == 6, by_name
        assert len(by_name["测试乙"]) == 1, by_name
        print(f"[PASS] 端到端：甲 6 行（3 种子×hint2）/ 乙 1 行（无别名单种子），"
              f"text-only 排除，配额循环收敛")

        # 3) 幂等：重跑零追加
        flow.main()
        rows2 = [json.loads(l) for l in open(manifest, encoding="utf-8")]
        assert len(rows2) == len(rows_out)
        print("[PASS] 幂等重跑零追加")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
