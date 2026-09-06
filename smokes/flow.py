"""collect_v2/flow.py 最小冒烟：全 mock（检索/下载/VLM）端到端跑编排入口，
验证声明式管线与幂等续跑。运行：python3 -m smoke_flow
"""

from __future__ import annotations

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


def png_bytes(w: int, h: int, rgb) -> bytes:
    im = Image.new("RGB", (w, h), rgb)
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()


GOOD_ANN = json.dumps({
    "kb_match": 8, "richness": 7, "identity": True, "focus": 8,
    "caption": "一段足够长的中文描述，用来满足 caption 最小字数校验要求，确保解析成功。",
}, ensure_ascii=False)

CASES = [("慕田峪长城", (200, 30, 30)), ("菠萝包", (30, 200, 30)),
         ("绫波丽", (30, 30, 200))]
BYTES_BY_URL: dict = {}


def searxng_handler(req: httpx.Request) -> httpx.Response:
    q = str(req.url.params.get("q", ""))
    rgb = next((c for n, c in CASES if n == q), (10, 10, 10))
    results = []
    for i in range(5):
        data = png_bytes(64 + i, 48, rgb)
        sha = hashlib.sha256(data).hexdigest()
        BYTES_BY_URL[f"https://mock.cdn/{sha}.png"] = data
        BYTES_BY_URL[f"https://mock.cdn/{sha}_t.png"] = data
        results.append({
            "img_src": f"https://mock.cdn/{sha}.png",
            "thumbnail_src": f"https://mock.cdn/{sha}_t.png",
            "url": f"https://mock.page/{i}",
            "engine": "mock images", "title": f"{q}-{i}",
            "resolution": f"{64+i} x 48",
        })
    return httpx.Response(200, json={"results": results})


def run_flow(tmp: str, dataset_dir: str, inst_path: str, shard: str = ""):
    from operators import search
    from demiflow.collect import net
    from demiflow.collect.llm import (AsyncLLMClient, close_all_llm,
                                      inject_endpoint_client)

    net.RETRY_INTERVAL = 0.05
    net._gates.clear()
    net._client_direct = net._client_proxy = None
    net._dl_client_direct = net._dl_client_proxy = None
    net.set_client(httpx.AsyncClient(
        transport=httpx.MockTransport(searxng_handler)))
    dl = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(
            200, content=BYTES_BY_URL.get(str(req.url), b""))
        if BYTES_BY_URL.get(str(req.url)) else httpx.Response(404)))
    net.set_download_client(dl)
    net.set_download_client(dl, proxy=True)
    inject_endpoint_client("demiwtg_vlm", AsyncLLMClient(
        base_url="http://mock/v1", model="mock-model",
        http=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda req: httpx.Response(200, json={
                "choices": [{"message": {"content": GOOD_ANN}}]})))))

    # 路由收敛到 searxng 单源（smoke 不打真源）
    _orig = dict(search.ROUTE_TABLE)
    search.ROUTE_TABLE = {"zh": ["searxng"], "latin": ["searxng"]}
    argv = ["flow", "--instances", inst_path, "--dataset", dataset_dir,
            "--alias-cache", os.path.join(tmp, "alias_western.json"),
            "--top-n", "2", "--vlm-concurrency", "4",
            "--search-concurrency", "2", "--download-concurrency", "4",
            "--instance-concurrency", "3", "--log-every", "1"]
    if shard:
        argv += ["--shard", shard]
    sys.argv = argv
    try:
        flow.main()
    finally:
        search.ROUTE_TABLE = _orig
        asyncio_close = close_all_llm
        import asyncio
        asyncio.run(asyncio_close())


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="smoke_flow_")
    try:
        inst_path = os.path.join(tmp, "instances.json")
        json.dump({"instances": [{"name": n, "desc": "测试实体知识。"}
                                 for n, _ in CASES]},
                  open(inst_path, "w", encoding="utf-8"), ensure_ascii=False)
        dataset_dir = os.path.join(tmp, "demiwtg")

        run_flow(tmp, dataset_dir, inst_path)

        manifest = os.path.join(dataset_dir, "meta", "image.jsonl")
        rows = [json.loads(l) for l in open(manifest, encoding="utf-8")]
        assert len(rows) == 6, f"期望 6 行，实际 {len(rows)}"
        by_inst = {}
        for r in rows:
            by_inst.setdefault(r["concepts"][0], []).append(r)
        assert set(by_inst) == {n for n, _ in CASES}
        for name, rs in by_inst.items():
            assert len(rs) == 2
            for r in rs:
                assert r["source"] == "searxng"
                assert r["kb_match"] == 8 and r["identity"] is True
                blob = os.path.join(dataset_dir, r["path"])
                assert hashlib.sha256(
                    open(blob, "rb").read()).hexdigest() == r["sha256"]
        print("[PASS] flow 端到端：3 实例 → 6 行落盘（标注/内容寻址/字段齐全）")

        run_flow(tmp, dataset_dir, inst_path)     # 幂等续跑
        rows2 = [json.loads(l) for l in open(manifest, encoding="utf-8")]
        assert len(rows2) == 6, f"续跑应零追加，实际 {len(rows2)}"
        print("[PASS] 幂等续跑零追加")

        # 5) 分片端到端：两分片各自单写者 → merge 合并 = 全量（D1 分布式形态）
        from operators.annotate import merge_manifests
        ds_shard = os.path.join(tmp, "demiwtg_sharded")
        for idx in (0, 1):
            run_flow(tmp, ds_shard, inst_path, shard=f"{idx}/2")
        shards = sorted(f for f in os.listdir(os.path.join(ds_shard, "meta"))
                        if not f.startswith("."))
        assert shards == ["image-shard-0-of-2.jsonl",
                          "image-shard-1-of-2.jsonl"], shards
        r = merge_manifests(ds_shard)
        assert r["output_rows"] == 6, r
        merged = [json.loads(l) for l in open(
            os.path.join(ds_shard, "meta", "image.jsonl"), encoding="utf-8")]
        base_keys = {(x["sha256"], x["concepts"][0]) for x in rows2}
        merged_keys = {(x["sha256"], x["concepts"][0]) for x in merged}
        assert base_keys == merged_keys            # 分片并集 == 单进程全量
        print("[PASS] 分片运行 + merge 合并 = 全量（每分片单写者）")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
