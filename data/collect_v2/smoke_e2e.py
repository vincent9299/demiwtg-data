"""collect_v2 端到端冒烟：实网 + 真 VLM 全链路验证（链路正确性优先，不收量）。

链路（契约 §4 全算子流）：instances.json → op_seed.project（desc 全量入判定）
→ getsource.route → op_search（逐源）→ op_download → op_annotate（vLLM）
→ op_sink（metadata.jsonl）。

两段（2026-08-20 用户拍板）：
- ① 临时湖：全链路 + 断言，跑完即删，不碰真实数据；
- ② 真湖小批：同链路少量 Item 写真实 datasets/demiwtg/（blobs 内容寻址幂等安全，
  metadata.jsonl 为 v2 专属新清单）。

规模：3 实例 × 每源只下载首条候选；VLM 并发 4。
运行：PYTHONPATH=<仓库根> python3 collect_v2/smoke_e2e.py
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile

# 环境代理残留清除（AGENTS.md §7：宕机旧代理会拖死直连池；
# httpx 默认 trust_env 会捡环境代理，必须在建客户端前清掉）
for _k in list(os.environ):
    if "proxy" in _k.lower():
        del os.environ[_k]

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INSTANCES_PATH = os.path.join(REPO_ROOT, "datasets", "demiwtg", "meta", "instances.json")
REAL_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")

SAMPLE_NAMES = ["皮卡丘", "初音未来", "灶门炭治郎"]   # curated×2 + llm×1（8 aliases 压测选择题）
K = 3               # 每源检索候选数
VLM_CONCURRENCY = 4
REAL_LAKE_BATCH = 3  # 真湖演示条数

from collect_v2 import getsource, infra, op_annotate, op_download, op_search, op_seed, op_sink
from collect_v2.op_search import Item


def load_samples() -> list[dict]:
    """取样实例（name/desc/aliases），缺失即测试失败。"""
    doc = json.loads(open(INSTANCES_PATH, encoding="utf-8").read())
    insts = {i["name"]: i for i in doc.get("instances", [])}
    out = []
    for name in SAMPLE_NAMES:
        assert name in insts, f"样本实例 {name!r} 不在 instances.json"
        out.append(insts[name])
    return out


def read_manifest(path: str) -> list:
    recs = []
    with open(path, encoding="utf-8") as f:
        for l in f:
            l = l.strip()
            if not l:
                continue
            try:
                recs.append(json.loads(l))
            except json.JSONDecodeError:
                continue
    return recs


async def main() -> None:
    samples = load_samples()
    kb = op_annotate.load_instance_kb(INSTANCES_PATH)
    tmp = tempfile.mkdtemp(prefix="e2e_smoke_")
    try:
        cache = op_seed.SeedCache(os.path.join(tmp, "alias_western.json"))
        vlm_client = __import__("httpx").AsyncClient()
        vlm_sem = asyncio.Semaphore(VLM_CONCURRENCY)
        downloaded: list[Item] = []
        hit_sources: set[str] = set()
        miss_log: list[str] = []

        # ---------------- ① 临时湖全链路 ----------------
        sink = op_sink.Sink(os.path.join(tmp, "dataset"))
        assert os.path.basename(sink.manifest) == "metadata.jsonl"
        sink.load_index()

        for inst in samples:
            name = inst["name"]
            seeds = await op_seed.project(
                name, inst.get("aliases") or [], cache,
                desc=inst.get("desc") or "", client=vlm_client)
            # 种子断言：中文本体必有；三样本均有合格西文别名，投影必出
            assert seeds[0].lang == "zh" and seeds[0].query == name
            assert len(seeds) == 2, f"{name}: 西文投影缺失 {seeds}"
            assert seeds[1].lang == "latin"
            assert seeds[1].query.lower() in {
                str(a).lower() for a in inst.get("aliases") or []
            }, f"{name}: 投影 {seeds[1].query!r} 不在 aliases 内（防幻觉护栏失效）"
            print(f"[seed] {name} → zh + latin({seeds[1].query!r})")

            for seed in seeds:
                for sd, source in getsource.route(seed):
                    try:
                        items = await op_search.search(sd, source, k=K)
                    except infra.InfraError as exc:
                        # 无命中 404 也走这里（如 anilist/mal 对中文词），属正常认缺
                        miss_log.append(f"{name}/{source}: 检索失败 "
                                        f"{type(exc).__name__}: {exc}")
                        continue
                    if not items:
                        miss_log.append(f"{name}/{source}: 0 候选")
                        continue
                    hit_sources.add(source)
                    it = items[0]   # 只取首条：链路正确性优先不收量
                    try:
                        got = await op_download.download(it)
                    except infra.InfraError as exc:
                        miss_log.append(f"{name}/{source}: 下载失败 {exc}")
                        continue
                    if got is None:
                        miss_log.append(f"{name}/{source}: 首条被拒（非图/超限）")
                        continue
                    async with vlm_sem:
                        await op_annotate.annotate(got, kb, client=vlm_client)
                    downloaded.append(got)
                    print(f"[hit] {name}/{source}: {got.actual_width}x{got.actual_height} "
                          f"{got.ext} kb_match={got.kb_match}")

        # 链路断言
        assert len(downloaded) >= 5, f"下载成功过少（{len(downloaded)}），链路异常"
        assert len(hit_sources) >= 6, f"有召回源过少（{hit_sources}）"
        assert any(it.kb_match is not None for it in downloaded), "VLM 全部失败"
        print(f"[PASS] 全链路产出 {len(downloaded)} 图，有召回源 {len(hit_sources)} 个：",
              sorted(hit_sources))

        # 落盘断言：metadata.jsonl 行/blob/sha/字段集
        sunk = [it for it in downloaded if await sink.sink(it)]
        assert sunk, "临时湖落盘 0 条"
        recs = read_manifest(sink.manifest)
        assert len(recs) == len(sunk)
        sha_set = set()
        for r in recs:
            blob = os.path.join(sink.dataset_dir, r["path"])
            assert os.path.isfile(blob) and r["sha256"] not in sha_set
            sha_set.add(r["sha256"])
            for fld in ("instances", "queries", "query_langs", "kb_match",
                        "richness", "caption", "identity", "focus", "quality",
                        "source", "fetched_at"):
                assert fld in r, f"清单缺字段 {fld}"
        # 撞车跳过：重复 sink 同一图
        assert await sink.sink(sunk[0]) is False
        print(f"[PASS] 临时湖落盘 {len(sunk)} 条（blob/字段/去重均通过）")
        await vlm_client.aclose()
        await infra.close_client()

        # ---------------- ② 真湖小批演示 ----------------
        real_sink = op_sink.Sink(REAL_DATASET)   # 默认 metadata.jsonl（用户拍板）
        before = real_sink.load_index()
        batch = sunk[:REAL_LAKE_BATCH]
        results = [await real_sink.sink(it) for it in batch]
        recs_real = read_manifest(real_sink.manifest)
        for it, ok in zip(batch, results):
            if ok:
                blob = os.path.join(REAL_DATASET, it.local_path)
                assert os.path.isfile(blob)
            print(f"[real] {it.instance}/{it.source}: {'新落盘' if ok else 'sha 已存在跳过'}")
        print(f"[PASS] 真湖演示完成：索引 {before} → 新落 {sum(results)} 条，"
              f"metadata.jsonl 共 {len(recs_real)} 行")

        # 认缺汇报（只报不断言：源波动属正常，链层认缺）
        if miss_log:
            print(f"[认缺] {len(miss_log)} 条：")
            for line in miss_log:
                print("  -", line)
        print("端到端冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
