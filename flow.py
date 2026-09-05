"""collect_v2 编排（纯声明，2026-09-04·十 dict 行化终态）：
准备实例 → 算子列表 → demiflow run_stages。零算子逻辑。

算子集（各自文件，自包含）：
  seed.SeedStage        实例行 → 种子行集
  search.SearchStage    种子行 → 候选行集（路由+13 引擎+限速自声明）
  download.DownloadStage 候选行 → 图像行（fetch_tiers+verify）
  annotate.AnnotateSinkStage 图像行 → 标注并落盘（含清单契约）

平台（demiflow）：StreamStage 规范 / run_stages 编排入口（并发覆盖 +
退出期资源收尾）/ LLM 端点注册表 / HTTP 双池限速 / scan_counts 续跑现算。

运行：PYTHONPATH=<仓库根> python3 -m flow --limit 200
"""

from __future__ import annotations

import argparse
import os
import random
import time

# 环境代理残留清除（AGENTS.md §7：建客户端之前必须清掉；
# 代理配置走平台 env DEMIFLOW_PROXY_URL）
for _k in list(os.environ):
    if "proxy" in _k.lower():
        del os.environ[_k]

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INSTANCES = os.path.join(REPO_ROOT, "datasets", "demiwtg", "meta", "instances.json")
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")
DEFAULT_ALIAS_CACHE = os.path.join(REPO_ROOT, "datasets", "demiwtg", "meta", "alias_western.json")

from operators import annotate, download, search, seed
from demiflow.collect.llm import reconfigure_endpoint
from demiflow.collect.resume import scan_counts
from demiflow.standalone import local_data, run_stages

SAVE_EVERY = 100           # 词表每 N 实例落盘（断点续跑第三层）


def load_instances(path: str) -> list:
    import json
    doc = json.loads(open(path, encoding="utf-8").read())
    return [i for i in doc.get("instances", []) if i.get("name")]


# ---------------------------------------------------------------------------
# 输入准备（编排侧数据筛选，非算子）：覆盖现算过滤 + 稳定分区
# ---------------------------------------------------------------------------

def load_coverage(dataset_dir: str, *, min_quality: float = 0,
                  require_identity: bool = False,
                  manifest_name: str = "metadata.jsonl") -> dict:
    """主清单现算 {实例名: 合格图数}（机制在平台 scan_counts）。

    质量门口径：合格 = quality >= min_quality（缺字段按不合格）且（若启用）
    identity=True；两门全关退化为数全部行。「有图但全不合格」按 0 图继续采。
    """
    manifest = os.path.join(dataset_dir, "meta", manifest_name)

    def row_filter(rec: dict) -> bool:
        if min_quality > 0:
            q = rec.get("quality")
            if not isinstance(q, (int, float)) or q < min_quality:
                return False
        if require_identity and rec.get("identity") is not True:
            return False
        return True

    return scan_counts(manifest, row_filter=row_filter,
                       key_of=lambda r: r.get("instances") or [])


def filter_uncovered(insts: list, counts: dict, min_images: int) -> tuple:
    """按覆盖度过滤实例（保原表序只选择不改写）。"""
    if min_images <= 0:
        return insts, 0
    kept = [i for i in insts if counts.get(i.get("name") or "", 0) < min_images]
    return kept, len(insts) - len(kept)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="collect_v2 采集编排（demiflow 声明式）")
    p.add_argument("--instances", default=DEFAULT_INSTANCES)
    p.add_argument("--dataset", default=DEFAULT_DATASET,
                   help="清单/状态根（多机部署用本地盘：追加型写入不适合对象存储挂载）")
    p.add_argument("--blob-root", default="",
                   help="blob 落盘根（共享存储，跨机内容寻址共享；缺省=--dataset）")
    p.add_argument("--alias-cache", default=DEFAULT_ALIAS_CACHE)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--skip-covered", type=int, default=0, metavar="N")
    p.add_argument("--min-quality", type=float, default=8.0)
    p.add_argument("--require-identity", action=argparse.BooleanOptionalAction,
                   default=True)
    p.add_argument("--top-n", type=int, default=2)
    p.add_argument("--k", type=int, default=search.K_SEMANTIC)
    p.add_argument("--vlm-concurrency", type=int, default=48)
    p.add_argument("--search-concurrency", type=int, default=16)
    p.add_argument("--download-concurrency", type=int, default=32)
    p.add_argument("--instance-concurrency", type=int, default=16)
    p.add_argument("--shuffle", type=int, default=None, metavar="SEED")
    p.add_argument("--log-every", type=int, default=20)
    p.add_argument("--shard", default="", metavar="I/N",
                   help="分片运行：实例按 I::N 切片、清单与词表用分片后缀"
                        "（分布式 D2 前置；每分片单写者，merge_shards.py 合并）")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    insts = load_instances(args.instances)
    if args.skip_covered > 0:
        counts = load_coverage(
            args.dataset, min_quality=args.min_quality,
            require_identity=args.require_identity)
        insts, skipped = filter_uncovered(insts, counts, args.skip_covered)
        gate = (f"quality>={args.min_quality:g}"
                + ("、identity" if args.require_identity else ""))
        print(f"[flow] 覆盖过滤（{gate}）：跳过 {skipped} 个已有 "
              f"≥{args.skip_covered} 张合格图的实例，剩 {len(insts)} 待消费", flush=True)
        head = [i for i in insts if counts.get(i.get("name") or "", 0) == 0]
        tail = [i for i in insts if counts.get(i.get("name") or "", 0) > 0]
        insts = head + tail          # 稳定分区：0 图排队首、难啃实例沉底
    if args.shuffle is not None:
        random.Random(args.shuffle).shuffle(insts)
    insts = insts[args.offset:]
    if args.limit > 0:
        insts = insts[:args.limit]
    manifest_name = "metadata.jsonl"
    alias_cache = args.alias_cache
    if args.shard:
        try:
            i, n = (int(x) for x in args.shard.split("/"))
            assert 0 <= i < n
        except Exception:
            raise SystemExit(f"--shard 需为 I/N 形式（收到 {args.shard!r}）")
        insts = insts[i::n]                 # 输入分片：词表/闸门语义随分片正确
        manifest_name = f"metadata-shard-{i}-of-{n}.jsonl"
        alias_cache = f"{args.alias_cache}.shard{i}-of-{n}"
        search.scale_engine_limits(n)      # 限速预算等分：N 进程合计不超发
        print(f"[flow] 分片 {i}/{n}：实例切片后 {len(insts)}，"
              f"清单 {manifest_name}（限速预算已等分）", flush=True)
    print(f"[flow] 待消费实例 {len(insts)}（top_n={args.top_n} k={args.k}）", flush=True)

    cache = seed.SeedCache(alias_cache)
    kb = annotate.load_instance_kb(args.instances)
    sink = annotate.ManifestSink(args.dataset, manifest_name=manifest_name)
    print(f"[flow] sink 去重索引 {sink.load_index()} 条 "
          f"（清单 {sink.manifest}）", flush=True)
    reconfigure_endpoint("demiwtg_vlm",
                         max_connections=args.vlm_concurrency + 8)

    # 算子列表 = 管线声明（策略默认值在算子类上，此处只覆盖并发）
    stages = [
        seed.SeedStage(cache),
        search.SearchStage(args.top_n, args.k),
        download.DownloadStage(args.blob_root or args.dataset),
        annotate.AnnotateSinkStage(sink, kb),
    ]
    concurrency = {
        "seed": (args.instance_concurrency, args.instance_concurrency * 4),
        "search": (args.search_concurrency, args.download_concurrency * 4),
        "download": (args.download_concurrency, args.vlm_concurrency),
        "annotate_sink": (args.vlm_concurrency, None),   # 深度=并发（字节上界）
    }

    t0 = time.time()
    n = len(insts)

    def on_progress(engine_stats) -> None:
        done = engine_stats.stage("seed")["in"]
        if done % SAVE_EVERY == 0:
            cache.save()               # 断点续跑第三层：词表增量落盘
        if done % args.log_every == 0 and done:
            rate = done / (time.time() - t0) if time.time() > t0 else 0.0
            print(f"[进度] {done}/{n}（{rate:.1f} 实例/s） "
                  f"sunk={engine_stats.emitted} "
                  f"认缺={sum(engine_stats.miss.values())}", flush=True)

    def on_drain(engine_stats) -> None:
        cache.save()                   # 同步落盘最前（中断路径 await 可能截断）

    engine_stats = run_stages(local_data(), insts, stages,
                              concurrency=concurrency,
                              on_progress=on_progress, on_drain=on_drain,
                              log_every=args.log_every)
    elapsed = time.time() - t0
    print(f"[flow] 完成，耗时 {elapsed/60:.1f} 分钟")
    print(f"[flow] 落盘 {engine_stats.emitted} 行；"
          f"打标 {stages[3].annotated} 条")
    print(f"[flow] 引擎口径：{engine_stats.summary()}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[flow] 中断（词表/已落盘数据均已保存，重跑续上）")
