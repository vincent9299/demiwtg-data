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
from demiflow.data.plan import StreamStage
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
                  manifest_name: str = "image.jsonl") -> dict:
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
                       key_of=lambda r: r.get("concepts") or [])


def filter_uncovered(insts: list, counts: dict, min_images: int) -> tuple:
    """按覆盖度过滤实例（保原表序只选择不改写）。"""
    if min_images <= 0:
        return insts, 0
    kept = [i for i in insts if counts.get(i.get("name") or "", 0) < min_images]
    return kept, len(insts) - len(kept)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="collect_v2 采集编排（demiflow 声明式）")
    p.add_argument("--instances", default=DEFAULT_INSTANCES)
    p.add_argument("--concepts", default="",
                   help="概念批任务模式：concepts_batch json（优先于 --instances）")
    p.add_argument("--docs-pages", type=int, default=20,
                   help="docs 线每概念页面配额（默认 20）")
    p.add_argument("--quota-passes", type=int, default=2,
                   help="配额循环最大轮数（不足 min_images 的概念重跑；引擎结果"
                        "漂移有限，主要靠配额驱动的每行 top_n）")
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
    concept_mode = bool(args.concepts)
    quota_passes = 1
    if concept_mode:
        from operators import concepts as concepts_mod
        all_rows, plan = concepts_mod.load_concepts(args.concepts)
        image_rows = [c for c in all_rows if c["carriers"] != "text"]
        text_only = len(all_rows) - len(image_rows)
        print(f"[flow] 概念批任务：{len(all_rows)} 概念"
              f"（图像线 {len(image_rows)}；text-only 跳过 {text_only}，"
              f"待文本线）", flush=True)
        insts = image_rows
        quota_passes = max(1, args.quota_passes)
    else:
        insts = load_instances(args.instances)
        all_rows, plan = [], {}
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
    manifest_name = "image.jsonl"
    alias_cache = args.alias_cache
    if args.shard:
        try:
            i, n = (int(x) for x in args.shard.split("/"))
            assert 0 <= i < n
        except Exception:
            raise SystemExit(f"--shard 需为 I/N 形式（收到 {args.shard!r}）")
        insts = insts[i::n]                 # 先切分片（词表/闸门语义随分片正确），
        manifest_name = f"image-shard-{i}-of-{n}.jsonl"   # 后 offset/limit——
        alias_cache = f"{args.alias_cache}.shard{i}-of-{n}"  # limit 语义=每分片
        search.scale_engine_limits(n)      # 限速预算等分：N 进程合计不超发
        print(f"[flow] 分片 {i}/{n}：实例切片后 {len(insts)}，"
              f"清单 {manifest_name}（限速预算已等分）", flush=True)
    insts = insts[args.offset:]
    if args.limit > 0:
        insts = insts[:args.limit]
    print(f"[flow] 待消费实例 {len(insts)}（top_n={args.top_n} k={args.k}）", flush=True)

    cache = seed.SeedCache(alias_cache)
    if concept_mode:
        kb = {c["name"]: {"desc": "", "aliases": c["aliases"]}
              for c in all_rows}   # 概念行无知识文本；KB 块切 docs 层（P1）
    else:
        kb = annotate.load_instance_kb(args.instances)
    reconfigure_endpoint("demiwtg_vlm",
                         max_connections=args.vlm_concurrency + 8)

    # 算子列表 = 管线声明（策略默认值在算子类上，此处只覆盖并发）。
    # 每轮重建（配额循环多轮各自事件循环——Sink 持有 loop 绑定的锁，
    # 跨轮复用会炸；重建后 load_index 吸收上一轮行，去重语义不变）
    def _build_stages():
        if concept_mode:
            from operators.concepts import ConceptSeedStage
            seed_stage_ = ConceptSeedStage()
        else:
            seed_stage_ = seed.SeedStage(cache)
        sink_ = annotate.ManifestSink(args.dataset, manifest_name=manifest_name)
        sink_.load_index()
        return [seed_stage_,
                search.SearchStage(args.top_n, args.k),
                download.DownloadStage(args.blob_root or args.dataset),
                annotate.AnnotateSinkStage(sink_, kb)]

    stages = _build_stages()
    sink = stages[3].sink
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
        # 引擎遥测落盘（反爬/性能分析数据源；ops_watch 采样收集）
        try:
            from demiflow.collect.search import dump_engine_telemetry
            import json as _json
            _path = os.path.join(args.dataset, "meta", "engine_telemetry.json")
            _json.dump({"t": time.time(),
                        "engines": dump_engine_telemetry()},
                       open(_path, "w", encoding="utf-8"), ensure_ascii=False)
        except Exception:  # noqa: BLE001 - 遥测失败不影响主链
            pass

    def _run_once(rows):
        nonlocal stages
        stages = _build_stages()
        return run_stages(local_data(), rows, stages,
                          concurrency=concurrency,
                          on_progress=on_progress, on_drain=on_drain,
                          log_every=args.log_every)

    engine_stats = _run_once(insts)
    if concept_mode and quota_passes > 1:
        from operators.concepts import concept_coverage
        manifest_path = os.path.join(args.dataset, "meta", manifest_name)
        target = {c["name"]: c["min_images"] for c in insts}
        for p_i in range(quota_passes - 1):
            cov = concept_coverage(manifest_path, set(target))
            under = [c for c in insts if cov[c["name"]] < c["min_images"]]
            met = len(insts) - len(under)
            print(f"[flow] 配额盘点（第 {p_i + 1} 轮后）：{met}/{len(insts)} "
                  f"概念达标，重跑 {len(under)} 个不足概念", flush=True)
            if not under:
                break
            engine_stats = _run_once(under)
    # ------------------------------------------------------------------
    # docs 线（概念模式）：页面图文一体采集（carriers != image 的概念）
    # ------------------------------------------------------------------
    if concept_mode:
        text_rows = [c for c in all_rows if c["carriers"] != "image"]
        if text_rows:
            from operators.concepts import ConceptSeedStage
            from operators.page import (DocsSinkStage, InlineImageStage,
                                        PageFetchStage)
            from operators.text_engines import TextSearchStage
            shard_tag = (f"{args.shard.replace('/', '-of-')}"
                         if args.shard else "")
            docs_name = (f"docs-shard-{shard_tag}.jsonl" if shard_tag
                         else "docs.jsonl")
            share = args.blob_root or args.dataset
            aliases_map = {c["name"]: c["aliases"] for c in all_rows}

            def _docs_run(rows, seed_stage=None):
                st = [seed_stage or ConceptSeedStage(),
                      TextSearchStage(per_query=3, aliases_by_name=aliases_map),
                      PageFetchStage(share,
                                max_pages_per_concept=args.docs_pages),
                      InlineImageStage(share),
                      DocsSinkStage(args.dataset, docs_name)]
                stats = run_stages(local_data(), rows, st,
                                   concurrency={
                                       "seed": (8, 32),
                                       "text_search": (8, 48),
                                       "pages": (4, 8),
                                       "inline": (8, 16),
                                       "docs_sink": (4, None),
                                   }, log_every=args.log_every)
                return st, stats

            print(f"[flow] docs 线启动：{len(text_rows)} 概念（含 text-only），"
                  f"清单 {docs_name}", flush=True)
            t_stages, t_stats = _docs_run(text_rows)

            # 二轮递归补检：高相关文档不足的概念用扩展词（百科/介绍）
            # 再检索一轮——SERP 检索式补广，与首轮幂等去重
            from operators.concepts import concept_coverage
            docs_path = os.path.join(args.dataset, "meta", docs_name)
            cov = concept_coverage(docs_path,
                                   {c["name"] for c in text_rows})
            under = [c for c in text_rows if cov[c["name"]] < 2]
            if under:
                exp_rows = []
                for c in under:
                    exp_rows += [{"name": c["name"], "query": f"{c['name']} 百科",
                                  "lang": "zh", "top_n_hint": 2},
                                 {"name": c["name"], "query": f"{c['name']} 介绍",
                                  "lang": "zh", "top_n_hint": 2}]
                class _ExpandSeeds(StreamStage):
                    label = "seed"
                    concurrency = 8

                    async def __call__(self, row):
                        return [row]

                print(f"[flow] docs 二轮补检：{len(under)} 概念文档不足"
                      f"（<2），扩展词重检", flush=True)
                t_stages, t_stats = _docs_run(exp_rows, _ExpandSeeds())
            print(f"[flow] docs 线完成：新页 {t_stages[2].pages} 张、"
                  f"落 docs {t_stages[4].sunk} 行；"
                  f"引擎口径：{t_stats.summary()}", flush=True)

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
