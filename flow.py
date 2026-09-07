"""collect_v2 编排（纯声明，2026-09-04·十 dict 行化终态；
2026-09-07 结构化重组：image/docs 双线各自成函数，main 只做组合）。

算子集（各自文件，自包含，策略自声明）：
  seed.SeedStage / concepts.ConceptSeedStage / seed.RowSeedStage
  search.SearchStage            种子行 → 候选行集（路由+引擎自声明）
  download.DownloadStage        候选行 → 图像行（fetch_tiers+verify）
  annotate.AnnotateSinkStage    图像行 → 标注并落盘（含清单契约）
  text_engines.TextSearchStage  种子行 → 页面候选行（相关性门）
  page.PageFetchStage 等        docs 线页面图文一体采集

平台（demiflow）：StreamStage 规范 / Dataset 链式 API（map_stage +
run_stream，退出期资源收尾在终结动作）/ LLM 端点注册表 / HTTP 双池限速 /
scan_counts 续跑现算。

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

SAVE_EVERY = 100           # 词表每 N 实例落盘（断点续跑第三层）


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
    p.add_argument("--k", type=int, default=0,
                   help="每源候选 K（缺省取 operators.search.K_SEMANTIC）")
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


# ---------------------------------------------------------------------------
# 输入准备（编排侧数据筛选，非算子）：加载 → 覆盖现算过滤 → 分片/切片
# ---------------------------------------------------------------------------

def load_coverage(dataset_dir: str, *, min_quality: float = 0,
                  require_identity: bool = False,
                  manifest_name: str = "image.jsonl") -> dict:
    """主清单现算 {实例名: 合格图数}（机制在平台 scan_counts）。

    质量门口径：合格 = quality >= min_quality（缺字段按不合格）且（若启用）
    identity=True；两门全关退化为数全部行。「有图但全不合格」按 0 图继续采。
    """
    from demiflow.collect.resume import scan_counts
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


def prepare_inputs(args) -> dict:
    """加载与筛选：概念/实例 → 覆盖过滤 → 分片 → 切片（纯数据准备）。"""
    from operators import search, concepts as concepts_mod
    concept_mode = bool(args.concepts)
    if concept_mode:
        all_rows, _plan = concepts_mod.load_concepts(args.concepts)
        image_rows = [c for c in all_rows if c["carriers"] != "text"]
        print(f"[flow] 概念批任务：{len(all_rows)} 概念"
              f"（图像线 {len(image_rows)}；text-only 跳过 "
              f"{len(all_rows) - len(image_rows)}，待文本线）", flush=True)
        insts = image_rows
    else:
        import json
        doc = json.loads(open(args.instances, encoding="utf-8").read())
        insts = [i for i in doc.get("instances", []) if i.get("name")]
        all_rows = []
    if args.skip_covered > 0:
        counts = load_coverage(
            args.dataset, min_quality=args.min_quality,
            require_identity=args.require_identity)
        kept = [i for i in insts
                if counts.get(i.get("name") or "", 0) < args.skip_covered]
        gate = (f"quality>={args.min_quality:g}"
                + ("、identity" if args.require_identity else ""))
        print(f"[flow] 覆盖过滤（{gate}）：跳过 {len(insts) - len(kept)} 个已有 "
              f"≥{args.skip_covered} 张合格图的实例，剩 {len(kept)} 待消费",
              flush=True)
        insts = ([i for i in kept if counts.get(i.get("name") or "", 0) == 0]
                 + [i for i in kept if counts.get(i.get("name") or "", 0) > 0])
    if args.shuffle is not None:
        random.Random(args.shuffle).shuffle(insts)
    manifest_name, alias_cache = "image.jsonl", args.alias_cache
    if args.shard:
        try:
            i, n = (int(x) for x in args.shard.split("/"))
            assert 0 <= i < n
        except Exception:
            raise SystemExit(f"--shard 需为 I/N 形式（收到 {args.shard!r}）")
        insts = insts[i::n]                 # 先切分片（词表/闸门语义随分片正确），
        manifest_name = f"image-shard-{i}-of-{n}.jsonl"   # 后 offset/limit——
        alias_cache = f"{args.alias_cache}.shard{i}-of-{n}"  # limit 语义=每分片
        search.scale_engine_limits(n)       # 限速预算等分：N 进程合计不超发
        print(f"[flow] 分片 {args.shard}：实例切片后 {len(insts)}，"
              f"清单 {manifest_name}（限速预算已等分）", flush=True)
    insts = insts[args.offset:]
    if args.limit > 0:
        insts = insts[:args.limit]
    print(f"[flow] 待消费实例 {len(insts)}"
          f"（top_n={args.top_n} k={args.k or search.K_SEMANTIC}）", flush=True)
    return {"insts": insts, "all_rows": all_rows, "concept_mode": concept_mode,
            "manifest_name": manifest_name, "alias_cache": alias_cache}


def _telemetry_dump(dataset_dir: str) -> None:
    """引擎遥测落盘（反爬/性能分析数据源；ops_watch 采样收集）。"""
    try:
        from demiflow.collect.search import dump_engine_telemetry
        import json as _json
        _path = os.path.join(dataset_dir, "meta", "engine_telemetry.json")
        _json.dump({"t": time.time(), "engines": dump_engine_telemetry()},
                   open(_path, "w", encoding="utf-8"), ensure_ascii=False)
    except Exception:  # noqa: BLE001 - 遥测失败不影响主链
        pass


# ---------------------------------------------------------------------------
# 图像线：种子 → 检索 → 下载 → 标注落盘（配额循环多轮）
# ---------------------------------------------------------------------------

def image_line(args, ctx: dict) -> object:
    """图像线：管线声明 + 配额循环。返回最终一轮 StreamStats。"""
    from operators import annotate, concepts as concepts_mod, download, search, seed
    from demiflow.collect.llm import reconfigure_endpoint
    from demiflow.standalone import local_data

    cache = seed.SeedCache(ctx["alias_cache"])
    if ctx["concept_mode"]:
        kb = {c["name"]: {"desc": "", "aliases": c["aliases"]}
              for c in ctx["all_rows"]}   # 概念行无知识文本；KB 块切 docs 层（P1）
    else:
        kb = annotate.load_instance_kb(args.instances)
    reconfigure_endpoint("demiwtg_vlm", max_connections=args.vlm_concurrency + 8)

    # 管线声明（链式 Dataset API）：算子构造时以实例属性覆盖并发/深度
    # （策略默认值在算子类上）。
    def _tune(stage, concurrency, depth):
        stage.concurrency, stage.queue_depth = concurrency, depth
        return stage

    t0 = time.time()
    n = len(ctx["insts"])

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
        _telemetry_dump(args.dataset)

    # 配额一轮 = 新链：算子持 loop 绑定资源（Sink 锁/浏览器），跨轮
    # 复用会炸；重构造后 load_index 吸收上一轮行，去重语义不变。
    def _image_run(rows):
        sink_ = annotate.ManifestSink(args.dataset,
                                      manifest_name=ctx["manifest_name"])
        sink_.load_index()
        seed_stage = (concepts_mod.ConceptSeedStage()
                      if ctx["concept_mode"] else seed.SeedStage(cache))
        line = (
            _tune(seed_stage, args.instance_concurrency,
                  args.instance_concurrency * 4),
            _tune(search.SearchStage(args.top_n,
                                     args.k or search.K_SEMANTIC),
                  args.search_concurrency, args.download_concurrency * 4),
            _tune(download.DownloadStage(args.blob_root or args.dataset),
                  args.download_concurrency, args.vlm_concurrency),
            _tune(annotate.AnnotateSinkStage(sink_, kb),
                  args.vlm_concurrency, None),   # 深度=并发（字节上界）
        )
        stats = (local_data().from_items(rows)
                 .map_async(line[0]).map_async(line[1])
                 .map_async(line[2]).map_async(line[3])
                 .run_stream(on_progress=on_progress, on_drain=on_drain,
                             log_every=args.log_every))
        return line, stats

    stages, engine_stats = _image_run(ctx["insts"])
    quota_passes = max(1, args.quota_passes) if ctx["concept_mode"] else 1
    if quota_passes > 1:
        from operators.concepts import concept_coverage
        manifest_path = os.path.join(args.dataset, "meta", ctx["manifest_name"])
        target = {c["name"]: c["min_images"] for c in ctx["insts"]}
        for p_i in range(quota_passes - 1):
            cov = concept_coverage(manifest_path, set(target))
            under = [c for c in ctx["insts"] if cov[c["name"]] < c["min_images"]]
            met = len(ctx["insts"]) - len(under)
            print(f"[flow] 配额盘点（第 {p_i + 1} 轮后）：{met}/{len(ctx['insts'])} "
                  f"概念达标，重跑 {len(under)} 个不足概念", flush=True)
            if not under:
                break
            _, engine_stats = _image_run(under)
    return engine_stats, stages, t0


# ---------------------------------------------------------------------------
# docs 线（概念模式）：页面图文一体采集（carriers != image 的概念）
# ---------------------------------------------------------------------------

def docs_line(args, ctx: dict) -> tuple:
    """docs 线：检索 → 抓页 → 内联图 → 落盘 + 二轮补检。返回 (stages, stats)。"""
    from operators import concepts as concepts_mod
    from operators.page import DocsSinkStage, InlineImageStage, PageFetchStage
    from operators.seed import RowSeedStage
    from operators.text_engines import TextSearchStage
    from demiflow.standalone import local_data

    def _tune(stage, concurrency, depth):
        stage.concurrency, stage.queue_depth = concurrency, depth
        return stage

    text_rows = [c for c in ctx["all_rows"] if c["carriers"] != "image"]
    if not text_rows:
        return None, None
    shard_tag = (f"{args.shard.replace('/', '-of-')}" if args.shard else "")
    docs_name = (f"docs-shard-{shard_tag}.jsonl" if shard_tag else "docs.jsonl")
    share = args.blob_root or args.dataset
    aliases_map = {c["name"]: c["aliases"] for c in ctx["all_rows"]}

    def _docs_run(rows, seed_stage=None):
        # 管线声明（链式 Dataset API）：docs 线并发策略在此实例化
        stages_list = (
            _tune(seed_stage or concepts_mod.ConceptSeedStage(), 8, 32),
            _tune(TextSearchStage(per_query=3,
                                  aliases_by_name=aliases_map), 8, 48),
            _tune(PageFetchStage(share,
                                 max_pages_per_concept=args.docs_pages), 4, 8),
            _tune(InlineImageStage(share), 8, 16),
            _tune(DocsSinkStage(args.dataset, docs_name), 4, None),
        )
        stats = (local_data().from_items(rows)
                 .map_async(stages_list[0]).map_async(stages_list[1])
                 .map_async(stages_list[2]).map_async(stages_list[3])
                 .map_async(stages_list[4])
                 .run_stream(log_every=args.log_every))
        return list(stages_list), stats

    print(f"[flow] docs 线启动：{len(text_rows)} 概念（含 text-only），"
          f"清单 {docs_name}", flush=True)
    t_stages, t_stats = _docs_run(text_rows)

    # 二轮递归补检：高相关文档不足的概念用扩展词（百科/介绍）
    # 再检索一轮——SERP 检索式补广，与首轮幂等去重
    from operators.concepts import concept_coverage
    docs_path = os.path.join(args.dataset, "meta", docs_name)
    cov = concept_coverage(docs_path, {c["name"] for c in text_rows})
    under = [c for c in text_rows if cov[c["name"]] < 2]
    if under:
        exp_rows = []
        for c in under:
            exp_rows += [{"name": c["name"], "query": f"{c['name']} 百科",
                          "lang": "zh", "top_n_hint": 2},
                         {"name": c["name"], "query": f"{c['name']} 介绍",
                          "lang": "zh", "top_n_hint": 2}]
        print(f"[flow] docs 二轮补检：{len(under)} 概念文档不足"
              f"（<2），扩展词重检", flush=True)
        t_stages, t_stats = _docs_run(exp_rows, RowSeedStage())
    print(f"[flow] docs 线完成：新页 {t_stages[2].pages} 张、"
          f"落 docs {t_stages[4].sunk} 行；"
          f"引擎口径：{t_stats.summary()}", flush=True)
    return t_stages, t_stats


# ---------------------------------------------------------------------------
# main：纯组合（准备 → 图像线 → docs 线 → 汇报）
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    ctx = prepare_inputs(args)
    engine_stats, stages, t0 = image_line(args, ctx)
    if ctx["concept_mode"]:
        docs_line(args, ctx)
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
