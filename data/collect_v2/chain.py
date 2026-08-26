"""collect_v2 链：算子链最后一环，业务编排层（**零数据处理逻辑**，用户修正后的硬约束：
chain 不基于 Item 业务字段做判断/筛选/改写，只读计数除外；
拓扑/并发/认缺统计本就是编排职责）。

契约（.qoder/handoff_collect_v2.md §6.1 + 2026-08-20 用户拍板；
2026-08-21 级间流水线化，方案与语义对账见 .qoder/chain_pipeline_phase0.md）：
- 实例流驱动：instances.json → op_coverage（启动期覆盖过滤）
  → 投影循环（op_seed.project → getsource.route → 投递 q_pairs）
  → search worker 组（op_search，top_n 固定切片）
  → download worker 组（op_download）
  → annotate worker 组（op_annotate + op_sink 合并一级）；
  数据处理全在算子内，chain 只做拓扑衔接、并发分配、计数、认缺归集；
- 阶段级流水线（取代旧 (seed, 源) 条内串行管道）：三队列 + 三 worker 组
  各自独立并发，worker 数即该级并发封顶（无独立信号量）；VLM 在飞数只由
  --vlm-concurrency 决定，与下载速率解耦；sentinel（None）逐级注入收尾；
- top_n（用户拍板策略参数）：每 (seed, 源) 检索后取源原生序前 N 条候选，
  **固定切片、无补位**（下载失败不拉 rank N+1 替补），默认 2，CLI 可调；
- 认缺：检索/下载 InfraError、0 候选、下载拒收（None）只计数继续，不断链；
- stats.instances 口径=投影投递完毕（下游可能仍在飞）：进度行速率为投递
  速率，下游真值由同行 落盘/下载 计数呈现；
- 续跑：op_coverage 实例级覆盖跳过（--skip-covered，从真相清单现算不落盘）+
  sink (sha, instance) 去重幂等（同图不同实例追加新行，归因争议留下游仲裁），
  别名词表增量补判 + 定期落盘，中断重跑零成本续上；
- 适合夜跑长驻：周期进度行 + 词表周期保存 + 退出前总账。

运行：PYTHONPATH=<仓库根> python3 -m collect_v2.chain --limit 200
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import time

# 环境代理残留清除（AGENTS.md §7：宕机旧代理会拖死直连池；
# httpx 默认 trust_env 会捡环境代理，必须在建客户端之前清掉）
for _k in list(os.environ):
    if "proxy" in _k.lower():
        del os.environ[_k]

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_INSTANCES = os.path.join(REPO_ROOT, "datasets", "demiwtg", "meta", "instances.json")
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")
DEFAULT_ALIAS_CACHE = os.path.join(REPO_ROOT, "datasets", "demiwtg", "meta", "alias_western.json")

TOP_N_DEFAULT = 2                  # 用户拍板：每 (seed, 源) 下载前 N 条候选
SEARCH_CONCURRENCY_DEFAULT = 24    # search worker 数（有效速率另受 infra 按源闸门封顶）
DOWNLOAD_CONCURRENCY_DEFAULT = 32  # download worker 数（≥ vlm 并发，保下游供给）
VLM_CONCURRENCY_DEFAULT = 48       # annotate worker 数即 VLM 并发（双链实测饱和值，旧默认 4）
SAVE_EVERY = 100                   # 别名词表定期落盘周期（实例数）

import httpx

from collect_v2 import (getsource, infra, op_annotate, op_coverage,
                        op_download, op_search, op_seed, op_sink)


class Stats:
    """链路计数器（认缺归集只计数不建文件：只写不读的账本一律不建）。"""

    def __init__(self):
        self.instances = 0      # 投影投递完毕的实例数（下游可能仍在飞）
        self.pairs = 0          # (seed, 源) 投递对数
        self.hits = 0           # 有召回的 (seed, 源) 对数
        self.candidates = 0     # 进入下载的候选数
        self.downloaded = 0     # 下载成功数
        self.rejected = 0       # 下载拒收数（非图/超限，download 返 None）
        self.annotated = 0      # VLM 标注成功数
        self.sunk = 0           # 落盘数
        self.dup_skipped = 0    # (sha, instance) 完全重复跳过数（2026-08-21 改判后口径，
                                # 同图不同实例是合法追加不计数；仅日志文案旧称「撞车跳过」）
        self.miss: dict[str, int] = {}   # 认缺原因 → 计数

    def add_miss(self, reason: str) -> None:
        self.miss[reason] = self.miss.get(reason, 0) + 1

    def summary(self) -> str:
        miss_txt = "、".join(f"{k}×{v}" for k, v in
                             sorted(self.miss.items(), key=lambda x: -x[1]))
        return (f"实例={self.instances} 投递对={self.pairs} 有召回={self.hits} "
                f"候选={self.candidates} 下载成功={self.downloaded} "
                f"拒收={self.rejected} 标注={self.annotated} "
                f"落盘={self.sunk} 完全重复跳过={self.dup_skipped}"
                + (f"\n认缺：{miss_txt}" if miss_txt else ""))


async def search_worker(q_pairs, q_cands, *, k: int, top_n: int,
                        stats: Stats) -> None:
    """search 级：取 (seed, 源) → 检索 → top_n 固定切片逐条放行（认缺只计数）。"""
    while True:
        t = await q_pairs.get()
        if t is None:                          # sentinel：投影已全部投递
            return
        seed, source = t
        try:
            items = await op_search.search(seed, source, k=k)
        except (infra.InfraError, httpx.HTTPError) as exc:
            # 同下载路径：读流阶段原样上抛的网络异常也认缺不断链
            stats.add_miss(f"search:{type(exc).__name__}")
            continue
        if not items:
            stats.add_miss("search:empty")
            continue
        stats.hits += 1
        for it in items[:top_n]:               # 固定切片无补位：失败不拉 rank N+1
            stats.candidates += 1
            await q_cands.put(it)


async def download_worker(q_cands, q_dl, stats: Stats) -> None:
    """download 级：候选 → 下载，成功者放行打标（拒收/异常只计数）。"""
    while True:
        it = await q_cands.get()
        if it is None:
            return
        try:
            got = await op_download.download(it)
        except (infra.InfraError, httpx.HTTPError):
            # InfraError=分类重试语义；httpx.HTTPError=读流阶段原样上抛的网络异常
            # （infra.stream 契约），两者都认缺不断链（2026-08-21 夜跑崩溃后补防）
            stats.add_miss("download:网络异常")
            continue
        if got is None:                      # 非图/超限拒收（正常流转）
            stats.rejected += 1
            continue
        stats.downloaded += 1
        await q_dl.put(got)


async def annotate_worker(q_dl, *, sink, kb, vlm_client,
                          stats: Stats) -> None:
    """annotate 级：打标 + 落盘合并一级（sink 是毫秒级本地 IO，不值得独立队列）。"""
    while True:
        got = await q_dl.get()
        if got is None:
            return
        # 撞车前置（2026-08-22 拍板）：重试区撞车率 89%，不前置则 ~9 成 VLM 槽位
        # 烧在重复图上；无锁内存快查仅咨询，权威判定仍在 sink 锁内（漏查最坏多打一次标）。
        if sink.contains(got):
            stats.dup_skipped += 1
            continue
        # 不加 try：annotate 内部失败置字段 None 正常返回；真异常上抛终止整链
        # （与旧版崩溃语义一致）。打标失败不阻断落盘（kb_match=None 照样 sink）。
        await op_annotate.annotate(got, kb, client=vlm_client)
        if got.kb_match is not None:
            stats.annotated += 1
        if await sink.sink(got):
            stats.sunk += 1
        else:
            stats.dup_skipped += 1


def load_instances(path: str) -> list[dict]:
    """读实例表全量（原表序，只留有名字的）；筛选/切片在 main 编排。"""
    doc = json.loads(open(path, encoding="utf-8").read())
    return [i for i in doc.get("instances", []) if i.get("name")]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="collect_v2 算子链（零数据处理逻辑，纯编排；适合夜跑长驻）")
    p.add_argument("--instances", default=DEFAULT_INSTANCES)
    p.add_argument("--dataset", default=DEFAULT_DATASET)
    p.add_argument("--alias-cache", default=DEFAULT_ALIAS_CACHE)
    p.add_argument("--limit", type=int, default=0,
                   help="消费实例数（0=全量）")
    p.add_argument("--offset", type=int, default=0,
                   help="跳过前 N 个实例（续跑用）")
    p.add_argument("--skip-covered", type=int, default=0, metavar="N",
                   help="已有 ≥N 张合格图的实例启动期整体跳过（op_coverage 从"
                        "真相清单现算，0=不启用；先全覆盖用 1，回补缺口用更大值）")
    p.add_argument("--min-quality", type=float, default=8.0,
                   help="覆盖口径质量门：只数 quality>=该值的行（0=不门；"
                        "缺 quality 字段按不合格；2026-08-23 默认口径拍板 8）")
    p.add_argument("--require-identity", action=argparse.BooleanOptionalAction,
                   default=True,
                   help="覆盖口径要求 identity=true（--no-require-identity 关）")
    p.add_argument("--top-n", type=int, default=TOP_N_DEFAULT,
                   help="每 (seed, 源) 下载前 N 条候选（默认 2，用户拍板）")
    p.add_argument("--k", type=int, default=op_search.K_SEMANTIC,
                   help="每源检索候选数封顶")
    p.add_argument("--vlm-concurrency", type=int,
                   default=VLM_CONCURRENCY_DEFAULT,
                   help="annotate worker 数，即 VLM 调用并发（旧信号量由 worker 数等价替代）")
    p.add_argument("--search-concurrency", type=int,
                   default=SEARCH_CONCURRENCY_DEFAULT,
                   help="search worker 数；有效速率受 infra 按源闸门封顶，调大只增排队")
    p.add_argument("--download-concurrency", type=int,
                   default=DOWNLOAD_CONCURRENCY_DEFAULT,
                   help="download worker 数；应 ≥ vlm-concurrency（上游供给 ≥ 下游消费）")
    p.add_argument("--instance-concurrency", type=int, default=16,
                   help="实例级投影并发（源限速由 infra 闸门把关，不会打爆源）；"
                        "投影产出 (seed, 源) 对，是全链路供给的源头")
    p.add_argument("--shuffle", type=int, default=None, metavar="SEED",
                   help="按给定随机种子打乱实例顺序（抽样看 case 用）")
    p.add_argument("--log-every", type=int, default=20,
                   help="每 N 个实例输出一行进度")
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    insts = load_instances(args.instances)
    if args.skip_covered > 0:
        counts = op_coverage.load_coverage(
            args.dataset, min_quality=args.min_quality,
            require_identity=args.require_identity)
        insts, skipped = op_coverage.filter_uncovered(
            insts, counts, args.skip_covered)
        gate = (f"quality>={args.min_quality:g}"
                + ("、identity" if args.require_identity else ""))
        print(f"[chain] 覆盖过滤（{gate}）：跳过 {skipped} 个已有 "
              f"≥{args.skip_covered} 张合格图的实例，剩 {len(insts)} 待消费"
              f"（清单合格覆盖 {len(counts)} 实例）", flush=True)
        # 0 张实例排队首、有存量（1~N-1 张）的难啃实例沉底（2026-08-22 拍板）：
        # 重试区实测撞车 89%、实例速率只有干净区的一半不到，先吃干净区；
        # 稳定分区保原表序，不改变集合本身（跳与不跳仍由阈值决定）。
        head = [i for i in insts if counts.get(i.get("name") or "", 0) == 0]
        tail = [i for i in insts if counts.get(i.get("name") or "", 0) > 0]
        insts = head + tail
    if args.shuffle is not None:
        random.Random(args.shuffle).shuffle(insts)
    insts = insts[args.offset:]
    if args.limit > 0:
        insts = insts[:args.limit]
    print(f"[chain] 待消费实例 {len(insts)}（top_n={args.top_n} k={args.k} "
          f"vlm并发={args.vlm_concurrency} 检索并发={args.search_concurrency} "
          f"下载并发={args.download_concurrency} "
          f"实例并发={args.instance_concurrency}）", flush=True)

    kb = op_annotate.load_instance_kb(args.instances)
    cache = op_seed.SeedCache(args.alias_cache)
    sink = op_sink.Sink(args.dataset)
    print(f"[chain] sink 去重索引 {sink.load_index()} 条 "
          f"（清单 {sink.manifest}）", flush=True)

    # 连接池必须显式给足：httpx 裸默认 keepalive 仅 20，且 _call_vlm 的
    # timeout=600 会连带「等连接」也放大到 10 分钟——夜跑实测 vLLM 关
    # keep-alive 后 CLOSE-WAIT 占坑，48 并发挤少量槽位，吞吐 85→4 张/分
    vlm_client = httpx.AsyncClient(limits=httpx.Limits(
        max_connections=args.vlm_concurrency + 8,
        max_keepalive_connections=args.vlm_concurrency + 8))
    # 级间队列（深度见方案文档 §8）：q_dl 载荷含图字节，深度恰等于 worker 数，
    # 内存上限 = vlm-concurrency × op_download.MAX_BYTES，且 worker 永不空等
    q_pairs = asyncio.Queue(maxsize=args.search_concurrency * 4)
    q_cands = asyncio.Queue(maxsize=args.download_concurrency * 4)
    q_dl = asyncio.Queue(maxsize=args.vlm_concurrency)
    inst_sem = asyncio.Semaphore(args.instance_concurrency)
    stats = Stats()
    t0 = time.time()

    async def worker(inst: dict) -> None:
        """投影循环：投影 → 域路由 → 逐对投递 q_pairs 后即计数（下游可能仍在飞）。"""
        async with inst_sem:
            name = inst["name"]
            seeds = await op_seed.project(
                name, inst.get("aliases") or [], cache,
                desc=inst.get("desc") or "", client=vlm_client)
            pairs = []
            for sd in seeds:
                pairs.extend(getsource.route(sd))
            stats.pairs += len(pairs)
            for sd, source in pairs:
                await q_pairs.put((sd, source))
            stats.instances += 1
            if stats.instances % SAVE_EVERY == 0:
                cache.save()
            if stats.instances % args.log_every == 0:
                elapsed = time.time() - t0
                rate = stats.instances / elapsed if elapsed > 0 else 0.0
                print(f"[进度] {stats.instances}/{len(insts)} "
                      f"（{rate:.1f} 实例/s） 落盘={stats.sunk} "
                      f"下载={stats.downloaded} 完全重复跳过={stats.dup_skipped}",
                      flush=True)

    search_tasks = [asyncio.create_task(search_worker(
        q_pairs, q_cands, k=args.k, top_n=args.top_n, stats=stats))
        for _ in range(args.search_concurrency)]
    download_tasks = [asyncio.create_task(download_worker(q_cands, q_dl, stats))
                      for _ in range(args.download_concurrency)]
    annotate_tasks = [asyncio.create_task(annotate_worker(
        q_dl, sink=sink, kb=kb, vlm_client=vlm_client, stats=stats))
        for _ in range(args.vlm_concurrency)]

    try:
        await asyncio.gather(*(asyncio.create_task(worker(i)) for i in insts))
        # 投影投递完毕 → sentinel 逐级注入：先 join 上游 worker 组，
        # 再向其下游队列每 worker 投一个 None（线性链、消费侧常驻，无死锁）
        for _ in search_tasks:
            await q_pairs.put(None)
        await asyncio.gather(*search_tasks)
        for _ in download_tasks:
            await q_cands.put(None)
        await asyncio.gather(*download_tasks)
        for _ in annotate_tasks:
            await q_dl.put(None)
        await asyncio.gather(*annotate_tasks)
    finally:
        cache.save()
        await vlm_client.aclose()
        await infra.close_client()
    elapsed = time.time() - t0
    print(f"[chain] 完成，耗时 {elapsed/60:.1f} 分钟")
    print(stats.summary())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[chain] 中断（词表/已落盘数据均已保存，重跑续上）")
