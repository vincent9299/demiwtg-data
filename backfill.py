"""补采编排：湖侧 candidates 清单 → blob 复原（2026-09-08，湖机丢图灾后线）。

背景（HANDOFF_20260908）：湖侧 blobs 丢失 196 万，湖机给极简五键清单
{"c","u","s","e","src"}（缺 blob 且带 URL，同 sha 多概念已合并）。
本线**只下载不复原元数据**——湖侧 images.jsonl 全量在册，回灌后按
(sha, concept) join 还原，零丢失。

行契约：
- 读键：c（概念数组）/ u（下载直链）/ s（内容 sha256，校验+落盘名）/
  e（扩展名）/ src（源键，按 operators.download.download_headers_for
  匹配防盗链头表）
- 落盘清单 backfill-shard-*.jsonl（对齐 image 行键）：concepts/source/
  content_url/sha256/ext/blob_path/size_bytes——回灌合并用
- 死信 dead-shard-*.jsonl：{s,u,src,reason}（CDN 签名过期/源删除属
  正常损耗，留档，常规检索线兜底）

闸门：sha256 复验是唯一入库闸门（不符即死信，不落盘）；
单图 20MB 上限；原子写（内容寻址，跨节点并发同内容写安全）。

断点续跑（现算幂等，demi 规范）：跳过 = 本分片 backfill 清单已记的 sha
∪ dead 清单已记的 sha ∪ blob 实存（跨机共享桶他机已下）。

分片：行号 i::N 切片 + 单写者清单（同 flow D2 语义）；
--sources 过滤（CN 源归 CN 机、西方源归 SG 机的路由切法）。

用法：
    PYTHONPATH=<demiflow 仓> python3 -m backfill --candidates \
        ~/candidates.jsonl.gz --shard 0/5 --sources baidu,huaban_api \
        --dataset ~/lake --blob-root /lhcos-data/demiwtg-data/datasets/demiwtg
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import threading
import time

for _k in list(os.environ):
    if "proxy" in _k.lower():
        del os.environ[_k]

from demiflow.collect.fetch import fetch_tiers
from demiflow.collect.store import atomic_write_bytes
from demiflow.data.plan import StreamStage

from operators.download import DOWNLOAD_HARD_TIMEOUT, MAX_DOWNLOAD_BYTES, download_headers_for


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="湖侧 candidates 补采（纯下载线）")
    p.add_argument("--candidates", required=True, help="candidates.jsonl.gz（五键行）")
    p.add_argument("--dataset", required=True, help="清单根（本地盘，追加型写入不适合对象存储）")
    p.add_argument("--blob-root", required=True, help="blob 落盘根（共享存储，跨机内容寻址）")
    p.add_argument("--shard", default="", metavar="I/N", help="分片：行号 I::N 切片（单写者清单）")
    p.add_argument("--sources", default="", help="逗号分隔源键白名单（缺省=全部；机组路由用）")
    p.add_argument("--concurrency", type=int, default=24, help="下载并发（默认 24）")
    p.add_argument("--limit", type=int, default=0, help="本分片最多消费行数（0=不限）")
    p.add_argument("--log-every", type=int, default=200)
    p.add_argument("--retry-dead", action="store_true",
                   help="重试本分片历史死信（清空 dead 清单重下；网络类损耗给一次复活机会）")
    p.add_argument("--synced-ledger", default="",
                   help="已回湖 sha 账本（lake_sync 清理前写入组共享桶；命中即跳过——"
                        "blob 被清理后不重下）")
    return p.parse_args()


def load_candidates(path: str, shard: str, sources: set | None, limit: int) -> list[dict]:
    """读 gz 清单 → 源过滤 → 行号分片 → 切片（纯数据准备，对齐 flow.prepare_inputs）。"""
    rows: list[dict] = []
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            if shard:
                si, sn = (int(x) for x in shard.split("/"))
                if i % sn != si:
                    continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if sources and row.get("src") not in sources:
                continue
            rows.append(row)
    if limit > 0:
        rows = rows[:limit]
    return rows


def load_sha_ledger(path: str, key: str) -> set:
    """jsonl 清单 → sha 集（断点现算：已 done / 已 dead 的跳过面）。"""
    shas: set = set()
    if not os.path.exists(path):
        return shas
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                shas.add(json.loads(line)[key])
            except (json.JSONDecodeError, KeyError):
                continue
    return shas


class BackfillStage(StreamStage):
    """五键行 → blob 落盘（sha 复验闸门）。返回 None = 认缺（死信已记）。"""

    label = "backfill"
    concurrency = 24
    queue_depth = 48

    def __init__(self, blob_root: str, sink: "BackfillSink"):
        self.blob_root = blob_root
        self.sink = sink

    async def __call__(self, row: dict):
        s, ext, src, url = row["s"], row["e"], row["src"], row["u"]
        rel = f"blobs/{s[:2]}/{s}.{ext}"
        # 断点现算第三层：跨机他分片已下的 blob（清单没进本分片也能看到）
        if os.path.exists(os.path.join(self.blob_root, rel)):
            return None
        try:
            got = await fetch_tiers(
                [url], source=src,
                max_bytes=MAX_DOWNLOAD_BYTES,
                hard_timeout=DOWNLOAD_HARD_TIMEOUT,
                headers=download_headers_for(src),
                verify=None)          # sha 闸门在业务面比（非内容类型拒收）
        except Exception as exc:      # noqa: BLE001 - 全部网络类确定性失败
            await self.sink.dead(row, f"net:{type(exc).__name__}")
            return None
        if got is None:
            await self.sink.dead(row, "capped")     # 超字节上限拒收
            return None
        if got.sha256 != s:
            await self.sink.dead(row, "sha_mismatch")
            return None
        import asyncio
        await asyncio.to_thread(
            atomic_write_bytes, os.path.join(self.blob_root, rel), got.data)
        out = {
            "concepts": row["c"], "source": src, "content_url": url,
            "sha256": s, "ext": ext, "blob_path": rel,
            "size_bytes": got.size_bytes,
        }
        await self.sink.record(out)
        return out


class BackfillSink:
    """单写者双清单：backfill 行 + dead 行（线程锁串行追加）。

    非管线级（不进链）：BackfillStage 业务面直调，行不穿链。"""

    def __init__(self, dataset_dir: str, manifest: str, dead: str):
        self.paths = (os.path.join(dataset_dir, "meta", manifest),
                      os.path.join(dataset_dir, "meta", dead))
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(self.paths[0]), exist_ok=True)
        self.done = 0
        self.dead_count = 0

    async def record(self, out: dict) -> None:
        import asyncio
        line = json.dumps(out, ensure_ascii=False) + "\n"
        await asyncio.to_thread(self._append, 0, line)
        self.done += 1

    async def dead(self, row: dict, reason: str) -> None:
        import asyncio
        line = json.dumps(
            {"s": row["s"], "u": row["u"], "src": row["src"], "reason": reason},
            ensure_ascii=False) + "\n"
        await asyncio.to_thread(self._append, 1, line)
        self.dead_count += 1

    def _append(self, which: int, line: str) -> None:
        with self._lock, open(self.paths[which], "a", encoding="utf-8") as f:
            f.write(line)


def main() -> None:
    args = parse_args()
    sources = set(filter(None, args.sources.split(","))) or None
    tag = args.shard.replace("/", "-of-") if args.shard else "all"
    manifest = f"backfill-shard-{tag}.jsonl"
    dead = f"dead-shard-{tag}.jsonl"
    sink = BackfillSink(args.dataset, manifest, dead)

    # 断点现算：done ∪ dead（--retry-dead 复活死信）∪ blob 实存（行级现查）
    done = load_sha_ledger(sink.paths[0], "sha256")
    dead_shas = set() if args.retry_dead else load_sha_ledger(sink.paths[1], "s")
    skip = done | dead_shas
    if args.synced_ledger:
        skip |= load_sha_ledger(args.synced_ledger, "s")

    rows = load_candidates(args.candidates, args.shard, sources, args.limit)
    todo = [r for r in rows if r["s"] not in skip]
    print(f"[backfill] 分片 {args.shard or '0/1'} 源过滤 {sorted(sources) if sources else '全部'}："
          f"{len(rows)} 行，跳过已办/死信 {len(rows) - len(todo)}，待下 {len(todo)}",
          flush=True)
    if not todo:
        print("[backfill] 无待办，退出")
        return

    from demiflow.standalone import local_data
    stage = BackfillStage(args.blob_root, sink)
    stage.concurrency = args.concurrency
    stage.queue_depth = args.concurrency * 2

    t0 = time.time()
    n = len(todo)
    seen = 0

    def on_progress(stats) -> None:
        nonlocal seen
        done_in = stats.stage("backfill")["in"]
        if done_in >= seen + args.log_every:
            seen = done_in
            rate = done_in / max(time.time() - t0, 1e-9)
            print(f"[进度] {done_in}/{n}（{rate:.1f} 行/s） "
                  f"落盘={sink.done} 死信={sink.dead_count}", flush=True)

    (local_data().from_items(todo)
     .map_async(stage)
     .run_stream(on_progress=on_progress, log_every=args.log_every))
    print(f"[backfill] 完成：落盘 {sink.done}、死信 {sink.dead_count}、"
          f"耗时 {(time.time() - t0) / 60:.1f} 分钟；清单 {sink.paths[0]}",
          flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[backfill] 中断（清单/死信均已落盘，重跑续上）")
