"""湖侧镜像 → 真 meta 例行合并（2026-09-09）：sync/manifests → datasets/demiwtg/meta。

定位：lake_sync 把节点清单**镜像**到 sync/manifests/（逐字节、未合并），
本模块把镜像**合并成全局总账**——消费端（检索/知识库/清洗管线）只读
meta/，不碰镜像。由 lake_sync 每轮末尾自动调用（--no-merge-meta 跳过），
也可手动 `python3 merge_meta.py --once`。

两类账、两种合并策略（量级决定）：
- docs.jsonl（万行级）：**全量重合并**，原子替换（os.replace），
  同输入同输出、重跑幂等；键 (page_sha, concepts 元组)；
- images.jsonl（285 万行 / 2.6GB，重写不可接受）：**增量追加合并**——
  ① 流扫既有账建键集 (sha256, instances 元组)（坏行容忍）；
  ② 按文件偏移只消费镜像新增字节（完整行语义，尾部残行留下轮）；
  ③ 新键行追加进 meta/images.jsonl（追加不重写）；轮转（远端变小）
  偏移归零重扫，键集兜底防重复追加。断点状态 sync/merge_state.json。
  语义：镜像行是分片原始账（含跨节点重复），meta 是去重全局账——
  与节点 ~/lake/meta → merge_shards → 湖 images.jsonl 同一口径。

不进账的镜像：backfill-shard（blob 复原操作，键已在账；字段面不同）、
dead-shard（死信留档，非账目）。

并发防护：fcntl 锁（sync/.merge_meta.lock）——手动跑与 daemon 轮次
重叠时串行化（否则双方各自建键集后会重复追加）。
"""

from __future__ import annotations

import argparse
import fcntl
import glob
import json
import os

LAKE_ROOT = "/yzp/zhaozy/yangzepeng/0905/demiwtg"
MIRROR_ROOT = f"{LAKE_ROOT}/sync/manifests"
META_ROOT = f"{LAKE_ROOT}/datasets/demiwtg/meta"
DOCS_OUT = f"{META_ROOT}/docs.jsonl"
IMAGES_OUT = f"{META_ROOT}/images.jsonl"
STATE_PATH = f"{LAKE_ROOT}/sync/merge_state.json"
LOCK_PATH = f"{LAKE_ROOT}/sync/.merge_meta.lock"


def _row_key(rec: dict):
    """清单行 → 去重键。image 行实例字段实测为 instances（历史口径），
    backfill/concepts 行为 concepts——统一兼容读取。"""
    inst = rec.get("instances") or rec.get("concepts") or [""]
    return (rec.get("sha256"), tuple(inst))


# ---------------------------------------------------------------------------
# docs：全量重合并（原子替换）
# ---------------------------------------------------------------------------

def merge_docs(dry_run: bool = False) -> dict:
    shard_files = sorted(glob.glob(f"{MIRROR_ROOT}/*/docs*.jsonl"))
    seen: set = set()
    out_lines: list[str] = []
    total = 0
    for sf in shard_files:
        with open(sf, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1
                key = (rec.get("page_sha"),
                       tuple(rec.get("concepts") or [""]))
                if key in seen:
                    continue
                seen.add(key)
                out_lines.append(line)
    if not dry_run and out_lines:
        tmp = f"{DOCS_OUT}.merge.tmp.{os.getpid()}"
        os.makedirs(os.path.dirname(DOCS_OUT), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines) + "\n")
        os.replace(tmp, DOCS_OUT)
    return {"shards": len(shard_files), "input_rows": total,
            "output_rows": len(out_lines),
            "dup_dropped": total - len(out_lines)}


# ---------------------------------------------------------------------------
# images：增量追加合并（偏移 + 键集去重，不重写大账）
# ---------------------------------------------------------------------------

def _load_offsets() -> dict:
    if os.path.exists(STATE_PATH):
        try:
            return json.load(open(STATE_PATH))["offsets"]
        except (json.JSONDecodeError, KeyError, OSError):
            pass
    return {}


def _save_offsets(offsets: dict) -> None:
    tmp = f"{STATE_PATH}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"offsets": offsets}, f)
    os.replace(tmp, STATE_PATH)


def merge_images(dry_run: bool = False) -> dict:
    """image-shard 镜像增量 → meta/images.jsonl 追加。返回计数。

    键集每轮从既有账全量重建（285 万行流扫 ~1 分钟级，小时节拍可承受；
    换取免维护键集缓存的一致性风险）。偏移只推进到已消费完整行。"""
    # ① 既有键集（含本轮尚未落账的历史追加）
    keys: set = set()
    if os.path.exists(IMAGES_OUT):
        with open(IMAGES_OUT, encoding="utf-8") as f:
            for line in f:
                try:
                    keys.add(_row_key(json.loads(line)))
                except json.JSONDecodeError:
                    continue
    # ② 镜像增量消费
    offsets = _load_offsets()
    shard_files = sorted(glob.glob(f"{MIRROR_ROOT}/*/image-shard-*.jsonl"))
    # 镜像文件消失（节点退场）：残留偏移一并清退
    live = set(shard_files)
    for stale in [p for p in offsets if p not in live]:
        del offsets[stale]
    scanned = appended = 0
    out_f = None
    if not dry_run:
        os.makedirs(os.path.dirname(IMAGES_OUT), exist_ok=True)
        out_f = open(IMAGES_OUT, "a", encoding="utf-8")
    try:
        for sf in shard_files:
            size = os.path.getsize(sf)
            off = offsets.get(sf, 0)
            if size < off:            # 轮转/重建：归零重扫（键集防重）
                off = 0
            if size == off:
                continue
            with open(sf, "rb") as f:
                f.seek(off)
                data = f.read()
            cut = data.rfind(b"\n") + 1   # 只消费完整行
            if cut == 0:
                continue
            chunk = data[:cut]
            for line in chunk.decode("utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                scanned += 1
                key = _row_key(rec)
                if key in keys or not key[0]:
                    continue
                keys.add(key)
                appended += 1
                if out_f is not None:
                    out_f.write(line + "\n")
            offsets[sf] = off + len(chunk)
    finally:
        if out_f is not None:
            out_f.close()
    if not dry_run:
        _save_offsets(offsets)
    return {"shards": len(shard_files), "increment_rows": scanned,
            "appended": appended, "ledger_rows": len(keys)}


# ---------------------------------------------------------------------------
# 例行入口（lake_sync 每轮末尾调用；锁防手动/轮次重叠）
# ---------------------------------------------------------------------------

def merge_all(dry_run: bool = False) -> dict:
    os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
    with open(LOCK_PATH, "a") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            return {"docs": merge_docs(dry_run=dry_run),
                    "images": merge_images(dry_run=dry_run)}
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def main() -> None:
    p = argparse.ArgumentParser(description="镜像清单 → 真 meta 总账合并")
    p.add_argument("--once", action="store_true",
                   help="跑一轮后退出（缺省即单轮；daemon 由 lake_sync 内联调用）")
    p.add_argument("--dry-run", action="store_true", help="只统计不落盘")
    args = p.parse_args()
    print(json.dumps(merge_all(dry_run=args.dry_run), ensure_ascii=False))


if __name__ == "__main__":
    main()
