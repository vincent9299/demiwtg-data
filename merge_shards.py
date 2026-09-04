"""分片清单合并入口（编排侧维护工具，2026-09-04·D1）：
meta/metadata-shard-*.jsonl → (sha256, instances) 去重合并为全量清单。

用法：python3 merge_shards.py [--dataset DIR] [--pattern GLOB]
                              [--output NAME] [--dry-run]
"""

from __future__ import annotations

import argparse
import os

from operators.annotate import merge_manifests

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")


def main() -> None:
    p = argparse.ArgumentParser(description="分片清单合并（去重 + 原子写）")
    p.add_argument("--dataset", default=DEFAULT_DATASET)
    p.add_argument("--pattern", default="metadata-shard-*.jsonl")
    p.add_argument("--output", default="metadata.jsonl")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    r = merge_manifests(args.dataset, pattern=args.pattern,
                        output=args.output, dry_run=args.dry_run)
    print(f"[merge] 分片 {r['shards']} 个：读入 {r['input_rows']} 行 → "
          f"输出 {r['output_rows']} 行（去重 {r['dup_dropped']}）"
          + ("（dry-run 未落盘）" if args.dry_run else ""))


if __name__ == "__main__":
    main()
