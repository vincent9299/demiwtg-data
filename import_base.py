"""base 层导入入口（编排侧）：离线/外部文本 → 同款清洗 → docs 清单。

复用链路：extract_passages（链密度/垃圾图过滤）+ quality_gate（壳页门）
+ DocsSinkStage（docs.jsonl 幂等追加 + pages/ 内容寻址）——与在线采集
完全相同的 operators，authority="offline-dump" 溯源。

输入格式（--input，jsonl 每行）：
  {"concepts": ["极光"], "title": "Aurora",
   "url": "https://en.wikipedia.org/wiki/Aurora",   # 可选（内容寻址键）
   "text": "正文纯文本..."}                          # 必填
（wikipedia dump / wikidata / 任何清洗过的语料适配成此格式即可）

用法：
  python3 import_base.py --input dump.jsonl [--dataset ~/lake] \
      [--blob-root /lhcos-data/...] [--limit 0]
"""

from __future__ import annotations

import argparse
import json
import os
import time

from operators.page import BaseIngestStage, DocsSinkStage
from demiflow.standalone import local_data, run_stages

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATASET = "/lhcos-data/demiwtg-data/datasets/demiwtg"


def main() -> None:
    ap = argparse.ArgumentParser(description="base 层导入（复用在线清洗链路）")
    ap.add_argument("--input", required=True, help="jsonl：{concepts,title,url?,text}")
    ap.add_argument("--dataset", default=os.path.expanduser("~/lake"),
                    help="清单根（本地，与在线采集同款）")
    ap.add_argument("--blob-root", default=DEFAULT_DATASET,
                    help="pages/ 共享根")
    ap.add_argument("--manifest", default="docs.jsonl")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    rows = []
    with open(args.input, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if r.get("text") and r.get("concepts"):
                rows.append(r)
    if args.limit > 0:
        rows = rows[:args.limit]
    print(f"[import] 读入 {len(rows)} 行（壳页/垃圾段将被同款质量门过滤）",
          flush=True)
    if not rows:
        return

    stages = [BaseIngestStage(args.blob_root),
              DocsSinkStage(args.dataset, args.manifest)]
    t0 = time.time()
    stats = run_stages(local_data(), rows, stages,
                       concurrency={"base_ingest": (8, 16),
                                   "docs_sink": (4, None)},
                       log_every=5000)
    print(f"[import] 完成，耗时 {(time.time()-t0)/60:.1f} 分钟："
          f"落 docs {stages[1].sunk} 行；{stats.summary()}")


if __name__ == "__main__":
    main()
