"""湖侧统一 docs 总账：镜像分片 docs 清单 → docs.jsonl（2026-09-09）。

背景：lake_sync 只做增量镜像（sync/manifests/<节点>/docs*.jsonl 累积
副本），283 概念 × 2.8 万行散在 10 个节点的镜像文件里；湖侧消费端
（检索/知识库）需要一张去重总账——对齐 images.jsonl 在图像线的角色。

口径（与采集端 DocsSinkStage、merge_shards 同构）：
- 去重键 (page_sha, concepts 元组)：同页跨概念为合法多行，
  同 (页, 概念集) 先到先得；
- 节点顺序确定性：按节点名排序遍历，重跑幂等（同输入同输出）；
- 坏行容忍（与读端口径一致）；pid 唯一临时文件 + os.replace 原子发布；
- 只合并不校验页面实存（页面拉取是 lake_sync 的职责，账实对账另做）。

用法（湖 pod 直跑，stdlib only）：
    python3 merge_docs.py                     # 合并 → datasets/demiwtg/docs.jsonl
    python3 merge_docs.py --dry-run           # 只统计不落盘
"""

from __future__ import annotations

import argparse
import glob
import json
import os

LAKE_ROOT = "/yzp/zhaozy/yangzepeng/0905/demiwtg"
MANIFEST_GLOB = f"{LAKE_ROOT}/sync/manifests/*/docs*.jsonl"
OUTPUT = f"{LAKE_ROOT}/datasets/demiwtg/docs.jsonl"


def merge_docs(dry_run: bool = False) -> dict:
    shard_files = sorted(glob.glob(MANIFEST_GLOB))
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
        tmp = f"{OUTPUT}.merge.tmp.{os.getpid()}"
        os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines) + "\n")
        os.replace(tmp, OUTPUT)
    concepts = set()
    for line in out_lines:
        for c in json.loads(line).get("concepts") or []:
            concepts.add(c)
    return {"shards": len(shard_files), "input_rows": total,
            "output_rows": len(out_lines),
            "dup_dropped": total - len(out_lines),
            "concepts": len(concepts)}


def main() -> None:
    p = argparse.ArgumentParser(description="镜像 docs 清单 → 统一总账")
    p.add_argument("--dry-run", action="store_true", help="只统计不落盘")
    args = p.parse_args()
    r = merge_docs(dry_run=args.dry_run)
    print(json.dumps(r, ensure_ascii=False))


if __name__ == "__main__":
    main()
