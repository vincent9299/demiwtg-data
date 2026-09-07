#!/usr/bin/env python3
"""饥饿检测报告：聚合各机配额盘点 + 概念覆盖 → 饥饿概念清单。

巡检轮调用（或手动）：python3 starvation_report.py
产出：telemetry/starvation_report.json + 终端摘要
用途：领域→源路由的扩源触发器——饥饿概念聚集的 taxonomy 分支 =
该领域现有源覆盖不足，agent 据此补源（改 domain_sources.json 或写新引擎）。
"""

from __future__ import annotations

import glob
import json
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone

MACHINES = {
    "local": ("", "/home/ubuntu/lake/meta"),
    "A": ("pipeline-a", "/home/ubuntu/lake/meta"),
    "B": ("pipeline-b", "/home/ubuntu/lake/meta"),
    "C": ("pipeline-c", "/home/ubuntu/lake/meta"),
    "D": ("pipeline-d", "/home/ubuntu/lake/meta"),
    "E": ("pipeline-e", "/home/ubuntu/lake/meta"),
    "F": ("pipeline-f", "/home/ubuntu/lake/meta"),
}
OUT = "/lhcos-data/demiwtg-data/telemetry/starvation_report.json"
CN_KEY = os.path.expanduser("~/.ssh/cn_key")


def read_manifests(tag, host, meta_dir):
    """读该机全部 image-shard 清单 → {概念: 行数}。"""
    rows = {}
    for pat in ("image-shard-*.jsonl", "image.jsonl"):
        cmd = (f"cat {meta_dir}/{pat}" if not host else
               f"ssh -i {CN_KEY} -o ConnectTimeout=15 -o BatchMode=yes "
               f"{host} 'cat {meta_dir}/{pat}'")
        try:
            r = subprocess.run(["bash", "-c", cmd], capture_output=True,
                               text=True, timeout=60)
        except subprocess.TimeoutExpired:
            continue
        for line in r.stdout.splitlines():
            try:
                d = json.loads(line)
            except ValueError:
                continue
            for q in (d.get("queries") or {}):
                rows[q] = rows.get(q, 0) + 1
    return rows


def main():
    batch = json.load(open(
        "/lhcos-data/demiwtg-data/concepts_batch_200.json",
        encoding="utf-8"))
    items = batch if isinstance(batch, list) else batch.get(
        "concepts", batch.get("batch", []))
    target = {c["name"]: (c.get("min_images") or 20, c.get("taxonomy") or [])
              for c in items}

    fleet = Counter()
    alive = []
    for tag, (host, meta) in MACHINES.items():
        counts = read_manifests(tag, host, meta)
        if counts:
            alive.append(tag)
            fleet.update(counts)

    starved = []
    branch = Counter()
    for name, (quota, tax) in target.items():
        got = fleet.get(name, 0)
        if got < quota:
            starved.append({"name": name, "got": got, "quota": quota,
                            "taxonomy": tax})
            # 归因到二级分支（路由挂载粒度）
            if len(tax) >= 2:
                branch[" / ".join(t.strip() for t in tax[:2])] += 1

    report = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "machines": alive,
        "concepts_total": len(target),
        "starved_total": len(starved),
        "starved_ratio": round(len(starved) / max(len(target), 1), 3),
        "starved_branches_top": branch.most_common(10),
        "starved_sample": sorted(starved, key=lambda x: x["got"])[:30],
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    json.dump(report, open(OUT, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(f"[饥饿] 机 {len(alive)}/{len(MACHINES)}｜概念 {len(target)}｜"
          f"饥饿 {len(starved)}（{report['starved_ratio']:.0%}）")
    for b, n in branch.most_common(5):
        print(f"  {n:4d}  {b}")
    print(f"报告: {OUT}")


if __name__ == "__main__":
    sys.exit(main())
