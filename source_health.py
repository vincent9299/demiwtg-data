#!/usr/bin/env python3
"""源健康度报告 v2：折源终态后上游引擎的健康口径。

数据源（适配折源架构）：
- 产量侧：七机 image/docs 清单的 source 分布（真实产出）
- 启用侧：本机 searxng /config 启用引擎集
- 判定：启用但全队零产出的引擎 = 死透候选（bang 探测复核后杀）；
  lifecycle 记 dead_since，> 72h 进金丝雀复活名单（单机重试→观察→
  转正或再杀）
动作（agent 巡检执行，人工门）：杀/复活 = settings + webgate 重启
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from datetime import datetime, timezone

MACHINES = {"local": "", "A": "pipeline-a", "B": "pipeline-b",
            "C": "pipeline-c", "D": "pipeline-d",
            "E": "pipeline-e", "F": "pipeline-f"}
LIFE = "/lhcos-data/demiwtg-data/telemetry/source_lifecycle.json"
OUT = "/lhcos-data/demiwtg-data/telemetry/source_health.json"
CN_KEY = os.path.expanduser("~/.ssh/cn_key")
PROBATION_AFTER_H = 72


def fleet_manifest_sources():
    """七机清单 → {source: 行数}（image+docs 合并口径）。"""
    out = {}
    for tag, host in MACHINES.items():
        base = "/home/ubuntu/lake/meta"
        for pat in ("image-shard-*.jsonl", "docs-shard-*.jsonl"):
            cmd = (f"cat {base}/{pat}" if not host else
                   f"ssh -i {CN_KEY} -o ConnectTimeout=15 -o BatchMode=yes "
                   f"{host} 'cat {base}/{pat}'")
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
                s = d.get("source") or d.get("authority") or "?"
                out[s] = out.get(s, 0) + 1
    return out


def local_enabled_engines():
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8080/config",
            headers={"User-Agent": "demiwtg-collector/0.1"})
        d = json.loads(urllib.request.urlopen(req, timeout=6).read())
        return {e["name"] for e in d.get("engines", []) if e.get("enabled")}
    except Exception:                    # noqa: BLE001
        return set()


def main():
    yield_map = fleet_manifest_sources()
    enabled = local_enabled_engines()
    now = datetime.now(timezone.utc).isoformat()
    life = json.load(open(LIFE)) if os.path.exists(LIFE) else {}

    zero_yield = sorted(e for e in enabled if yield_map.get(e, 0) == 0)
    health = {}
    probation_due = []
    for e in sorted(enabled):
        rows = yield_map.get(e, 0)
        state = "alive"
        st = life.get(e, {})
        if e in zero_yield:
            state = "dead-candidate"
            if not st.get("dead_since"):
                st["dead_since"] = now
            age_h = (datetime.now(timezone.utc)
                     - datetime.fromisoformat(st["dead_since"])).total_seconds() / 3600
            if age_h >= PROBATION_AFTER_H and not st.get("probation"):
                st["probation"] = now
                probation_due.append(e)
        else:
            st.pop("dead_since", None)
            st.pop("probation", None)
        st["last_seen"] = now
        st["rows"] = rows
        life[e] = st
        health[e] = {"rows": rows, "state": state}

    json.dump(life, open(LIFE, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    json.dump({"ts": now, "enabled": len(enabled),
               "yield_map": yield_map, "engines": health,
               "dead_candidates": zero_yield,
               "probation_due": probation_due},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    print(f"[健康] 启用 {len(enabled)}｜有产出 {len(enabled)-len(zero_yield)}"
          f"｜零产出候选 {len(zero_yield)}｜待复活 {len(probation_due)}")
    if zero_yield:
        print("  零产出:", ", ".join(zero_yield[:10]))
    top = sorted(yield_map.items(), key=lambda x: -x[1])[:8]
    print("  产量 Top8:", ", ".join(f"{k}={v}" for k, v in top))


if __name__ == "__main__":
    main()
