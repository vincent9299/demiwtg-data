#!/usr/bin/env python3
"""源健康度报告 v3：折源终态后上游引擎的健康口径。

数据源（适配折源架构）：
- 产量侧：七机 image/docs 清单的 source 分布（真实产出，带 fetched_at
  时间窗：全量/24h/1h）
- 启用侧：本机 searxng /config 启用引擎集，**限定路由可达类目**
  （images+general；videos/it/science 等从不被查询的类目不算候选）
- 判定：
  * 有效采集源数（24h/1h 窗口内有产出的源）——一等巡检指标，
    战略目标：在注册/路由规则上最大化可用源规模（searxng+自定义）
  * 启用且全量零产出 = 死透候选（bang 探测复核后杀）
  * 有历史产出但 24h 零产出 = 降温（观察，不杀）
  * lifecycle 记 dead_since，> 72h 进金丝雀复活名单
动作（agent 巡检执行，人工门）：杀/复活 = settings + webgate 重启
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
from datetime import datetime, timezone

MACHINES = {"local": "", "A": "pipeline-a", "B": "pipeline-b",
            "C": "pipeline-c", "D": "pipeline-d",
            "E": "pipeline-e", "F": "pipeline-f"}
LIFE = "/lhcos-data/demiwtg-data/telemetry/source_lifecycle.json"
OUT = "/lhcos-data/demiwtg-data/telemetry/source_health.json"
TREND = "/lhcos-data/demiwtg-data/telemetry/effective_sources.jsonl"
CN_KEY = os.path.expanduser("~/.ssh/cn_key")
PROBATION_AFTER_H = 72
ROUTABLE = {"images", "general"}   # 路由会实际下 bang 的类目
W24, W1 = 24 * 3600, 3600


def fleet_manifest_sources() -> dict[str, dict]:
    """七机清单 → {source: {rows, w24, w1}}（image+docs 合并口径）。"""
    out: dict[str, dict] = {}
    now = time.time()
    for tag, host in MACHINES.items():
        base = "/home/ubuntu/lake/meta"
        for pat in ("image-shard-*.jsonl", "docs-shard-*.jsonl"):
            cmd = (f"cat {base}/{pat}" if not host else
                   f"ssh -i {CN_KEY} -o ConnectTimeout=20 -o BatchMode=yes "
                   f"-o ControlMaster=auto -o ControlPath=/tmp/kilo/cm-%r@%h-%p "
                   f"-o ControlPersist=300 "
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
                s = _norm(d.get("source") or d.get("authority") or "?")
                m = out.setdefault(s, {"rows": 0, "w24": 0, "w1": 0})
                m["rows"] += 1
                ts = d.get("fetched_at") or 0
                if now - ts <= W24:
                    m["w24"] += 1
                if now - ts <= W1:
                    m["w1"] += 1
    return out


def local_enabled_engines() -> tuple[set[str], set[str]]:
    """(全部启用, 路由可达启用)——类目 ∩ {images, general}。"""
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8080/config",
            headers={"User-Agent": "demiwtg-collector/0.1"})
        d = json.loads(urllib.request.urlopen(req, timeout=6).read())
        all_en, routed = set(), set()
        for e in d.get("engines", []):
            if not e.get("enabled"):
                continue
            all_en.add(e["name"])
            if ROUTABLE & set(e.get("categories") or []):
                routed.add(e["name"])
        return all_en, routed
    except Exception:                    # noqa: BLE001
        return set(), set()


def _norm(name: str) -> str:
    """清单 source 键（下划线归一）↔ 引擎名（空格）对齐。"""
    return name.strip().lower().replace(" ", "_")


def main():
    yield_map = fleet_manifest_sources()
    enabled_all, enabled = local_enabled_engines()
    now = datetime.now(timezone.utc).isoformat()
    life = json.load(open(LIFE)) if os.path.exists(LIFE) else {}

    zero_yield = sorted(e for e in enabled if yield_map.get(_norm(e), {}).get("rows", 0) == 0)
    eff24 = sorted(e for e in enabled if yield_map.get(_norm(e), {}).get("w24", 0) > 0)
    eff1 = sorted(e for e in enabled if yield_map.get(_norm(e), {}).get("w1", 0) > 0)
    cooldown = sorted(e for e in enabled
                      if yield_map.get(_norm(e), {}).get("rows", 0) > 0
                      and e not in eff24)
    health = {}
    probation_due = []
    for e in sorted(enabled):
        m = yield_map.get(_norm(e), {})
        state = "alive" if m.get("w24") else (
            "cooldown" if m.get("rows") else "dead-candidate")
        st = life.get(e, {})
        if e in zero_yield:
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
        st.update({"rows": m.get("rows", 0), "w24": m.get("w24", 0)})
        life[e] = st
        health[e] = {"rows": m.get("rows", 0), "w24": m.get("w24", 0),
                     "w1": m.get("w1", 0), "state": state}

    json.dump(life, open(LIFE, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    json.dump({"ts": now, "enabled": len(enabled),
               "enabled_all": len(enabled_all),
               "effective_24h": eff24, "effective_1h": eff1,
               "cooldown": cooldown,
               "yield_map": yield_map, "engines": health,
               "dead_candidates": zero_yield,
               "probation_due": probation_due},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    with open(TREND, "a", encoding="utf-8") as f:
        f.write(json.dumps({"ts": now, "enabled_routed": len(enabled),
                            "eff_24h": len(eff24), "eff_1h": len(eff1),
                            "dead_candidates": len(zero_yield)},
                           ensure_ascii=False) + "\n")

    print(f"[有效源] 24h {len(eff24)}｜1h {len(eff1)}｜"
          f"启用(路由可达) {len(enabled)}｜降温 {len(cooldown)}")
    print(f"[健康] 死透候选 {len(zero_yield)}｜待复活 {len(probation_due)}")
    if zero_yield:
        print("  零产出:", ", ".join(zero_yield[:12]))
    if cooldown:
        print("  降温:", ", ".join(cooldown[:8]))
    top = sorted(((k, v["w24"]) for k, v in yield_map.items()),
                 key=lambda x: -x[1])[:8]
    print("  24h 产量 Top8:", ", ".join(f"{k}={v}" for k, v in top if v))


if __name__ == "__main__":
    main()
