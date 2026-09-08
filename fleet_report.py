#!/usr/bin/env python3
"""舰队采集报表：所有在线源的状态总览。

运行：python3 fleet_report.py          （默认 SG 五台，秒级）
      python3 fleet_report.py --cn     （含 E/F：跨境链路抖动窗口不定，
                                        预热+复用+重试，最坏可能数分钟）

口径（三层漏斗）：
- 查询/候选：各机 ~/lake/meta/engine_telemetry.json 的 searxng 网关聚合
  （attempts=发出查询，results=返回候选）。遥测在 flow 退出时 drain 落盘
  （运行期在内存），快照时刻见 drain 列——查询数是"截至上次 drain 的
  累计"，速率以清单实时口径为准。
- 下载成功：七机清单实读（image 按 source / docs 按 authority），
  窗口 1h/24h 按 fetched_at 精确实时。
- 分组：SG（本机+A/B/C/D，西方池）/ CN（E/F，国内池）。
- 已知缺口：分引擎的查询数在 webgate 侧无统计（searxng 无 metrics），
  列入 backlog；本表查询层是网关聚合。
- CN 读不通=SG→CN 跨境 SSH 链路抖动（banner 超时，时间窗相关），
  E/F 采集本身自治不受影响——不可达≠停采。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

CN_KEY = os.path.expanduser("~/.ssh/cn_key")
MACHINES = [                     # (tag, group, ssh host or "" = local)
    ("local", "SG", ""), ("A", "SG", "pipeline-a"), ("B", "SG", "pipeline-b"),
    ("C", "SG", "pipeline-c"), ("D", "SG", "pipeline-d"),
    ("E", "CN", "pipeline-e"), ("F", "CN", "pipeline-f")]
SEP = "===TELE==="
W1, W24 = 3600, 24 * 3600


def _ssh(host: str, script: str, tries: int = 4) -> str | None:
    # ControlMaster 复用：跨境链路对连续新建 TCP 有隐性限速，复用通道
    # 一次握手后不再触发（09-08 实证：连发 5 台 SG 后 E/F 必挂，复用后连发全通）
    cm = ["-o", "ControlMaster=auto",
          "-o", f"ControlPath=/tmp/kilo/cm-%r@%h-%p",
          "-o", "ControlPersist=300"]
    key = ["-i", CN_KEY] if host in ("pipeline-e", "pipeline-f") else []
    if key:                          # CN 预热：跨境 banner 抖动窗口不定，
        for _ in range(6):           # 热通一次后复用通道即稳
            try:
                r = subprocess.run(
                    ["ssh", *key, *cm, "-o", "ConnectTimeout=25",
                     "-o", "BatchMode=yes", host, "echo ok"],
                    capture_output=True, text=True, timeout=40)
                if r.returncode == 0:
                    break
            except subprocess.TimeoutExpired:
                pass
            time.sleep(5)
    for _ in range(tries):
        cmd = (["ssh", *key, *cm, "-o", "ConnectTimeout=20",
                "-o", "BatchMode=yes", host, script] if host
               else ["bash", "-c", script])
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
            if r.returncode == 0 and r.stdout.strip():
                return r.stdout
        except subprocess.TimeoutExpired:
            pass
        time.sleep(3)
    return None


def fetch_machine(host: str) -> dict:
    """一次 ssh 拉齐：进程态 + 遥测 + 双清单（带 fetched_at/source/size）。"""
    script = (
        'echo "PROC $(pgrep -fc \'[p]ython -m flow\' || echo 0)"; '
        f'cat ~/lake/meta/engine_telemetry.json 2>/dev/null || echo {{}}; echo {SEP}; '
        'cat ~/lake/meta/image-shard-*.jsonl ~/lake/meta/docs-shard-*.jsonl 2>/dev/null'
    )
    out = _ssh(host, script)
    if out is None:
        return {"reach": False}
    head, _, mani = out.partition(SEP)
    lines = head.splitlines()
    proc = 0
    tel = {}
    for ln in lines:
        if ln.startswith("PROC "):
            proc = int(ln.split()[1] or 0)
        elif ln.strip().startswith("{"):
            try:
                tel = json.loads(ln)
            except ValueError:
                pass
    img, doc = [], []
    now = time.time()
    for line in mani.splitlines():
        try:
            d = json.loads(line)
        except ValueError:
            continue
        ts = d.get("fetched_at") or 0
        r = {"src": (d.get("source") or d.get("authority") or "?"),
             "ts": ts, "mb": (d.get("size_bytes") or 0) / 1048576}
        (img if "source" in d else doc).append(r)
    eng = (tel.get("engines") or {}).get("searxng") or {}
    return {"reach": True, "flow": proc, "drain": tel.get("t", 0),
            "queries": eng.get("attempts", 0), "cands": eng.get("results", 0),
            "img": img, "doc": doc}


def wcount(rows: list, w: float) -> int:
    now = time.time()
    return sum(1 for r in rows if now - r["ts"] <= w)


def main() -> None:
    with_cn = "--cn" in sys.argv
    print("═" * 72)
    print(f"舰队采集报表  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
          f"  （源状态 / 漏斗 / 速率，by 机器 & SG/CN）"
          f"{'  [含CN]' if with_cn else '  [SG模式，--cn 含 E/F]'}")
    print("═" * 72)
    data = {tag: (fetch_machine(host) if (with_cn or grp == "SG") else {"reach": False, "skip": True})
            for tag, grp, host in MACHINES}
    now = time.time()

    # ── 机器层 ──
    print(f"\n{'组':<4}{'机':<6}{'flow':<5}{'查询*':>9}{'候选*':>10}"
          f"{'图累计':>8}{'图24h':>7}{'图1h':>6}{'doc累计':>8}{'doc24h':>7}{'MB':>8}")
    gsum = {}
    for tag, grp, _h in MACHINES:
        d = data[tag]
        if d.get("skip"):
            print(f"{grp:<4}{tag:<6}{'-跳过':<5}（SG 模式；--cn 可试读）")
            continue
        if not d.get("reach"):
            print(f"{grp:<4}{tag:<6}{'✗链路':<5}（跨境抖动窗口；采集自治不受影响）")
            continue
        drain = (f"{int((now-d['drain'])/60)}分前" if d.get("drain") else "-")
        it, dt = len(d["img"]), len(d["doc"])
        mb = sum(r["mb"] for r in d["img"])
        print(f"{grp:<4}{tag:<6}{d['flow']:<5}{d['queries']:>9}{d['cands']:>10}"
              f"{it:>8}{wcount(d['img'], W24):>7}{wcount(d['img'], W1):>6}"
              f"{dt:>8}{wcount(d['doc'], W24):>7}{mb:>8.0f}")
        g = gsum.setdefault(grp, {"q": 0, "c": 0, "it": 0, "i24": 0,
                                  "dt": 0, "d24": 0, "mb": 0.0})
        g.update(q=g["q"]+d["queries"], c=g["c"]+d["cands"], it=g["it"]+it,
                 i24=g["i24"]+wcount(d["img"], W24), dt=g["dt"]+dt,
                 d24=g["d24"]+wcount(d["doc"], W24), mb=g["mb"]+mb)
    print(f"\n{'组':<6}{'查询*':>10}{'候选*':>11}{'图累计':>9}{'图24h':>8}"
          f"{'doc累计':>9}{'doc24h':>8}{'MB':>9}")
    for grp, g in gsum.items():
        print(f"{grp:<6}{g['q']:>10}{g['c']:>11}{g['it']:>9}{g['i24']:>8}"
              f"{g['dt']:>9}{g['d24']:>8}{g['mb']:>9.0f}")
    print("* 查询/候选=遥测 drain 快照累计（flow 退出时落盘，见机器行注释；"
          "下载层为清单实读）")
    print("* 累计=该机清单全历史（含 09-07 重分片前的 of-3 时代行；池列="
          "行所在机器组，非引擎当前归属——如 toutiao 的 SG 行系分片重构前"
          "CN 引擎尚在 SG 时代所采）")

    # ── 源层 ──
    src = {}
    for tag, grp, _h in MACHINES:
        d = data[tag]
        if not d.get("reach"):
            continue
        for r in d["img"] + d["doc"]:
            s = src.setdefault(r["src"], {"total": 0, "w24": 0, "w1": 0,
                                          "mb": 0.0, "grp": set()})
            s["total"] += 1
            s["mb"] += r["mb"]
            s["grp"].add(grp)
            if now - r["ts"] <= W24:
                s["w24"] += 1
            if now - r["ts"] <= W1:
                s["w1"] += 1
    print(f"\n{'源':<22}{'池':<7}{'累计行':>8}{'24h':>7}{'1h':>5}{'MB':>9}{'均/h':>7}")
    for s, v in sorted(src.items(), key=lambda x: -x[1]["w24"]):
        if v["total"] < 2 and v["w24"] == 0:
            continue                       # 噪声键（历史残留 1-2 行）
        pool = "/".join(sorted(v["grp"]))
        print(f"{s:<22}{pool:<7}{v['total']:>8}{v['w24']:>7}{v['w1']:>5}"
              f"{v['mb']:>9.0f}{v['w24']/24:>7.1f}")
    print("═" * 72)


if __name__ == "__main__":
    main()
