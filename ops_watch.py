"""采集护航监控：三机周期采样 + 异常记档（性能/反爬分析数据源）。

采样（默认 5 分钟）写入共享存储 telemetry/：
- samples.jsonl：{t, host, image_rows, docs_rows, blobs, cpu, mem,
                 supervise_restarts, flow_miss, docs_pages, engine_tel}
  （engine_tel 来自各机 ~/lake/meta/engine_telemetry.json，flow drain 周期更新）
- incidents.jsonl：supervise 重启/停摆、清单零增长 >30 分钟、进程消失

运行：python3 ops_watch.py [--interval 300]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time

REPO = os.path.dirname(os.path.abspath(__file__))
TELE = "/lhcos-data/demiwtg-data/telemetry"
HOSTS = {"local": None, "pipeline-a": "pipeline-a", "pipeline-b": "pipeline-b"}


def _run(host: str, cmd: str) -> str:
    """list 形式直传（本地零 shell——远程命令自带引号不再被本地层破坏）。"""
    argv = (["ssh", "-o", "ConnectTimeout=8", host, cmd]
            if HOSTS[host] else ["bash", "-c", cmd])
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=15)
        return r.stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


def sample(host: str, state: dict) -> dict:
    ls = _run(host, "ls ~/lake/meta/ 2>/dev/null | tr '\\n' ' '")
    img = _run(host, "wc -l ~/lake/meta/image-shard-*.jsonl 2>/dev/null | "
                     "tail -1 | awk '{print $1}'")
    docs = _run(host, "wc -l ~/lake/meta/docs*.jsonl 2>/dev/null | tail -1 | "
                      "awk '{print $1}'")
    blobs = _run(host, "df /lhcos-data | tail -1 | awk '{print $3}'")
    up = _run(host, "uptime | sed 's/.*load average: //'")
    restarts = _run(host, "grep -c '重启' ~/lake_supervise.log 2>/dev/null")
    miss = _run(host, "grep '认缺' ~/pipeline/demiwtg-data/logs/supervised_flow.log"
                      " 2>/dev/null | tail -1")
    docs_pages = _run(host, "grep 'docs 线完成' "
                            "~/pipeline/demiwtg-data/logs/supervised_flow.log "
                            "2>/dev/null | tail -1")
    tel = _run(host, "cat ~/lake/meta/engine_telemetry.json 2>/dev/null")
    try:
        tel = json.loads(tel) if tel else {}
    except json.JSONDecodeError:
        tel = {}
    s = {"t": time.time(), "host": host,
         "image_rows": int(img or 0), "docs_rows": int(docs or 0),
         "blobs": int(blobs or 0),
         "load": up, "restarts": int(restarts or 0),
         "flow_miss_tail": miss[:200],
         "docs_tail": docs_pages[:160],
         "engine_tel": tel.get("engines", {}),
         "meta_files": ls}
    # 异常检测：重启计数增长 / 图像零增长
    prev = state.get(host, {})
    if prev and s["restarts"] > prev.get("restarts", 0):
        _incident({"t": s["t"], "host": host, "type": "supervise_restart",
                   "restarts": s["restarts"]})
    if (prev and prev.get("image_rows") == s["image_rows"]
            and s["image_rows"] > 0
            and time.time() - prev.get("t", time.time()) > 1800):
        _incident({"t": s["t"], "host": host, "type": "image_stall",
                   "rows": s["image_rows"]})
    state[host] = s
    return s


def _incident(rec: dict) -> None:
    os.makedirs(TELE, exist_ok=True)
    with open(os.path.join(TELE, "incidents.jsonl"), "a",
              encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"[incident] {rec['host']} {rec['type']}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=300)
    args = ap.parse_args()
    os.makedirs(TELE, exist_ok=True)
    state: dict = {}
    print(f"[ops_watch] 护航启动：{list(HOSTS)} 每 {args.interval}s 采样 → "
          f"{TELE}", flush=True)
    n = 0
    while True:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as ex:
            recs = list(ex.map(lambda h: sample(h, state), HOSTS))
        with open(os.path.join(TELE, "samples.jsonl"), "a",
                  encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        n += 1
        tot = sum(r["image_rows"] for r in recs)
        tot_d = sum(r["docs_rows"] for r in recs)
        print(f"[sample#{n}] 图 {tot} 行 / docs {tot_d} 行 / "
              f"blobs {recs[0]['blobs']}", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
