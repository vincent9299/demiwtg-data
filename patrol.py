"""docs 质量巡检（45 分钟轮训）：健康快照 + docs 抽样质量画像 → telemetry/。

每轮产出 patrol.jsonl 一行：
- health：三机行数/重启增量/引擎遥测错误率 TOP（反爬信号）
- docs_sample：随机抽 N 条 docs 行的质量画像（段落数/均段长/图绑定率/
  链密度/标题相关性分）+ 待人工 review 的 URL 清单
- alerts：错误率>30% 的引擎、连续零增长的分片、壳页率>40%

运行：python3 patrol.py [--interval 2700] [--sample 8]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import time

TELE = "/lhcos-data/demiwtg-data/telemetry"
HOSTS = {"local": None, "pipeline-a": "pipeline-a", "pipeline-b": "pipeline-b"}
SAMPLES = os.path.join(TELE, "samples.jsonl")
PATROL = os.path.join(TELE, "patrol.jsonl")

_LINK_RE = re.compile(r"\[[^\]]*\]\([^)]*\)")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def _ssh(host: str, cmd: str) -> str:
    import subprocess
    argv = (["ssh", "-o", "ConnectTimeout=8", host, cmd]
            if HOSTS[host] else ["bash", "-c", cmd])
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=20)
        return r.stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


def health(prev_state: dict) -> dict:
    h = {"hosts": {}, "alerts": []}
    for host in HOSTS:
        img = _ssh(host, "wc -l ~/lake/meta/image-shard-*.jsonl 2>/dev/null "
                         "| tail -1 | awk '{print $1}'")
        docs = _ssh(host, "cat ~/lake/meta/docs*.jsonl 2>/dev/null "
                          "| grep -v .bak | wc -l")
        # 注意：grep -v .bak 在多文件时退化为行过滤；直接 wc 每个再求和
        docs = _ssh(host, "for f in ~/lake/meta/docs*.jsonl; do "
                          "[ -f \"$f\" ] && [[ \"$f\" != *.bak ]] && "
                          "wc -l < \"$f\"; done | awk '{s+=$1} END {print s}'")
        rst = _ssh(host, "grep -c '重启' ~/lake_supervise.log 2>/dev/null")
        proc = _ssh(host, "pgrep -c -f 'python -m flow' 2>/dev/null")
        tel = {}
        try:
            tel = json.loads(_ssh(
                host, "cat ~/lake/meta/engine_telemetry.json "
                      "2>/dev/null") or "{}").get("engines", {})
        except json.JSONDecodeError:
            pass
        h["hosts"][host] = {"image": int(img or 0), "docs": int(docs or 0),
                            "restarts": int(rst or 0),
                            "flow_procs": int(proc or 0)}
        p = prev_state.get(host, {})
        if p and int(img or 0) == p.get("image", -1) and int(img or 0) > 0:
            h["alerts"].append(f"{host}: 图像零增长（{img} 行停滞）")
        if p and int(rst or 0) > p.get("restarts", 0) + 3:
            h["alerts"].append(f"{host}: 重启频繁（+{int(rst)-p.get('restarts',0)}）")
        prev_state[host] = {"image": int(img or 0),
                            "restarts": int(rst or 0)}
        # 引擎反爬画像：错误率>30% 告警
        for eng, t in tel.items():
            er = t.get("error_rate", 0)
            if er > 0.3 and t.get("attempts", 0) >= 10:
                h["alerts"].append(
                    f"{host}/{eng}: 错误率 {er:.0%}（反爬嫌疑）")
    return h


def load_docs_rows(limit: int = 400) -> list:
    """近端 docs 行（本机 + AB 机各取尾部拼合）。"""
    rows = []
    for host in HOSTS:
        out = _ssh(host,
                   "for f in ~/lake/meta/docs*.jsonl; do "
                   "[ -f \"$f\" ] && [[ \"$f\" != *.bak ]] && tail -150 \"$f\"; "
                   "done")
        for line in (out or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows[-limit:]


def passage_metrics(text: str) -> dict:
    """段落质量画像（同 extract_passages 的口径近似）。"""
    blocks = [b for b in re.split(r"\n\s*\n", text or "") if b.strip()]
    lens, link_ratio = [], []
    for b in blocks:
        plain = _LINK_RE.sub("", b)
        plain = re.sub(r"\s+", "", plain)
        total = len(re.sub(r"\s+", "", b))
        if total > 0:
            lens.append(len(plain))
            link_ratio.append(len(plain) / total)
    good = [l for l in lens if l >= 100]
    return {"n_blocks": len(blocks), "n_good": len(good),
            "avg_good_len": round(sum(good) / len(good), 0) if good else 0,
            "avg_link_ratio": round(sum(link_ratio) / len(link_ratio), 3)
            if link_ratio else 1.0}


def sample_docs(sample_n: int) -> dict:
    rows = load_docs_rows()
    if not rows:
        return {"sampled": 0, "note": "docs 尚无产出"}
    random.seed(int(time.time()))
    picks = random.sample(rows, min(sample_n, len(rows)))
    recs, shell = [], 0
    for r in picks:
        page_path = _page_path(r)
        text = ""
        if page_path:
            text = _ssh(_host_of(page_path), f"cat {page_path} 2>/dev/null") \
                if False else _read_shared(page_path)
        m = passage_metrics(text)
        rec = {"concept": (r.get("concepts") or ["?"])[0],
               "authority": r.get("authority"),
               "title": str(r.get("title"))[:40],
               "url": str(r.get("url"))[:80],
               "n_passages": r.get("n_passages", 0),
               "n_images": r.get("n_images", 0), **m}
        if (r.get("n_passages", 0) or 0) <= 1:
            shell += 1
        recs.append(rec)
    shell_rate = round(shell / len(picks), 2)
    out = {"sampled": len(picks), "shell_rate": shell_rate,
           "avg_images": round(sum(r["n_images"] for r in recs)
                               / len(recs), 1), "picks": recs}
    if shell_rate > 0.4:
        out["alert"] = f"壳页率 {shell_rate:.0%}（>40%，质量门或抽取需复查）"
    return out


def _page_path(r: dict) -> str:
    root = "/lhcos-data/demiwtg-data/datasets/demiwtg"
    p = r.get("path") or ""
    return os.path.join(root, p) if p else ""


def _host_of(_p) -> str:
    return "local"


def _read_shared(path: str) -> str:
    try:
        return open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=2700)   # 45 分钟
    ap.add_argument("--sample", type=int, default=8)
    args = ap.parse_args()
    os.makedirs(TELE, exist_ok=True)
    state: dict = {}
    print(f"[patrol] 每 {args.interval}s 巡检：健康+docs 抽样 "
          f"→ {PATROL}", flush=True)
    n = 0
    while True:
        n += 1
        rec = {"t": time.time(), "round": n,
               "health": health(state),
               "docs": sample_docs(args.sample)}
        with open(PATROL, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        alerts = rec["health"]["alerts"]
        docs_alert = rec["docs"].get("alert", "")
        print(f"[patrol#{n}] alerts={len(alerts)}"
              + (f" | {docs_alert}" if docs_alert else ""), flush=True)
        for a in alerts[:5]:
            print(f"  ⚠ {a}", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
