"""data_pipeline landing_url 富化编排（纯声明，2026-09-04·十三 合规化）：
主清单 landing_url 去重 → CrawlStage 抓取 → PersistStage 落盘。

链路：
  读 metadata.jsonl → 收集去重 landing_url（http/https）→ 编排侧输入
  过滤（PersistStage 续跑索引的 ok 集合，同 flow 的覆盖过滤位）→
  demiflow run_stages 串联两算子 → state/collect/crawl/ 落盘。

断点续跑：index.jsonl 的 ok 集即续跑索引（失败页不写行→重跑自动重试）；
中断语义与 flow 同款（词表无、页面与索引行均已落盘）。

运行：python3 -m data_pipeline.enrich_landing --limit 100
依赖：crawl4ai（demiflow extras [crawl]）+ playwright 浏览器
"""

from __future__ import annotations

import argparse
import json
import os
import time

for _k in list(os.environ):
    if "proxy" in _k.lower():
        del os.environ[_k]

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")
DEFAULT_STATE = os.path.join(REPO_ROOT, "state", "collect", "crawl")

from data_pipeline.operators.crawl import CrawlStage, PersistStage
from demiflow.standalone import local_data, run_stages


def collect_url_rows(manifest: str) -> list:
    """扫主清单收集去重 landing_url 行（保出现序；坏行容忍）。"""
    rows, seen = [], set()
    if not os.path.exists(manifest):
        return rows
    with open(manifest, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            u = (rec.get("landing_url") or "").strip()
            if u.startswith(("http://", "https://")) and u not in seen:
                seen.add(u)
                rows.append({"url": u})
    return rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="landing_url 富化编排（Crawl4AI 正文抽取；适合夜跑长驻）")
    p.add_argument("--dataset", default=DEFAULT_DATASET)
    p.add_argument("--state", default=DEFAULT_STATE,
                   help="运行时状态目录（默认 state/collect/crawl）")
    p.add_argument("--limit", type=int, default=0,
                   help="本次抓取页数封顶（0=全量）")
    p.add_argument("--concurrency", type=int, default=4,
                   help="页级抓取并发（浏览器 tab 数）")
    p.add_argument("--proxy", default="",
                   help="浏览器出网代理（空=直连；显式传参不读 env）")
    p.add_argument("--log-every", type=int, default=50)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    manifest = os.path.join(args.dataset, "meta", "metadata.jsonl")
    persist = PersistStage(args.state)
    rows = collect_url_rows(manifest)
    todo = [r for r in rows if not persist.contains_url(r["url"])]
    if args.limit > 0:
        todo = todo[:args.limit]
    print(f"[enrich] 清单 landing_url 去重后 {len(rows)} 个，"
          f"已抓（ok）跳过 {len(rows) - len(todo)} 个，本次待抓 {len(todo)} 个"
          f"（并发 {args.concurrency}"
          + (f"，代理 {args.proxy}" if args.proxy else "，直连") + "）",
          flush=True)
    if not todo:
        print("[enrich] 无待抓页面")
        return

    stages = [
        CrawlStage(proxy=args.proxy or None),
        persist,
    ]
    concurrency = {
        "crawl": (args.concurrency, args.concurrency * 2),
        "persist": (8, 16),
    }
    t0 = time.time()
    stats = run_stages(local_data(), todo, stages,
                       concurrency=concurrency, log_every=args.log_every)
    print(f"[enrich] 完成，耗时 {(time.time() - t0)/60:.1f} 分钟："
          f"落盘 {stats.emitted} 页、认缺 {sum(stats.miss.values())}"
          f"（failed 下次重跑重试）")
    print(f"[enrich] 引擎口径：{stats.summary()}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[enrich] 中断（已写页面与索引行均已落盘，重跑续上）")
