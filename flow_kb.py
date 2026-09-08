"""kb 线编排（纯声明，2026-09-08）：Wikipedia dump → Concept 正文源清单。

与 flow.py（image/docs 采集线）的关系：kb 线是概念知识库的第一段
（Wikipedia 提供概念正文），生命周期为一次性批处理（dump 是快照，
非持续采集），故独立入口不并进 flow.py 的 main 组合。

管线（三级，全部 map_async 推模式）：
  from_iter(iter_dump_pages)     dump 惰性流（from_iter 新源：全量不物化，
                                 背压经有界队列反传回生成器暂停解析）
  → WikiParseStage               wikitext 结构解析（线程池并发）
  → PagesSinkStage               kb/pages-{lang}[-shard].jsonl 幂等落盘

分片：--shard i/N 按流位置切片（islice 步长），配分片清单名；跨机各分片
单写者，收尾合并沿用 merge_shards 模式（kb 合并脚本随 Phase 2 落地）。
重跑：清单 (lang,title) 键幂等，中断续跑零重复。

运行：PYTHONPATH=<仓库根> python3 flow_kb.py --dump <path> --lang zh
"""

from __future__ import annotations

import argparse
import os
import time

# 环境代理残留清除（flow.py 同款口径：建客户端之前必须清掉）
for _k in list(os.environ):
    if "proxy" in _k.lower():
        del os.environ[_k]

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="kb 线：Wikipedia dump 解析编排")
    p.add_argument("--dump", required=True,
                   help="multistream pages-articles dump 路径（.xml.bz2）")
    p.add_argument("--lang", default="zh",
                   help="语种标签（zh/en；清单命名与 Concept lang 字段用）")
    p.add_argument("--dataset", default=DEFAULT_DATASET,
                   help="清单根（追加型写入走本地盘，DEPLOY 口径）")
    p.add_argument("--limit", type=int, default=0,
                   help="最多处理页数（0=全量；验证/抽样用）")
    p.add_argument("--offset", type=int, default=0,
                   help="跳过前 N 页（流位置口径，非 title 排序）")
    p.add_argument("--shard", default="", metavar="I/N",
                   help="分片运行：按流位置步长切片 + 分片清单名")
    p.add_argument("--manifest-name", default="",
                   help="覆盖清单名（缺省 pages-{lang}[-shard]；磁盘受限机的"
                        "分块顺序执行用：pages-en-partN.jsonl 逐块归档腾空间）")
    p.add_argument("--merge-shards", action="store_true",
                   help="合并 kb/ 下该语种全部分片清单后退出（收尾动作用；"
                        "各机分片清单 scp 汇到一台后执行）")
    p.add_argument("--parse-concurrency", type=int, default=4)
    p.add_argument("--log-every", type=int, default=5000)
    return p.parse_args()


def _page_factory(args):
    """from_iter 的 factory 体声明：dump 流 + 切片窗口（闭包零状态，每次
    动作产新流；limit/offset/shard 全部叠加在迭代器层面）。"""
    import itertools
    from operators.wiki_dump import iter_dump_pages

    def factory():
        it = iter_dump_pages(args.dump, lang=args.lang)
        if args.offset > 0:
            it = itertools.islice(it, args.offset, None)
        if args.shard:
            i, n = (int(x) for x in args.shard.split("/"))
            it = itertools.islice(it, i, None, n)
        if args.limit > 0:
            it = itertools.islice(it, args.limit)
        return it

    return factory


def main() -> None:
    args = parse_args()
    from operators.wiki_dump import (PagesSinkStage, WikiParseStage,
                                     merge_page_shards)
    from demiflow.standalone import local_data

    if args.merge_shards:
        r = merge_page_shards(args.dataset, args.lang)
        print(f"[flow_kb] 合并 {r['shards']} 个分片：{r['input_rows']} 行 → "
              f"{r['output_rows']} 行（去重 {r['dup_dropped']}）")
        return

    manifest_name = f"pages-{args.lang}.jsonl"
    if args.shard:
        try:
            i, n = (int(x) for x in args.shard.split("/"))
            assert 0 <= i < n
        except Exception:
            raise SystemExit(f"--shard 需为 I/N 形式（收到 {args.shard!r}）")
        manifest_name = f"pages-{args.lang}-shard-{i}-of-{n}.jsonl"
    if args.manifest_name:
        manifest_name = args.manifest_name

    sink = PagesSinkStage(args.dataset, args.lang, manifest_name)
    parse_stage = WikiParseStage()
    parse_stage.concurrency = args.parse_concurrency
    parse_stage.queue_depth = args.parse_concurrency * 2

    t0 = time.time()
    stats = (local_data()
             .from_iter(_page_factory(args))
             .map_async(parse_stage)
             .map_async(sink)
             .run_stream(log_every=args.log_every))
    elapsed = time.time() - t0
    print(f"[flow_kb] 完成，耗时 {elapsed/60:.1f} 分钟；"
          f"新落盘 {sink.sunk} 页（重复/越窗跳过见认缺计数）")
    print(f"[flow_kb] 引擎口径：{stats.summary()}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[flow_kb] 中断（已落盘清单保留，重跑按 (lang,title) 续传）")
