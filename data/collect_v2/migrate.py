"""collect_v2 存量迁移链（2026-08-21 新增入口，记录驱动，独立于 chain.py）：
images.jsonl 存量图按 v2 口径过一遍 → 追加进 metadata.jsonl（一图多行）。

链路（全部复用既有算子/公共件，op_annotate/op_sink 零改动）：
  读 images.jsonl → 多实例炸开（每 实例×图 一条记录）
  → 读 blob 字节 + sha256 复验（防清单与字节区漂移）
  → VLM 补标（op_backfill：kb_match 重打 + identity/focus 新补；
    无 caption 的行走 op_annotate 全量五字段）
  → queries/query_langs 补全（沿用旧值 → alias_western 西文名 lang=latin
    → 实例名 lang=zh；旧值 'en' 统一为 'latin'）
  → 追加写 metadata.jsonl（v2 最小兼容字段集、保原 fetched_at、
    去重键 (sha256, instance)——对 sink 的 sha 撞车跳过契约定向豁免，
    一图多行是迁移后的合法形态；blob 已存在不写）。

续跑：启动期扫 metadata.jsonl 建 (sha, instance) 索引，已迁移记录跳过。
适合夜跑：周期进度行 + 退出总账。

运行：PYTHONPATH=<仓库根> python3 -m collect_v2.migrate [--limit N]
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import time
from typing import Optional, Set, Tuple

# 环境代理残留清除（AGENTS.md §7；VLM 走 localhost 直连，httpx trust_env
# 会捡环境代理把请求拖死，必须在建客户端之前清掉）
for _k in list(os.environ):
    if "proxy" in _k.lower():
        del os.environ[_k]

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")
DEFAULT_INSTANCES = os.path.join(DEFAULT_DATASET, "meta", "instances.json")
DEFAULT_ALIAS_WESTERN = os.path.join(DEFAULT_DATASET, "meta", "alias_western.json")

VLM_CONCURRENCY_DEFAULT = 16     # 实测 16 并发 ~326 图/min（补标口径）
IO_CONCURRENCY_DEFAULT = 8       # blob 读盘并发（本地大盘，不必太高）
FLUSH_EVERY = 200                # 清单批量追加周期（条）
LOG_EVERY = 2000                 # 进度行周期（条）

# 开源数据集可复得的源：不写入 metadata.jsonl，由用户另走元数据链路单独处理
# （2026-08-21 拍板，A 档 danbooru 双源；本地 datasets/danbooru2024 已有元数据）
EXCLUDE_SOURCES = frozenset({"danbooru", "bulk_danbooru2023"})

import httpx

from collect_v2 import op_annotate, op_backfill
from collect_v2.op_search import Item
from taxonomy.mount_map import load_mount_map, tree_sibling_of

# v2 最小兼容字段集（对齐 op_sink._record_for 的读端识别面；
# v1 特有字段 tiers/source_rank/source_score/source_kind/
# source_authorized/credit/asset_ids 不迁移——无消费者）
RECORD_FIELDS = (
    "sha256", "ext", "source", "license", "author",
    "width", "height", "orig_width", "orig_height",
    "size_bytes", "mime", "instances", "queries", "query_langs",
    "content_url", "landing_url", "fetched_at", "path",
    "kb_match", "richness", "caption", "identity", "focus", "quality",
)


def norm_lang(lang) -> str:
    """query_langs 口径统一：旧值 'en' → 'latin'（op_seed 词表口径）。"""
    lang = str(lang or "").strip().lower()
    return "latin" if lang in ("en", "latin") else (lang or "zh")


def load_done(manifest: str) -> Set[Tuple[str, str]]:
    """扫 metadata.jsonl 建 (sha, instance) 已迁移索引（续跑去重依据）。

    一行一个实例（instances 单元素）；坏行跳过（读端本就容忍）。
    """
    done: Set[Tuple[str, str]] = set()
    if not os.path.exists(manifest):
        return done
    with open(manifest, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            sha = rec.get("sha256")
            if not sha:
                continue
            for inst in (rec.get("instances") or [""]):
                done.add((sha, inst))
    return done


def load_alias_western(path: str) -> dict:
    """alias_western.json → {实例名: 西文投影或 None}（op_seed 专属词表只读消费）。"""
    if not os.path.exists(path):
        return {}
    try:
        return json.loads(open(path, encoding="utf-8").read())
    except (json.JSONDecodeError, OSError):
        return {}


def query_for(row: dict, instance: str, western: dict) -> Tuple[str, str]:
    """(query, lang) 补全：沿用旧值（lang 归一）→ 西文投影 → 实例名 zh。"""
    old_q = (row.get("queries") or {}).get(instance)
    old_l = (row.get("query_langs") or {}).get(instance)
    if old_q:
        return str(old_q), norm_lang(old_l)
    w = western.get(instance)
    if w:
        return str(w), "latin"
    return instance, "zh"


def build_record(row: dict, instance: str, ann: dict, western: dict) -> dict:
    """存量行 + 单实例 + 标注 → metadata.jsonl 记录（最小兼容集，保原 fetched_at）。"""
    query, lang = query_for(row, instance, western)
    rec = {
        "sha256": row.get("sha256"),
        "ext": row.get("ext"),
        "source": row.get("source", ""),
        "license": row.get("license") or "",
        "author": row.get("author"),
        "width": row.get("width"),
        "height": row.get("height"),
        "orig_width": row.get("orig_width"),
        "orig_height": row.get("orig_height"),
        "size_bytes": row.get("size_bytes"),
        "mime": row.get("mime"),
        "instances": [instance],
        "queries": {instance: query},
        "query_langs": {instance: lang},
        "content_url": row.get("content_url"),
        "landing_url": row.get("landing_url"),
        "fetched_at": row.get("fetched_at"),       # 保原采集时间，不写 now
        "path": row.get("path"),
        "kb_match": ann.get("kb_match"),
        "richness": ann.get("richness"),
        "caption": ann.get("caption"),
        "identity": ann.get("identity"),
        "focus": ann.get("focus"),
        "quality": ann.get("quality"),
    }
    return {k: rec.get(k) for k in RECORD_FIELDS}


class Stats:
    def __init__(self):
        self.total = 0          # 炸开后总记录数（本次待迁移）
        self.done = 0           # 本次已迁移（含 VLM 失败放行）
        self.skipped = 0        # 续跑命中已迁移
        self.annotated = 0      # VLM 补标/全量成功
        self.ann_fail = 0       # VLM 失败放行（字段 null）
        self.miss: dict[str, int] = {}

    def add_miss(self, reason: str) -> None:
        self.miss[reason] = self.miss.get(reason, 0) + 1

    def summary(self) -> str:
        miss_txt = "、".join(f"{k}×{v}" for k, v in
                             sorted(self.miss.items(), key=lambda x: -x[1]))
        return (f"待迁移={self.total} 已迁移={self.done} 续跑跳过={self.skipped} "
                f"标注成功={self.annotated} 标注失败放行={self.ann_fail}"
                + (f"\n认缺：{miss_txt}" if miss_txt else ""))


async def process_one(row: dict, instance: str, *, dataset_dir: str,
                      kb: dict, western: dict, vlm_client, vlm_sem,
                      io_sem, stats: Stats) -> Optional[dict]:
    """单条 (图, 实例)：读 blob → sha 复验 → VLM → 组记录。返回 None=认缺。"""
    sha = row.get("sha256")
    rel = row.get("path") or ""
    blob = os.path.join(dataset_dir, rel) if rel else ""
    if not sha or not os.path.exists(blob):
        stats.add_miss("blob缺失")
        return None
    # 读盘 + sha 复验丢线程池（同步阻塞，避免卡事件循环）
    async with io_sem:
        data = await asyncio.to_thread(_read_and_verify, blob, sha)
    if data is None:
        stats.add_miss("sha复验不符")
        return None

    has_caption = bool((row.get("caption") or "").strip())
    async with vlm_sem:
        if has_caption:
            ann = await op_backfill.backfill(
                data, instance, kb, client=vlm_client)
            if ann is not None:
                # richness/caption 实体无关，沿用存量；quality 同权派生
                ann = dict(ann, richness=row.get("richness"),
                           caption=row.get("caption"))
        else:
            item = Item(instance=instance, query="", data=data, sha256=sha)
            await op_annotate.annotate(item, kb, client=vlm_client)
            ann = {"kb_match": item.kb_match, "richness": item.richness,
                   "caption": item.caption, "identity": item.identity,
                   "focus": item.focus}
    if ann.get("kb_match") is None and ann.get("focus") is None:
        stats.ann_fail += 1
    else:
        stats.annotated += 1
    w_kb, w_fo, w_ri = op_annotate.QUALITY_WEIGHTS
    if None not in (ann.get("kb_match"), ann.get("focus"), ann.get("richness")):
        ann["quality"] = round(w_kb * ann["kb_match"] + w_fo * ann["focus"]
                               + w_ri * ann["richness"], 1)
    return build_record(row, instance, ann, western)


def _read_and_verify(blob: str, sha: str) -> Optional[bytes]:
    data = open(blob, "rb").read()
    return data if hashlib.sha256(data).hexdigest() == sha else None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="collect_v2 存量迁移链（images.jsonl → metadata.jsonl，"
                    "一图多行；适合夜跑长驻）")
    p.add_argument("--dataset", default=DEFAULT_DATASET)
    p.add_argument("--instances", default=DEFAULT_INSTANCES)
    p.add_argument("--alias-western", default=DEFAULT_ALIAS_WESTERN)
    p.add_argument("--limit", type=int, default=0,
                   help="本次迁移记录数封顶（0=全量，冒烟用）")
    p.add_argument("--mount-under", default="",
                   help="只迁移挂载在该树子树下的实例（节点名或完整路径前缀，"
                        "如 '虚构角色 IP'；空=全量）。挂载关系经 mount_map 现算，"
                        "不落盘（AGENTS.md 1.5 契约）")
    p.add_argument("--vlm-concurrency", type=int,
                   default=VLM_CONCURRENCY_DEFAULT)
    p.add_argument("--io-concurrency", type=int, default=IO_CONCURRENCY_DEFAULT)
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    meta_dir = os.path.join(args.dataset, "meta")
    src = os.path.join(meta_dir, "images.jsonl")
    manifest = os.path.join(meta_dir, "metadata.jsonl")

    kb = op_annotate.load_instance_kb(args.instances)
    western = load_alias_western(args.alias_western)
    done = load_done(manifest)
    print(f"[migrate] 已迁移索引 {len(done)} 条（清单 {manifest}）", flush=True)

    # --mount-under：从树现算子树挂载实例集，只迁移命中实例的记录。
    # 参数可传节点名（树内按名定位）或完整路径；节点 path 从根算起，
    # 裸名不能直接当前缀匹配
    allow: Optional[Set[str]] = None
    if args.mount_under:
        with open(tree_sibling_of(args.instances), encoding="utf-8") as f:
            tree = json.load(f).get("tree") or {}
        needle = args.mount_under.strip(" /")
        prefixes: list[str] = []

        def _locate(n) -> None:
            path = n.get("path", "")
            if n.get("name") == needle or path == needle:
                if path:
                    prefixes.append(path)
            for ch in n.get("children") or []:
                _locate(ch)

        _locate(tree)
        if not prefixes:
            raise SystemExit(f"[migrate] --mount-under '{needle}' 在树中未找到节点")
        mounts = load_mount_map(tree_sibling_of(args.instances))
        allow = {nm for nm, paths in mounts.items()
                 if any(p == pf or p.startswith(pf + " / ")
                        for p in paths for pf in prefixes)}
        print(f"[migrate] --mount-under '{needle}'（子树根 {prefixes}）："
              f"挂载实例 {len(allow)} 个，只迁移其记录", flush=True)

    # 炸开成任务列表：(row, instance)；无实例行跳过（待认领，另走一条链路）；
    # 开源数据集可复得的源（EXCLUDE_SOURCES）整体跳过，由用户单独处理
    stats = Stats()
    tasks: list[Tuple[dict, str]] = []
    n_rows = n_empty = n_excluded = 0
    with open(src, encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            n_rows += 1
            if row.get("source") in EXCLUDE_SOURCES:
                n_excluded += 1
                continue
            insts = [i for i in (row.get("instances") or []) if i]
            if allow is not None:
                insts = [i for i in insts if i in allow]
            if not insts:
                n_empty += 1
                continue
            for inst in insts:
                if (row.get("sha256"), inst) in done:
                    stats.skipped += 1
                    continue
                tasks.append((row, inst))
    if args.limit > 0:
        tasks = tasks[:args.limit]
    stats.total = len(tasks)
    print(f"[migrate] 存量 {n_rows} 行（无实例 {n_empty} 行跳过，"
          f"开源数据集源 {n_excluded} 行剔除），"
          f"炸开后本次待迁移 {stats.total} 条"
          f"（vlm并发={args.vlm_concurrency} io并发={args.io_concurrency}）",
          flush=True)

    # 连接池给足（chain.py 同款教训：裸默认池在高并发下 CLOSE-WAIT 占坑）
    vlm_client = httpx.AsyncClient(limits=httpx.Limits(
        max_connections=args.vlm_concurrency + 8,
        max_keepalive_connections=args.vlm_concurrency + 8))
    vlm_sem = asyncio.Semaphore(args.vlm_concurrency)
    io_sem = asyncio.Semaphore(args.io_concurrency)

    queue: asyncio.Queue = asyncio.Queue(maxsize=args.vlm_concurrency * 4)
    out_buf: list[str] = []
    out_lock = asyncio.Lock()
    t0 = time.time()

    async def flush(force: bool = False) -> None:
        async with out_lock:
            if not out_buf or (not force and len(out_buf) < FLUSH_EVERY):
                return
            chunk = out_buf[:]
            out_buf.clear()
        def _append():
            with open(manifest, "a", encoding="utf-8") as f:
                f.writelines(chunk)
        await asyncio.to_thread(_append)

    async def worker() -> None:
        while True:
            job = await queue.get()
            if job is None:
                queue.task_done()
                return
            row, inst = job
            try:
                rec = await process_one(row, inst, dataset_dir=args.dataset,
                                        kb=kb, western=western,
                                        vlm_client=vlm_client, vlm_sem=vlm_sem,
                                        io_sem=io_sem, stats=stats)
            except Exception as exc:  # noqa: BLE001 - 单条失败不断链
                stats.add_miss(f"worker:{type(exc).__name__}")
                rec = None
            if rec is not None:
                async with out_lock:
                    out_buf.append(json.dumps(rec, ensure_ascii=False) + "\n")
                await flush()
                stats.done += 1
                if stats.done % LOG_EVERY == 0:
                    elapsed = time.time() - t0
                    rate = stats.done / elapsed if elapsed > 0 else 0.0
                    print(f"[进度] {stats.done}/{stats.total} "
                          f"（{rate*60:.0f} 图/min） 标注={stats.annotated} "
                          f"失败放行={stats.ann_fail} "
                          f"认缺={sum(stats.miss.values())}", flush=True)
            queue.task_done()

    n_workers = args.vlm_concurrency + args.io_concurrency
    workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
    try:
        for row, inst in tasks:
            await queue.put((row, inst))
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)
        await flush(force=True)
    finally:
        await vlm_client.aclose()
    elapsed = time.time() - t0
    print(f"[migrate] 完成，耗时 {elapsed/60:.1f} 分钟")
    print(stats.summary())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[migrate] 中断（已追加的记录均已落盘，重跑续上）")
