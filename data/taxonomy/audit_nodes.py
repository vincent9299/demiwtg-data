#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""audit_nodes.py — LLM 批量审查标签树脏节点（死叶子），给出 keep/delete 判定。

候选范围：死叶子节点（无 children、无 instances、无 KB 字段、depth>=2）。
这类节点结构上无任何引用，删除零风险；节点名是否合法分类概念交给 LLM 语义判定
（启发式只做数量预估，不参与判定——短名/量词等规则误伤率高）。

有实例挂载或子树的脏节点不在本轮范围（删除涉及重挂/子树策略，另行处理）。

机制（共用 llm_common.py）：
  - OpenAI 兼容端点（LLM_BASE_URL），按父节点分组批量提交，断点续跑
  - 缓存 state/taxonomy/.audit_nodes_cache.jsonl（每节点一行判定）
  - 报告 state/taxonomy/audit_report.md（--report）

用法：
  python3 taxonomy/audit_nodes.py --branch "知识与学科" --dry-run   # 预览 prompt
  export LLM_API_KEY=sk-... LLM_BASE_URL=... LLM_MODEL=...
  python3 taxonomy/audit_nodes.py --branch "知识与学科"            # 试点审查
  python3 taxonomy/audit_nodes.py                                   # 全树审查
  python3 taxonomy/audit_nodes.py --report                          # 生成审查报告（人工抽查）
  python3 taxonomy/audit_nodes.py --apply --write                   # 从 taxonomy.json 删除 delete 节点
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # data/（裸包名 import 约定）

from taxonomy import llm_common as llm

ROOT = Path(__file__).resolve().parent.parent.parent      # 仓库根
TAXONOMY_PATH = ROOT / "datasets" / "demiwtg" / "meta" / "taxonomy.json"
STATE_DIR = ROOT / "state" / "taxonomy"
CACHE_PATH = STATE_DIR / ".audit_nodes_cache.jsonl"
REPORT_PATH = STATE_DIR / "audit_report.md"

KB_FIELDS = ["knowledge_intro", "aliases",
             "representative_cases", "related_tags"]
BATCH = 80   # 每次 LLM 调用判定的节点数

SYSTEM_PROMPT = (
    "你是中文分类标签体系的质量审计员。对每个候选标签名，判断它是否是合法的"
    "『分类概念』（即能作为树节点、指代一类事物/现象/学科/作品类型的类目名）。\n"
    "判 delete 的情况：动宾/动词短语（如『吹小号』『吩咐』『取得』）；"
    "量词或碎片词（如『一份』『一份利润』『一局』）；纯形容词/副词；"
    "含糊空泛无法界定外延的词；与所在父类目语义完全无关的条目。\n"
    "判 keep 的情况：名词或名词短语、能指代一类对象（哪怕冷门、抽象，"
    "如『低气压』『周转资产』『冰架』都是合法分类）。\n"
    "拿不准时倾向 keep（宁可漏删，不可误删）。\n"
    "只输出一个 JSON 对象，不要任何额外文字，格式：\n"
    '{"results": [{"name": "标签名", "verdict": "keep 或 delete", '
    '"reason": "简短理由(15字内)"}]}，results 与输入顺序一一对应。'
)


def is_dead_leaf(n):
    return (not n.get("children") and not n.get("instances")
            and not any(n.get(k) for k in KB_FIELDS)
            and n.get("depth", 0) >= 2)


def collect_candidates(branch):
    """死叶子按父路径分组，返回 {parent_path: [(node_path, name), ...]}。"""
    doc = json.load(open(TAXONOMY_PATH, encoding="utf-8"))
    groups = defaultdict(list)

    def walk(n):
        for c in n.get("children", []) or []:
            if is_dead_leaf(c) and (not branch or branch in c.get("path", "")):
                groups[n.get("path", "(根)")].append((c["path"], c["name"]))
            walk(c)
    walk(doc["tree"])
    return groups


def make_batches(groups):
    """候选多的父组单独切块成批；小父组合并装箱进同一批（批内按父路径分节）。

    返回批列表，每批 = [(parent, [(path,name)...]), ...]。
    """
    big = [(p, it) for p, it in groups.items() if len(it) > BATCH // 2]
    small = [(p, it) for p, it in groups.items() if len(it) <= BATCH // 2]
    batches = []
    for p, items in big:
        for i in range(0, len(items), BATCH):
            batches.append([(p, items[i:i + BATCH])])
    cur, size = [], 0
    for p, items in small:
        if size + len(items) > BATCH and cur:
            batches.append(cur)
            cur, size = [], 0
        cur.append((p, items))
        size += len(items)
    if cur:
        batches.append(cur)
    return batches


def build_user_prompt(batch):
    lines = []
    idx = 1
    for parent, items in batch:
        lines.append(f"父路径：{parent}（{len(items)} 个）")
        for _p, name in items:
            lines.append(f"{idx}. {name}")
            idx += 1
        lines.append("")
    return "\n".join(lines)


def scan(args):
    groups = collect_candidates(args.branch)
    total = sum(len(v) for v in groups.values())
    print(f"候选死叶子：{total} 个，分布于 {len(groups)} 个父节点下"
          + (f"（branch={args.branch!r}）" if args.branch else ""))
    return groups, total


def do_review(args):
    groups, total = scan(args)
    cache = llm.JsonlCache(str(CACHE_PATH))
    done = set() if args.overwrite else cache.done_keys()
    raw = make_batches(groups)
    batches = [[(p, [(pp, nm) for pp, nm in items if pp not in done])
                for p, items in batch] for batch in raw]
    batches = [[(p, items) for p, items in batch if items] for batch in batches]
    batches = [b for b in batches if b]
    todo = sum(len(items) for batch in batches for _p, items in batch)
    print(f"待审查：{todo} 个节点 / {len(batches)} 批（已完成 {len(done)}）", flush=True)
    if not todo:
        return
    if args.limit:
        kept, n = [], 0
        for batch in batches:
            cur = []
            for p, items in batch:
                take = items[: max(0, args.limit - n)]
                if take:
                    cur.append((p, take))
                    n += len(take)
            if cur:
                kept.append(cur)
        batches = kept
        print(f"（--limit 截断为 {sum(len(i) for b in batches for _p, i in b)} 个）")

    if args.dry_run:
        for batch in batches[:2]:
            print("=" * 60)
            preview = [(p, items[:30]) for p, items in batch[:3]]
            print(build_user_prompt(preview))
        print("=" * 60)
        print("[dry-run] 未调用 API，结束。")
        return

    llm.require_api_key()
    client = llm.make_client()

    def work(batch):
        rec = llm.generate(client, SYSTEM_PROMPT, build_user_prompt(batch))
        results = (rec or {}).get("results") or []
        by_name = {}
        for r in results:
            if isinstance(r, dict) and r.get("name"):
                by_name.setdefault(r["name"], r)
        n_ok = n = 0
        for parent, items in batch:
            for path, name in items:
                n += 1
                r = by_name.get(name)
                verdict = (r or {}).get("verdict", "").strip().lower()
                ok = verdict in ("keep", "delete")
                cache.append(path, {
                    "name": name, "parent": parent, "verdict": verdict,
                    "reason": str((r or {}).get("reason", ""))[:60],
                }, ok)
                n_ok += ok
        return n, n_ok

    tot_ok = tot_fail = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(work, batch): i for i, batch in enumerate(batches, 1)}
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                n, n_ok = fut.result()
                tot_ok += n_ok
                tot_fail += n - n_ok
                print(f"[批 {i}/{len(batches)}] {n_ok}/{n} 判定有效", flush=True)
            except Exception as e:
                print(f"[批 {i}/{len(batches)}] ERROR {e}", flush=True)
    print(f"审查完成：有效判定 {tot_ok} / 失败 {tot_fail}；缓存 {CACHE_PATH}")
    print("下一步：--report 生成审查报告人工抽查；确认后 --apply --write 执行删除。")


def load_verdicts():
    cache = llm.JsonlCache(str(CACHE_PATH))
    recs = cache.records()
    out = {path: r for path, r in recs.items()
           if r.get("verdict") in ("keep", "delete")}
    return out


def do_report():
    verdicts = load_verdicts()
    dels = {p: r for p, r in verdicts.items() if r["verdict"] == "delete"}
    keeps = len(verdicts) - len(dels)
    by_parent = defaultdict(list)
    for p, r in dels.items():
        by_parent[r.get("parent", "?")].append(r)
    lines = [
        f"# 标签树脏节点审查报告",
        "",
        f"已判定 {len(verdicts)} 个死叶子：keep {keeps} / delete {len(dels)}。",
        f"人工抽查下方 delete 名单后运行：`python3 taxonomy/audit_nodes.py --apply --write`",
        "",
    ]
    for parent in sorted(by_parent):
        items = by_parent[parent]
        lines.append(f"## {parent}（{len(items)} 删）")
        for r in sorted(items, key=lambda x: x["name"]):
            lines.append(f"- ~~{r['name']}~~ —— {r.get('reason', '')}")
        lines.append("")
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"报告已写出：{REPORT_PATH}（delete {len(dels)} / keep {keeps}）")


def do_apply(write):
    verdicts = load_verdicts()
    del_paths = {p for p, r in verdicts.items() if r["verdict"] == "delete"}
    if not del_paths:
        print("无 delete 判定，先跑审查（默认模式）或检查缓存。")
        return
    doc = json.load(open(TAXONOMY_PATH, encoding="utf-8"))
    removed, skipped = [], []

    def prune(n):
        kept = []
        for c in n.get("children", []) or []:
            if c.get("path") in del_paths:
                if is_dead_leaf(c):        # 二次确认：仍是死叶子才删
                    removed.append(c["path"])
                    continue
                skipped.append(c["path"])
            prune(c)
            kept.append(c)
        if kept:
            n["children"] = kept
        else:
            n.pop("children", None)
    prune(doc["tree"])

    print(f"将删除 {len(removed)} 个节点"
          + (f"，跳过 {len(skipped)} 个（已非死叶子）" if skipped else ""))
    if not write:
        print("（未加 --write，未落盘；抽查无误后重跑加 --write）")
        return
    json.dump(doc, open(TAXONOMY_PATH, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(f"已写出 {TAXONOMY_PATH}")
    print("后续同步：python3 viewer/build_viewer.py")


def main():
    ap = argparse.ArgumentParser(description="LLM 批量审查标签树死叶子节点")
    ap.add_argument("--report", action="store_true", help="从缓存生成审查报告")
    ap.add_argument("--apply", action="store_true",
                    help="按 delete 判定从 taxonomy.json 删节点")
    llm.add_common_args(ap)
    args = ap.parse_args()
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if args.report:
        do_report()
    elif args.apply:
        do_apply(args.write)
    else:
        do_review(args)


if __name__ == "__main__":
    main()
