#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gen_taxonomy_kb.py — LLM 一次调用为一个 taxonomy 节点生成 KB 字段。

对 datasets/demiwtg/meta/taxonomy.json 中 IP 分支下缺 KB 的节点，一次 LLM 调用产出：
    knowledge_intro / aliases / representative_cases / related_tags

上下文：节点 path、父节点、子节点名、挂载实例名、兄弟节点名。
约束：representative_cases 只允许从该节点的实例名中选；related_tags 只允许从
兄弟节点名中选（保证指向真实节点，不编造）。

机制（共用 llm_common.py）：
  - OpenAI 兼容端点（LLM_BASE_URL），LLM_WEB_SEARCH=1 可联网核实（仅官方端点）
  - 断点续跑：缓存 state/taxonomy/.llm_taxonomy_kb_cache.jsonl，--overwrite 重生成

用法：
  python3 taxonomy/gen_taxonomy_kb.py --dry-run --limit 3 --branch "内容作品 IP"
  export LLM_API_KEY=sk-... LLM_BASE_URL=https://api.openai.com/v1 LLM_MODEL=gpt-4o-mini
  python3 taxonomy/gen_taxonomy_kb.py --branch "内容作品 IP" --limit 50 --write
  python3 taxonomy/gen_taxonomy_kb.py --only-empty --write      # 只补缺口
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # data/（裸包名 import 约定）

from taxonomy import llm_common as llm

ROOT = Path(__file__).resolve().parent.parent.parent      # 仓库根
TAXONOMY_PATH = ROOT / "datasets" / "demiwtg" / "meta" / "taxonomy.json"
CACHE_PATH = ROOT / "state" / "taxonomy" / ".llm_taxonomy_kb_cache.jsonl"

KB_FIELDS = ["knowledge_intro", "aliases",
             "representative_cases", "related_tags"]

SYSTEM_PROMPT = (
    "你是中文 IP 标签体系的知识库撰写助手。对每个给定的标签节点（分类），产出客观、"
    "准确、不编造的内容，风格接近维基百科词条：必须有具体知识点（历史沿革、代表作品、"
    "产业数据、文化特征），拒绝空话套话。\n"
    "若你对该分类不了解，应使用可用的联网检索工具核实后再作答。\n"
    "只输出一个 JSON 对象，不要任何额外文字，格式：\n"
    '{"knowledge_intro": "知识介绍(150-350字，涵盖什么、典型特征、产业/文化背景、'
    '与相邻类别的边界；禁止写"是XX体系中的细分标签""用于标识XX相关的IP内容"一类空话)", '
    '"aliases": ["别名/别称", ...]（最多5个）,\n'
    ' "representative_cases": ["代表案例(必须是给出的实例名)", ...]（最多5个）, '
    '"related_tags": ["关联标签(必须从给出的兄弟节点名中选)", ...]（最多6个）}'
)


def find_ip_root(tree):
    for ch in tree.get("children", []) or []:
        if "IP" in ch.get("name", ""):
            return ch
    return tree


def walk_nodes(node, out, parent=None):
    out.append((node, parent))
    for ch in node.get("children", []) or []:
        walk_nodes(ch, out, node)


def load_targets(args):
    doc = json.load(open(TAXONOMY_PATH, encoding="utf-8"))
    ip_root = find_ip_root(doc["tree"])
    nodes = []
    walk_nodes(ip_root, nodes)
    out = []
    for n, _parent in nodes:
        path = n.get("path", "")
        if not path:
            continue
        if args.branch and args.branch not in path:
            continue
        if args.only_empty and all(n.get(k) for k in KB_FIELDS):
            continue
        if (not args.refresh) and n.get("knowledge_intro"):
            continue
        out.append(path)
    return out


def collect_children_names(n, limit=12):
    return [c.get("name", "") for c in (n.get("children") or [])[:limit]]


def collect_instance_names(n, limit=14):
    out = []
    def rec(m):
        for inst in (m.get("instances") or []):
            name = inst.get("name") if isinstance(inst, dict) else inst
            if name and name not in out:
                out.append(name)
                if len(out) >= limit:
                    return
        for ch in (m.get("children") or []):
            rec(ch)
            if len(out) >= limit:
                return
    rec(n)
    return out[:limit]


def build_user_prompt(node, parent, siblings):
    ctx = []
    if node.get("knowledge_intro"):
        ctx.append("已有知识介绍：" + node["knowledge_intro"])
    if node.get("aliases"):
        ctx.append("已有别名：" + "、".join(node["aliases"]))
    ctx_block = ("\n".join(ctx) + "\n") if ctx else ""
    children = collect_children_names(node)
    insts = collect_instance_names(node)
    return (
        f"节点路径：{node.get('path', '')}\n"
        f"父节点：{parent.get('path', '(根)') if parent else '(根)'}\n"
        + (f"子节点：{'、'.join(children)}\n" if children else "")
        + (f"挂载实例：{'、'.join(insts)}\n" if insts else "")
        + (f"兄弟节点（供 related_tags 选择）：{'、'.join(siblings)}\n" if siblings else "")
        + ctx_block
        + "请生成该节点的 knowledge_intro / aliases / "
          "representative_cases / related_tags。"
    )


def _clean_list(v, allowed, cap):
    if isinstance(v, str):
        v = [x.strip() for x in v.replace("、", ",").split(",") if x.strip()]
    if not isinstance(v, list):
        return []
    out = []
    for x in v:
        x = str(x).strip()
        if x in allowed and x not in out:
            out.append(x)
            if len(out) >= cap:
                break
    return out


def merge_node(node, rec, inst_names, sibling_names):
    changed = False
    v = (rec.get("knowledge_intro") or "").strip()
    if v and v != node.get("knowledge_intro"):
        node["knowledge_intro"] = v
        changed = True
    node.pop("definition", None)
    al = rec.get("aliases") or []
    if isinstance(al, str):
        al = [al]
    al = [str(x).strip() for x in al if str(x).strip()][:5]
    if al:
        node["aliases"] = al
        changed = True
    cases = _clean_list(rec.get("representative_cases"), set(inst_names), 5)
    if cases:
        node["representative_cases"] = cases
        changed = True
    rel = _clean_list(rec.get("related_tags"), set(sibling_names), 6)
    if rel:
        node["related_tags"] = rel
        changed = True
    return changed


def main():
    ap = argparse.ArgumentParser(description="LLM 一次调用为一个 taxonomy 节点生成 KB")
    ap.add_argument("--only-empty", action="store_true",
                    help="只处理四个 KB 字段全空的节点")
    ap.add_argument("--refresh", action="store_true",
                    help="连已有 knowledge_intro 的节点也重生成")
    llm.add_common_args(ap)
    args = ap.parse_args()

    targets = load_targets(args)
    cache = llm.JsonlCache(CACHE_PATH)
    done = set() if args.overwrite else cache.done_keys()
    targets = [p for p in targets if p not in done]
    if args.limit:
        targets = targets[:args.limit]

    print(f"目标 taxonomy 节点：{len(targets)} 个"
          + (f"（branch={args.branch!r}" if args.branch else "")
          + (", only-empty" if args.only_empty else "")
          + (", refresh" if args.refresh else ", 跳过已有KB") + "）", flush=True)

    # 节点查找表（dry-run 与生成共用）
    doc = json.load(open(TAXONOMY_PATH, encoding="utf-8"))
    ip_root = find_ip_root(doc["tree"])
    by_path = {}
    siblings_of = {}
    nodes = []
    walk_nodes(ip_root, nodes)
    for n, p in nodes:
        by_path[n.get("path", "")] = n
    for n, p in nodes:
        siblings_of[n.get("path", "")] = [
            s.get("name", "") for s, _ in nodes if s is not n and _ is p
        ]

    if args.dry_run:
        for path in targets[: max(args.limit, 3)]:
            n = by_path[path]
            print("=" * 60)
            print(build_user_prompt(n, None, siblings_of.get(path, [])))
        print("=" * 60)
        print("[dry-run] 未调用 API，结束。")
        return

    llm.require_api_key()
    use_responses = llm.want_responses()
    client = llm.make_client()

    def work(path):
        n = by_path[path]
        user = build_user_prompt(n, None, siblings_of.get(path, []))
        rec = llm.generate(client, SYSTEM_PROMPT, user, use_responses)
        ok = bool(rec and rec.get("knowledge_intro"))
        cache.append(path, rec or {}, ok)
        return path, ok, (rec or {}).get("knowledge_intro", "")[:40] if rec else ""

    ok_n = fail_n = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = [ex.submit(work, p) for p in targets]
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                path, ok, prev = fut.result()
                ok_n += ok
                fail_n += (not ok)
                print(f"[{i}/{len(targets)}] {'OK ' if ok else 'FAIL'} {path} | {prev}",
                      flush=True)
            except Exception as e:
                fail_n += 1
                print(f"[{i}/{len(targets)}] ERROR {e}", flush=True)
            if args.delay:
                time.sleep(args.delay)

    print(f"生成完成：成功 {ok_n} / 失败 {fail_n}；缓存于 {CACHE_PATH}")

    if args.write:
        apply_cache(cache, by_path, siblings_of)
    else:
        print("（未加 --write，缓存未合并。需要时再运行 --write）")


def apply_cache(cache, by_path, siblings_of):
    recs = cache.records()
    if not recs:
        print("无有效缓存，跳过合并。")
        return
    n = 0
    for path, rec in recs.items():
        node = by_path.get(path)
        if not node:
            continue
        inst_names = collect_instance_names(node)
        sibling_names = siblings_of.get(path, [])
        if merge_node(node, rec, inst_names, sibling_names):
            n += 1
    doc = json.load(open(TAXONOMY_PATH, encoding="utf-8"))
    doc["meta"] = dict(doc.get("meta", {}))
    doc["meta"]["source"] = (
        doc["meta"].get("source", "") + " + gen_taxonomy_kb.py(LLM taxonomy KB)")
    json.dump(doc, open(TAXONOMY_PATH, "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(f"已合并 {n} 个节点到 {TAXONOMY_PATH}")


if __name__ == "__main__":
    main()
