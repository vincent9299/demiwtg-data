#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""upgrade_v31.py — 标签体系 v3.1 一次性升级器（2026-08-24 架构决策）。

把外部交付的「融合世界标签体系 v3.1」终版底稿（CSV：路径+实例清单）
升级进本仓数据契约（AGENTS.md 1.5 两概念模型）：

  1. taxonomy.json 重建：交付包 21,406 路径建树；节点 instances 名单取
     该名单 ∩ instances.json 现存名（树引用保持合法）；幸存节点按 path
     精确匹配保留旧树 4 个 KB 字段，新节点 KB 留空（后续可选
     gen_taxonomy_kb --only-empty 补）。
  2. 死名回挂：旧树挂载、但名字不在 v3.1 名单中的实例（粗伞实例，如
     主战坦克/黄道十二宫）不剥离——按原挂载路径回挂到 v3.1 树；旧路径
     不存在时回退最近存活祖先；裁定表干跑打印，落盘前人工过目。
  3. 分批入库：v3.1 新增实例本次不写 instances.json，按域分组清单落
     state/taxonomy/v31_pending_instances.json 供后续批次生产入库；
     每批入库后重跑本脚本刷新树的 instances 名单（幂等）。
  4. alias_western.json 仅扩名单：在册实例缺失者登记 null 占位。

用法（默认干跑，只打印变更统计与裁定表；确认后 --apply 落盘）：
  python3 taxonomy/upgrade_v31.py
  python3 taxonomy/upgrade_v31.py --apply
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, OrderedDict
from datetime import datetime
from pathlib import Path

sys.setrecursionlimit(20000)
ROOT = Path(__file__).resolve().parent.parent.parent    # 仓库根
META = ROOT / "datasets" / "demiwtg" / "meta"
TAXONOMY_PATH = META / "taxonomy.json"
INSTANCES_PATH = META / "instances.json"
ALIAS_PATH = META / "alias_western.json"
PACKAGE_DIR = ROOT / "state" / "taxonomy" / "v31_交付包" / "taxonomy_v3.1_交付包"
PENDING_PATH = ROOT / "state" / "taxonomy" / "v31_pending_instances.json"
KB_REPORT_PATH = ROOT / "state" / "taxonomy" / "v31_kb_recovery_report.json"
SEP = " / "
KB_FIELDS = ["knowledge_intro", "aliases",
             "representative_cases", "related_tags"]

# 路径前缀精简（2026-08-24 架构决策）：底稿与旧树的根名/中间层（根『融合世界标签体系』
# 与一级分支『通用分类标签』，及历史双树期的『IP 分类标签』）统一剥掉，29 域直挂新根。
NEW_ROOT = "demiwtg"
OLD_L1 = {"通用分类标签", "IP 分类标签"}


def norm_path(p: str) -> str:
    """旧前缀路径 → 新前缀路径（根=demiwtg，域直挂；双树期 IP 路径同样归一）。"""
    segs = p.split(SEP)
    if len(segs) > 1 and segs[1] in OLD_L1:
        segs = segs[2:]
    else:
        segs = segs[1:]
    return SEP.join([NEW_ROOT] + [s for s in segs if s])


def recover_kb_by_fingerprint(rows, root, old_path2node, v31_paths):
    """名单指纹法抢救失配节点的 KB。

    IP 吸收等迁移重写了路径，旧树 KB 按 path 失配；但迁移是前缀替换，
    节点名单原样随行，故用名单指纹反查新路径：
      ① 直接名单精确相等 → ② 子树名单精确相等 → ③ 直接名单 Jaccard
    最佳且唯一（阈值 0.85）。命中后把旧节点 4 个 KB 字段写到新节点；
    失配清单返回给调用方上报（后续可用 gen_taxonomy_kb 重生成）。
    """
    direct, sub = {}, {}

    def compute(n):
        d = frozenset(rows_by_path.get(n["path"], ()))
        s = set(d)
        for c in n.get("children", []):
            s |= compute(c)
        direct[n["path"]] = d
        sub[n["path"]] = frozenset(s)
        return s

    rows_by_path = {p: tuple(dict.fromkeys(x for x in lst)) for p, lst in rows}
    compute(root)
    by_direct, by_sub = {}, {}
    for p in v31_paths:
        if direct[p]:
            by_direct.setdefault(direct[p], []).append(p)
        if sub[p]:
            by_sub.setdefault(sub[p], []).append(p)

    old_kb = [(p, n) for p, n in old_path2node.items()
              if any(n.get(k) for k in KB_FIELDS) and p not in set(v31_paths)]
    matched, unmatched = {}, []
    for p, n in old_kb:
        d_old = frozenset(n.get("instances") or [])
        cands = by_direct.get(d_old) if d_old else None
        method = "direct"
        if not cands:
            # 子树名单指纹：旧树子树 ∩ 在册名（名单裁剪口径与新树一致）
            s_old = set()

            def gsub(m):
                s_old.update(m.get("instances") or [])
                for c in m.get("children", []):
                    gsub(c)
            gsub(n)
            cands = by_sub.get(frozenset(s_old))
            method = "subtree"
        if cands and len(cands) == 1:
            matched[p] = (cands[0], method)
            continue
        # Jaccard 最佳且唯一（限同深度候选减低成本：全表扫描也可，21k 可接受）
        if d_old:
            best, best_j, runner = None, 0.0, 0.0
            for q in v31_paths:
                d_new = direct[q]
                if not d_new:
                    continue
                inter = len(d_old & d_new)
                if inter == 0:
                    continue
                j = inter / len(d_old | d_new)
                if j > best_j:
                    runner, best_j, best = best_j, j, q
                elif j > runner:
                    runner = j
            if best and best_j >= 0.85 and best_j - runner > 0.05:
                matched[p] = (best, f"jaccard={best_j:.2f}")
                continue
        unmatched.append(p)

    applied = 0
    for old_p, (new_p, _m) in matched.items():
        n_old, n_new = old_path2node[old_p], None
        stack = [root]
        while stack:
            cur = stack.pop()
            if cur["path"] == new_p:
                n_new = cur
                break
            stack.extend(cur.get("children", []))
        if n_new is None:
            continue
        for k in KB_FIELDS:
            if n_old.get(k) and not n_new.get(k):
                n_new[k] = n_old[k]
        if any(n_new.get(k) for k in KB_FIELDS):
            applied += 1
    return matched, unmatched, applied


def load_package_csv(path: Path):
    """读交付包底稿：[(路径, [实例名, ...]), ...]（保留原行序与名单序）。"""
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader)    # 表头
        for row in reader:
            p = row[0].strip()
            insts = [x.strip() for x in row[1].split("|")] if len(row) > 1 else []
            rows.append((p, [x for x in insts if x]))
    return rows


def load_old_tree(path: Path):
    """旧树 → (节点序列表 [node, ...], path→node, 实例名→[挂载路径, ...])。

    path 一律经 norm_path 归一到新前缀，使 KB 精确匹配与死名回挂目标路径
    与 build_new_tree 产出的新树键空间一致。
    """
    doc = json.loads(path.read_text(encoding="utf-8"))
    nodes, path2node = [], {}
    mounts: dict[str, list[str]] = {}

    def walk(n):
        nodes.append(n)
        p = norm_path(n.get("path", "") or NEW_ROOT)
        path2node[p] = n
        for nm in n.get("instances") or []:
            mounts.setdefault(str(nm), []).append(p)
        for ch in n.get("children") or []:
            walk(ch)

    walk(doc["tree"])
    return doc, nodes, path2node, mounts


def build_new_tree(rows, alive_names, dead_mounts, old_path2node):
    """v3.1 路径列表建树。

    alive_names: instances.json 现存名（∩ 后才进名单）；
    dead_mounts: {死名: 回挂路径}（追加到目标节点名单尾部）；
    返回 (tree, 统计)。
    """
    nodes: "OrderedDict[str, dict]" = OrderedDict()
    stats = Counter()
    for raw_p, inst_list in rows:
        p = norm_path(raw_p)    # 底稿路径同样归一：根=demiwtg，域直挂
        segs = p.split(SEP)
        node = {"name": segs[-1], "path": p, "depth": len(segs) - 1}
        old = old_path2node.get(p)
        if old is not None:
            kept = {k: old[k] for k in KB_FIELDS if old.get(k)}
            node.update(kept)
            if kept:
                stats["kb_kept_nodes"] += 1
        else:
            stats["new_nodes"] += 1
        kept_names = [x for x in inst_list if x in alive_names]
        stats["kept_inst_refs"] += len(kept_names)
        stats["dropped_inst_refs"] += len(inst_list) - len(kept_names)
        seen = set()
        deduped = []
        for x in kept_names:    # 名单内去重（同名挂一处，唯一主键约束）
            if x not in seen:
                seen.add(x)
                deduped.append(x)
        if deduped:
            node["instances"] = deduped
        nodes[p] = node

    # 死名回挂：目标路径不存在则回退最近存活祖先
    remount_resolved = {}
    for name, target in dead_mounts.items():
        t = target
        while t and t not in nodes:
            t = t.rsplit(SEP, 1)[0] if SEP in t else ""
        if not t:
            raise RuntimeError(f"死名 {name!r} 无存活祖先可回挂")
        nodes[t].setdefault("instances", []).append(name)
        remount_resolved[name] = t
        if t != target:
            stats["remount_fallback"] += 1

    # 底稿骨架缺口：部分中间节点（如实测 4 个空域域节点）在底稿中缺失，
    # 其子树成孤儿；隐式补齐全部缺失祖先（纯骨架，无实例名单），逐个上报。
    implicit_paths = set()
    for p in list(nodes):
        cur = p
        while SEP in cur:
            cur = cur.rsplit(SEP, 1)[0]
            if cur in nodes or cur in implicit_paths:
                break
            segs = cur.split(SEP)
            nodes[cur] = {"name": segs[-1], "path": cur,
                          "depth": len(segs) - 1}
            implicit_paths.add(cur)
            stats["implicit_nodes"] += 1

    # 组装树：先按路径深度升序保证父在子前，再按底稿行序挂接（底稿序保留）
    row_order = {p: i for i, (p, _) in enumerate(rows)}
    for p in sorted(nodes, key=lambda q: (q.count(SEP), row_order.get(q, -1))):
        node = nodes[p]
        if SEP not in p:
            root = node
            continue
        parent_path = p.rsplit(SEP, 1)[0]
        nodes[parent_path].setdefault("children", []).append(node)

    kids = Counter(1 for n in nodes.values() if "children" in n)
    stats["node_count"] = len(nodes)
    stats["leaf_count"] = len(nodes) - sum(kids.values())
    # 实挂引用总数 = 底稿名单保留数 + 死名回挂数（口径含回挂，与树遍历一致）
    stats["total_inst_refs"] = stats["kept_inst_refs"] + len(remount_resolved)
    return root, remount_resolved, stats, sorted(implicit_paths)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true",
                    help="落盘（默认干跑只打印）")
    ap.add_argument("--package-dir", default=str(PACKAGE_DIR),
                    help="交付包解压目录（含 data/taxonomy_merged_progress_instances.csv）")
    args = ap.parse_args()

    # ---- 读现状 ----
    old_doc, _old_nodes, old_path2node, old_mounts = load_old_tree(TAXONOMY_PATH)
    inst_doc = json.loads(INSTANCES_PATH.read_text(encoding="utf-8"))
    alive_names = {i["name"] for i in inst_doc["instances"]}
    alias_doc = json.loads(ALIAS_PATH.read_text(encoding="utf-8"))

    # ---- 读 v3.1 底稿（路径统一归一：根=demiwtg，域直挂）----
    raw_rows = load_package_csv(Path(args.package_dir) / "data" /
                                "taxonomy_merged_progress_instances.csv")
    # 归一后可能撞路径（实测仅根行与『通用分类标签』中间层行同归 demiwtg，
    # 均空名单）：同路径名单按序合并去重，上报合并数。
    merged: "OrderedDict[str, list]" = OrderedDict()
    for p, lst in ((norm_path(p), l) for p, l in raw_rows):
        bucket = merged.setdefault(p, [])
        seen = set(bucket)
        for x in lst:
            if x not in seen:
                seen.add(x)
                bucket.append(x)
    rows = [(p, lst) for p, lst in merged.items()]
    n_merged = len(raw_rows) - len(rows)
    v31_paths = [p for p, _ in rows]
    assert len(v31_paths) == len(set(v31_paths)), "交付包路径归一后仍重复"
    v31_names = set()
    for _, inst_list in rows:
        v31_names.update(inst_list)

    survive = alive_names & v31_names
    dead = sorted(alive_names - v31_names)
    new_insts = v31_names - alive_names

    print(f"== 集合对比 ==")
    print(f"  在册实例 {len(alive_names)}：幸存 {len(survive)} / "
          f"死名 {len(dead)} / v3.1 新增 {len(new_insts)}")
    print(f"  v3.1 路径 {len(raw_rows)} 行（归一后合并 {n_merged} 行，"
          f"存 {len(v31_paths)} 路径），实例名去重 {len(v31_names)}")

    # ---- 死名回挂裁定（原挂载路径；多挂载取首个并标注）----
    dead_mounts, multi = {}, []
    for name in dead:
        paths = old_mounts.get(name, [])
        if not paths:
            raise RuntimeError(f"死名 {name!r} 在旧树无挂载记录")
        dead_mounts[name] = paths[0]
        if len(paths) > 1:
            multi.append((name, paths))
    print(f"\n== 死名回挂裁定（{len(dead_mounts)} 条）==")
    for name in dead:
        print(f"  {name}  ->  {dead_mounts[name]}")
    if multi:
        print("  [多挂载，取首个]")
        for name, paths in multi:
            print(f"    {name}: {paths}")

    # ---- 建新树 ----
    root, remount_resolved, stats, implicit_paths = build_new_tree(
        rows, alive_names, dead_mounts, old_path2node)

    print(f"\n== 新树 ==")
    print(f"  节点 {stats['node_count']}（底稿 {len(v31_paths)} + 隐式补骨架 "
          f"{stats['implicit_nodes']}，叶 {stats['leaf_count']}）/ "
          f"幸存保留 KB 节点 {stats['kb_kept_nodes']} / 新节点 {stats['new_nodes']}")
    if implicit_paths:
        print("  [底稿缺失、隐式补齐的骨架节点]")
        for p in implicit_paths:
            print(f"    + {p}")
    print(f"  实例引用保留 {stats['kept_inst_refs']}（+回挂 "
          f"{len(remount_resolved)} = 实挂 {stats['total_inst_refs']}）/ "
          f"丢弃（未入库新实例）{stats['dropped_inst_refs']}")
    print(f"  回挂 {len(remount_resolved)}（退化到祖先 {stats['remount_fallback']}）")

    # ---- 失配节点 KB 指纹抢救 ----
    matched, unmatched, applied = recover_kb_by_fingerprint(
        rows, root, old_path2node, v31_paths)
    print(f"\n== KB 指纹抢救 ==  失配 KB 节点 {len(matched) + len(unmatched)}："
          f"命中 {len(matched)}（已写入 {applied}）/ 未命中 {len(unmatched)}")
    by_method = Counter(m for _p, (_q, m) in matched.items())
    for m, c in by_method.most_common():
        print(f"  命中方式[{m}]: {c}")
    if unmatched:
        print("  [未命中清单（后续可用 gen_taxonomy_kb 重生成）]")
        for p in unmatched[:30]:
            print(f"    x {p}")
        if len(unmatched) > 30:
            print(f"    ... 共 {len(unmatched)} 条")

    # ---- 断言 ----
    assert stats["node_count"] == len(v31_paths) + len(implicit_paths), \
        "节点数 = 底稿归一路径数 + 隐式补骨架数"

    def iter_names(n):
        for x in n.get("instances", []):
            yield x
        for c in n.get("children", []):
            yield from iter_names(c)

    def check_nodes(n):
        lst = n.get("instances", [])
        assert len(lst) == len(set(lst)), f"节点名单内重复: {n['path']}"
        for c in n.get("children", []):
            check_nodes(c)
    check_nodes(root)
    ref_names = list(iter_names(root))
    assert len(ref_names) == stats["total_inst_refs"], "实挂引用口径与树遍历不一致"
    assert set(ref_names) <= alive_names, "树引用了 instances.json 之外的名字"
    # 跨节点多挂是契约允许的（AGENTS.md 1.5：多处挂载表现为多个节点名单同名）
    ref_counter = Counter(ref_names)
    print(f"  树实例引用 {len(ref_names)} 次 / 唯一名 {len(ref_counter)} / "
          f"跨节点多挂名 {sum(1 for c in ref_counter.values() if c > 1)}")
    assert set(remount_resolved) == set(dead), "死名未全部回挂"
    new_path2node = {}    # 幸存节点 KB 不降

    def index(n):
        new_path2node[n["path"]] = n
        for c in n.get("children", []):
            index(c)
    index(root)
    for n_old in old_path2node.values():
        n_new = new_path2node.get(n_old.get("path"))
        if n_new is None:
            continue
        for k in KB_FIELDS:
            assert not (n_old.get(k) and not n_new.get(k)), \
                f"KB 字段丢失: {n_old.get('path')} / {k}"

    # ---- 分批入库候选清单（按域分组，域 = 归一后路径第二段）----
    dom_of = {}
    for p, inst_list in rows:
        segs = norm_path(p).split(SEP)
        dom = segs[1] if len(segs) >= 2 else "(根级)"
        for x in inst_list:
            if x in new_insts:
                dom_of.setdefault(x, dom)
    pending: dict[str, list[str]] = {}
    for x, dom in dom_of.items():
        pending.setdefault(dom, []).append(x)
    for dom in pending:
        pending[dom].sort()
    print(f"\n== 分批入库候选 ==  共 {len(dom_of)} 个新实例，{len(pending)} 个域")
    for dom in sorted(pending, key=lambda d: -len(pending[d])):
        print(f"  {dom}: {len(pending[dom])}")

    # ---- alias_western 扩名单 ----
    alias_add = sorted(alive_names - set(alias_doc))
    print(f"\n== alias_western ==  在册 {len(alive_names)}，词表现存 "
          f"{len(alias_doc)}，新增占位 {len(alias_add)}")

    # ---- 落盘 ----
    if not args.apply:
        print("\n[干跑] 未落盘。确认裁定表后加 --apply。")
        return

    desc = old_doc.get("meta", {}).get("description", "")
    desc = desc.replace("覆盖整棵树（通用分类标签 + IP 分类标签）",
                        "根 demiwtg 直挂 29 域（前缀精简，无中间层）")
    new_doc = {
        "schema_version": "1.1.0",
        "meta": {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source": "融合世界标签体系 v3.1 交付包（2026-08-24，"
                      "state/taxonomy/v31_交付包/；三批迁移终版：域级前缀替换+"
                      "IP 域吸收 29 域+知识与学科清理；路径前缀精简：根 demiwtg "
                      "直挂 29 域）",
            "description": desc,
        },
        "tree": root,
    }
    TAXONOMY_PATH.write_text(
        json.dumps(new_doc, ensure_ascii=False, separators=(", ", ": ")) + "\n",
        encoding="utf-8")
    for name in alias_add:
        alias_doc[name] = None
    ALIAS_PATH.write_text(
        json.dumps(alias_doc, ensure_ascii=False, separators=(", ", ": ")) + "\n",
        encoding="utf-8")
    PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    KB_REPORT_PATH.write_text(
        json.dumps({"meta": {"generated_at": datetime.now().isoformat(timespec="seconds"),
                             "note": "失配 KB 节点指纹抢救报告；unmatched 可用 "
                                     "gen_taxonomy_kb 重生成"},
                    "matched": {p: {"to": q, "method": m}
                                for p, (q, m) in matched.items()},
                    "unmatched": unmatched},
                   ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8")
    PENDING_PATH.write_text(
        json.dumps({"meta": {"generated_at": datetime.now().isoformat(timespec="seconds"),
                             "source": "taxonomy/upgrade_v31.py",
                             "note": "v3.1 新增实例分批入库候选；每批入库后重跑"
                                     "本脚本刷新树 instances 名单"},
                    "by_domain": pending},
                   ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8")
    print(f"\n[落盘] {TAXONOMY_PATH.name}（{stats['node_count']} 节点）/ "
          f"{ALIAS_PATH.name}（+{len(alias_add)}）/ "
          f"{PENDING_PATH.relative_to(ROOT)}（{len(dom_of)} 实例）")


if __name__ == "__main__":
    main()
