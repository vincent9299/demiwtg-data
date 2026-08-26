#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""upgrade_v31_en.py — 英文版标签体系一次性入库器（2026-08-24 架构决策）。

把「融合世界标签体系 v3.1」交付包的英文底稿（CSV：中文路径 + 英文路径 +
英文实例清单三列）建成一套**完全独立**的英文平行数据（契约同构
AGENTS.md 1.5 两概念模型）：

  1. taxonomy_en.json：英文路径建树（归一后根=demiwtg，29 域直挂，与
     中文 taxonomy.json 节点级同构）；底稿缺失的 4 个骨架域与中文版
     同源，按子路径截断隐式补齐。节点 KB 字段全空（英文全新树，无中
     文富知识可继承，不做映射）。
  2. instances_en.json：英文实例名轻量清洗（按词大小写归一，全大写缩
     写与混排词保留；清洗后同名的大小写变体合并——name 是唯一主键）
     后全量以 source=derived 占位入库。
  3. 翻译撞车自然合并（用户拍板 2026-08-24）：底稿有 120 处不同中文
     节点译成同一英文路径（如 炊具/锅具 → Cookware），英文树按英
     文路径自然合并（名单取并集），节点数比中文树少，域级对齐。
     撞车清单落报告供后续精译修复。
  4. 结构对齐验证（中文路径列为桥）：归一中文路径集合 ⊆ 中文树节点
     （差集恰为 4 个骨架域）；中文 → 英文路径单值校验。

不动：taxonomy.json / instances.json / alias_western.json（中文实例专属
词表）/ images.jsonl（英文实例与图打标零交集，英文版无图是预期）。
纯新增、零覆写既有文件，故不做入库前备份。

用法（默认干跑，只打印变更统计；确认后 --apply 落盘）：
  python3 taxonomy/upgrade_v31_en.py
  python3 taxonomy/upgrade_v31_en.py --apply
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, OrderedDict
from datetime import datetime
from pathlib import Path

sys.setrecursionlimit(20000)
ROOT = Path(__file__).resolve().parent.parent.parent    # 仓库根
META = ROOT / "datasets" / "demiwtg" / "meta"
TAX_ZH_PATH = META / "taxonomy.json"
TAX_EN_PATH = META / "taxonomy_en.json"
INST_EN_PATH = META / "instances_en.json"
PACKAGE_CSV = (ROOT / "state" / "taxonomy" / "v31_交付包" /
               "taxonomy_v3.1_交付包" / "data" / "taxonomy_tree_instances_en.csv")
CLEAN_REPORT = ROOT / "state" / "taxonomy" / "en_clean_report.json"
MERGE_REPORT = ROOT / "state" / "taxonomy" / "en_merge_report.json"
SEP = " / "
NEW_ROOT = "demiwtg"
# 前缀归一（中文版 norm_path 的英文同款）：剥根名与一级中间层，29 域直挂新根。
# IP 分支英文名为防御性收录（底稿实测只有 General Classification Tags）。
ZH_OLD_L1 = {"通用分类标签", "IP 分类标签"}
EN_OLD_L1 = {"General Classification Tags", "IP Classification Tags"}


def norm_path(p: str, old_l1: set) -> str:
    """旧前缀路径 → 新前缀路径（根=demiwtg，域直挂）。"""
    segs = p.split(SEP)
    if len(segs) > 1 and segs[1] in old_l1:
        segs = segs[2:]
    else:
        segs = segs[1:]
    return SEP.join([NEW_ROOT] + [s for s in segs if s])


# ---- 实例名轻量清洗（2026-08-24 拍板）-------------------------------------
# 按词归一：全小写/全大写词转首字母大写（light pen → Light Pen）；
# 全大写缩写（2-5 字母，NASA/USB）与大小写混排词原样保留。
# 清洗后同名的变体合并为一个实例（name 唯一主键，同一实体一条记录）。
_ACRONYM = re.compile(r"[A-Z]{2,5}")


def _clean_word(w: str) -> str:
    if _ACRONYM.fullmatch(w):
        return w
    if not w.islower() and not w.isupper():
        return w
    return w[:1].upper() + w[1:].lower()


def clean_name(n: str) -> str:
    return " ".join(_clean_word(w) for w in n.split(" "))


def load_csv(path: Path):
    """读英文底稿：[(中文路径, 英文路径, [实例名, ...]), ...]（保留原行序）。"""
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader)    # 表头
        for row in reader:
            zh, en = row[0].strip(), row[1].strip()
            insts = [x.strip() for x in row[2].split("|")] if len(row) > 2 else []
            rows.append((zh, en, [x for x in insts if x]))
    return rows


def load_zh_tree_paths(path: Path):
    """中文树全部节点路径（已是新前缀口径）。"""
    doc = json.loads(path.read_text(encoding="utf-8"))
    paths = set()

    def walk(n):
        paths.add(n["path"])
        for c in n.get("children") or []:
            walk(c)
    walk(doc["tree"])
    return doc, paths


def build_en_tree(rows_en, clean_stats):
    """英文路径列表建树（名单过清洗+去重，全保留；骨架缺口隐式补齐）。"""
    nodes: "OrderedDict[str, dict]" = OrderedDict()
    stats = Counter()
    for p, inst_list in rows_en:
        segs = p.split(SEP)
        node = {"name": segs[-1], "path": p, "depth": len(segs) - 1}
        seen, cleaned = set(), []
        for x in inst_list:
            c = clean_name(x)
            clean_stats["raw_refs"] += 1
            if c not in seen:
                seen.add(c)
                cleaned.append(c)
            else:
                clean_stats["deduped_refs"] += 1
        if cleaned:
            node["instances"] = cleaned
            stats["inst_refs"] += len(cleaned)
        nodes[p] = node

    # 底稿骨架缺口（与中文版同源：4 个域节点缺行），子路径截断隐式补齐
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

    # 组装树：深度升序保证父在子前，同深度按底稿行序
    row_order = {p: i for i, (p, _) in enumerate(rows_en)}
    root = None
    for p in sorted(nodes, key=lambda q: (q.count(SEP), row_order.get(q, -1))):
        node = nodes[p]
        if SEP not in p:
            root = node
            continue
        nodes[p.rsplit(SEP, 1)[0]].setdefault("children", []).append(node)

    stats["node_count"] = len(nodes)
    stats["leaf_count"] = sum(1 for n in nodes.values() if "children" not in n)
    return root, nodes, stats, sorted(implicit_paths)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true",
                    help="落盘（默认干跑只打印）")
    ap.add_argument("--csv", default=str(PACKAGE_CSV),
                    help="英文底稿 CSV 路径")
    args = ap.parse_args()

    # ---- 读底稿 + 归一 + 中文→英文单值校验 ----
    # 反向（英文→中文）允许撞车：底稿实测 120 处翻译撞车，自然合并（见下）。
    raw_rows = load_csv(Path(args.csv))
    zh2en = {}
    for zh, en, _lst in raw_rows:
        z, e = norm_path(zh, ZH_OLD_L1), norm_path(en, EN_OLD_L1)
        if zh2en.setdefault(z, e) != e:
            raise RuntimeError(f"中文路径 {z!r} 对应多个英文路径")
    n_merged_root = len(raw_rows) - len(zh2en)
    print(f"== 底稿与归一 ==")
    print(f"  底稿 {len(raw_rows)} 行，中文归一路径 {len(zh2en)}"
          f"（根行/中间层行同归 demiwtg，合并 {n_merged_root} 行）")

    # ---- 结构对齐（中文列为桥）----
    _zh_doc, zh_tree_paths = load_zh_tree_paths(TAX_ZH_PATH)
    csv_zh = set(zh2en)
    extra = csv_zh - zh_tree_paths
    assert not extra, f"底稿中文列有中文树之外的路径: {sorted(extra)[:5]}"
    missing = sorted(zh_tree_paths - csv_zh)
    assert all(p.count(SEP) == 1 for p in missing), "差集应为骨架域（深度 1）"
    print(f"\n== 结构对齐（中文列 ↔ 中文树 {len(zh_tree_paths)} 节点）==")
    print(f"  底稿中文路径 {len(csv_zh)} ⊆ 中文树；差集 {len(missing)} 个（预期 4 骨架域）")
    for p in missing:
        print(f"    - {p}")

    # ---- 归一英文行合并：根行/中间层行同归 demiwtg + 翻译撞车自然合并 ----
    # 撞车组名单取并集（用户拍板）；撞车清单落报告供后续精译修复。
    merged: "OrderedDict[str, list]" = OrderedDict()
    merge_of: dict = {}
    for zh, en, lst in raw_rows:
        e = norm_path(en, EN_OLD_L1)
        merge_of.setdefault(e, []).append(norm_path(zh, ZH_OLD_L1))
        bucket = merged.setdefault(e, [])
        seen = set(bucket)
        for x in lst:
            if x not in seen:
                seen.add(x)
                bucket.append(x)
    rows_en = [(p, lst) for p, lst in merged.items()]
    # 根路径撞车是前缀归一产物（根行+中间层行），不算翻译撞车，剔除后才是真撞车。
    collisions = {e: zs for e, zs in merge_of.items()
                  if len(zs) > 1 and e != NEW_ROOT}
    n_collapsed = sum(len(zs) - 1 for zs in collisions.values())
    print(f"\n== 英文路径合并 ==")
    print(f"  唯一英文路径 {len(rows_en)}（中文归一 {len(zh2en)}：翻译撞车 "
          f"{len(collisions)} 组、合并掉 {n_collapsed} 个节点，"
          f"域级对齐中文树）")
    for e in list(collisions)[:5]:
        print(f"    {e}  <-  {collisions[e]}")

    # ---- 实例名清洗统计（全底稿口径，含跨节点多挂的重复原名）----
    variants: "OrderedDict[str, set]" = OrderedDict()
    inst_order: "OrderedDict[str, None]" = OrderedDict()
    for _p, lst in rows_en:
        for x in lst:
            c = clean_name(x)
            variants.setdefault(c, set()).add(x)
            inst_order.setdefault(c, None)
    raw_unique = sum(len(v) for v in variants.values())
    merged_groups = {c: sorted(v) for c, v in variants.items() if len(v) > 1}
    print(f"\n== 实例名清洗 ==")
    print(f"  原名去重 {raw_unique} → 清洗后唯一 {len(inst_order)}"
          f"（合并 {raw_unique - len(inst_order)} 个大小写变体，"
          f"{len(merged_groups)} 组）")
    for c in list(merged_groups)[:5]:
        print(f"    {c}  <-  {merged_groups[c]}")

    # ---- 建英文树 ----
    clean_stats = Counter()
    root, nodes, stats, implicit_paths = build_en_tree(rows_en, clean_stats)
    # 隐式补齐分两档：深度 1 是底稿缺行的骨架域（与中文列差集同源）；
    # 更深的为译名折叠展开产物（如中文段『帝王蟹/蟹』译成 King Crab / Crab
    # 两段，隐式多出 King Crab 中间节点），上报入撞车报告。
    implicit_domains = [p for p in implicit_paths if p.count(SEP) == 1]
    implicit_extra = [p for p in implicit_paths if p.count(SEP) > 1]
    print(f"\n== 英文树 ==")
    print(f"  节点 {stats['node_count']}（底稿 {len(rows_en)} + 隐式补骨架 "
          f"{stats['implicit_nodes']}：骨架域 {len(implicit_domains)} + "
          f"译名折叠展开 {len(implicit_extra)}，叶 {stats['leaf_count']}）")
    for p in implicit_paths:
        print(f"    + {p}")
    print(f"  名单引用 {stats['inst_refs']} 次（节点内清洗去重 "
          f"{clean_stats['deduped_refs']}）")

    # ---- 断言 ----
    assert stats["node_count"] == len(rows_en) + len(implicit_paths)
    assert len(implicit_domains) == len(missing), "骨架域补齐应与中文列差集同源"
    assert stats["node_count"] == \
        len(zh_tree_paths) - n_collapsed + len(implicit_extra), \
        "英文树节点数 = 中文树 - 撞车合并 + 译名折叠展开"
    assert len(root.get("children", [])) == 29, "根应直挂 29 域"
    assert all(e.count(SEP) > 1 for e in collisions), "撞车不应发生在域级"
    for p in nodes:
        assert "Fused World Label System" not in p and \
               "General Classification Tags" not in p, f"旧前缀残留: {p}"

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
    assert len(ref_names) == stats["inst_refs"], "引用口径与树遍历不一致"
    assert set(ref_names) <= set(inst_order), "树引用了实例库之外的名字"
    ref_counter = Counter(ref_names)
    print(f"  唯一引用名 {len(ref_counter)} / 跨节点多挂名 "
          f"{sum(1 for c in ref_counter.values() if c > 1)}")

    # ---- 落盘 ----
    if not args.apply:
        print("\n[干跑] 未落盘。确认后加 --apply。")
        return

    now = datetime.now().isoformat(timespec="seconds")
    tax_doc = {
        "schema_version": "1.1.0",
        "meta": {
            "generated_at": now,
            "source": "融合世界标签体系 v3.1 交付包英文底稿"
                      "（taxonomy_tree_instances_en.csv，2026-08-24 入库；"
                      "前缀归一：根 demiwtg 直挂 29 域）",
            "description": "英文平行标签树（展示视角）：与中文 taxonomy.json "
                           "完全独立，域级对齐；翻译撞车 "
                           f"{len(collisions)} 组自然合并"
                           "（节点非一一对应，名单取并集）；"
                           "节点 KB 字段全空（后续可选生成）。",
        },
        "tree": root,
    }
    TAX_EN_PATH.write_text(
        json.dumps(tax_doc, ensure_ascii=False, separators=(", ", ": ")) + "\n",
        encoding="utf-8")
    inst_doc = {
        "schema_version": "1.1.0",
        "meta": {
            "generated_at": now,
            "source": "融合世界标签体系 v3.1 交付包英文底稿"
                      "（2026-08-24 入库；大小写轻量清洗后同名变体合并）",
            "description": "英文平行实例资产库：与中文 instances.json 完全独立，"
                           "名空间零交集；全量 derived 占位，富知识待后续富化。",
        },
        "instances": [{"name": n, "source": "derived"} for n in inst_order],
    }
    INST_EN_PATH.write_text(
        json.dumps(inst_doc, ensure_ascii=False, separators=(", ", ": ")) + "\n",
        encoding="utf-8")
    CLEAN_REPORT.parent.mkdir(parents=True, exist_ok=True)
    CLEAN_REPORT.write_text(
        json.dumps({"meta": {"generated_at": now,
                             "source": "taxonomy/upgrade_v31_en.py",
                             "note": "英文实例名轻量清洗合并明细"
                                     "（规则：全小写/全大写词转首字母大写，"
                                     "全大写缩写与混排词保留）"},
                    "stats": {"raw_unique": raw_unique,
                              "canonical": len(inst_order),
                              "merged_groups": len(merged_groups)},
                    "merged_groups": merged_groups},
                   ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8")
    MERGE_REPORT.write_text(
        json.dumps({"meta": {"generated_at": now,
                             "source": "taxonomy/upgrade_v31_en.py",
                             "note": "翻译撞车清单：同一英文路径由多个中文节点"
                                     "译成，英文树自然合并（名单并集）；"
                                     "供后续精译修复后重建"},
                    "stats": {"collision_groups": len(collisions),
                              "collapsed_nodes": n_collapsed,
                              "slash_expansion_nodes": implicit_extra},
                    "collisions": {e: zs for e, zs in collisions.items()}},
                   ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8")
    print(f"\n[落盘] {TAX_EN_PATH.name}（{stats['node_count']} 节点）/ "
          f"{INST_EN_PATH.name}（{len(inst_order)} 实例）/ "
          f"{CLEAN_REPORT.relative_to(ROOT)}（{len(merged_groups)} 组清洗合并）/ "
          f"{MERGE_REPORT.relative_to(ROOT)}（{len(collisions)} 组翻译撞车）")


if __name__ == "__main__":
    main()
