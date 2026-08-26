#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""mount_map.py — taxonomy 树挂载点聚合（只读现算，不落盘）。

解耦契约（AGENTS.md 1.5）：instances.json 是独立权威源（实体资产），
taxonomy.json 是展示视角（树 + 节点 instances 名单引用实例名）。两者的
关联关系不写进任何一方文件；需要「实例名 → 挂载路径」的消费者（缺口
分析、LLM 富化的分类上下文、涌现对齐等）通过本模块从树现场聚合。

未挂载任何树节点的实例是合法状态（待认领池），不会出现在返回的映射里。
"""
from __future__ import annotations

import json
from collections import defaultdict


def load_mount_map(taxonomy_path):
    """读 taxonomy.json，返回 {实例名: [节点路径, ...]}（按树遍历序去重）。

    树引用了脏数据（dict 形态的旧挂载项）时取其 name；空名/无 path 的
    引用直接忽略（结构卫生问题由审计工具报告，本函数只读不纠）。
    """
    with open(taxonomy_path, encoding="utf-8") as f:
        tree = json.load(f).get("tree") or {}
    mounts = defaultdict(list)

    def walk(n):
        path = n.get("path", "")
        for nm in n.get("instances") or []:
            if isinstance(nm, dict):
                nm = nm.get("name")
            nm = str(nm).strip() if nm is not None else ""
            if nm and path and path not in mounts[nm]:
                mounts[nm].append(path)
        for ch in n.get("children") or []:
            walk(ch)

    walk(tree)
    return dict(mounts)


def tree_sibling_of(instances_path):
    """instances.json 的同目录 taxonomy.json 路径（两文件约定同居 datasets/demiwtg/meta/）。"""
    import os
    return os.path.join(os.path.dirname(os.path.abspath(instances_path)),
                        "taxonomy.json")
