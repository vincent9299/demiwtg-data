"""data_pipeline 概念批任务算子：概念三字段模型（2026-09-06 定案）。

模型契约：
- 概念行（registry 真相）：{name, aliases[], carriers, taxonomy[]?} ——
  核心三字段 + taxonomy 补充字段（2026-09-06 增：体系路径，可多条，
  体系归属/溯源/策展用，检索链路不消费）；
  name 正名主键定了不改（收词不收算式）；aliases 身份归一（判重/外文
  路由/叫法归一）；carriers 产物形式（image/text/image+text）；
- ⛔ 概念行里没有知识文本——知识在 docs 层（pages/docs.jsonl）；
- 任务策略（配额/优先级/验收 gate）在 plan 段，与 registry 分离，
  不进真相区；candidate 概念只随任务采集、不入 registry。

行契约（demiflow 原生 dict 行）：
- 概念行（输入，含 plan 合并后的配额）：{name, aliases[], carriers,
  min_images}（min_images 为 plan 注入的任务态）
- 种子行（ConceptSeedStage 产出）：{name, query, lang, top_n_hint}——
  name 一枚（CJK→zh）+ aliases 逐条展开（CJK→zh / 其余→latin），
  纯机械零 LLM（SeedCache 词表退役——归一在 registry 策展期完成）
"""

from __future__ import annotations

import json
import os
import re

from demiflow.data.plan import StreamStage

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")

# 配额下限兜底（plan 缺省口径：strict 40 / category 20 / relevance 10）
_GATE_QUOTA = {"strict": 40, "category": 20, "relevance": 10}

# 配额驱动切片假设：单概念有效（种子×源）对 ≈ 56（7 种子 × 8 源），
# 候选存活率 ~0.6 → top_n = ceil(min_images × 1.6 / 56)，下限 1
_PAIRS_ASSUME = 56


def _load_v3(doc: dict) -> tuple:
    """3.0.0 原生：concepts[] 三字段 + plan{}（default/by_gate/priority/per_concept）。"""
    plan = doc.get("plan") or {}
    default_q = (plan.get("default") or {}).get("quota_images", 20)
    by_gate = {k: v.get("quota_images", v) if isinstance(v, dict) else v
               for k, v in (plan.get("by_gate") or {}).items()}
    per = {k: v.get("quota_images") if isinstance(v, dict) else v
           for k, v in (plan.get("per_concept") or {}).items()}
    rows, seen = [], set()
    for c in doc.get("concepts") or []:
        name = (c.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        rows.append({
            "name": name,
            "aliases": [a for a in (c.get("aliases") or []) if str(a).strip()],
            "carriers": c.get("carriers") or "image+text",
            "taxonomy": [t for t in (c.get("taxonomy") or []) if str(t).strip()],
            "min_images": int(per.get(name, default_q) or default_q),
        })
    return rows, plan


def _load_v2(doc: dict) -> tuple:
    """2.0.0 适配（concepts_batch_200 形态）：三字段提取 + 配额进 plan；
    desc/query/gate/provenance 不进概念行（desc 由迁移脚本导入 docs 层）。"""
    rows, seen = [], set()
    for c in doc.get("concepts") or []:
        name = (c.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        gate = c.get("gate") or "category"
        quota = (c.get("collect") or {}).get(
            "min_images", _GATE_QUOTA.get(gate, 20))
        rows.append({
            "name": name,
            "aliases": [a for a in (c.get("aliases") or []) if str(a).strip()],
            "carriers": c.get("carriers") or "image+text",
            "taxonomy": [t for t in (c.get("taxonomy") or []) if str(t).strip()],
            "min_images": int(quota or 20),
        })
    return rows, {}


def load_concepts(path: str) -> tuple:
    """批任务 json → (概念行列表, plan)。schema 3.0 原生 / 2.0 适配。"""
    doc = json.loads(open(path, encoding="utf-8").read())
    major = str(doc.get("schema_version", "")).split(".")[0]
    if major == "3":
        return _load_v3(doc)
    if major == "2":
        return _load_v2(doc)
    raise ValueError(f"不支持的 schema_version: {doc.get('schema_version')!r}")


def concept_coverage(manifest: str, names: set) -> dict:
    """image.jsonl 现算 {概念名: 已采行数}（配额循环只看量；验收 gate 留 backfill）。"""
    counts: dict = {n: 0 for n in names}
    if not os.path.exists(manifest):
        return counts
    with open(manifest, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            for name in rec.get("concepts") or []:
                if name in counts:
                    counts[name] += 1
    return counts


class ConceptSeedStage(StreamStage):
    """概念行 → 种子行集（name + aliases 机械展开，零 LLM）。

    top_n_hint 配额驱动：ceil(min_images × 1.6 / 56)，下限 1。
    """

    label = "seed"
    concurrency = 16
    queue_depth = 64

    async def __call__(self, concept: dict):
        hint = max(1, (concept["min_images"] * 8 + 5 * _PAIRS_ASSUME - 1)
                   // (5 * _PAIRS_ASSUME))
        seeds = [{"name": concept["name"], "query": concept["name"],
                  "lang": "zh" if _CJK_RE.search(concept["name"]) else "latin",
                  "top_n_hint": hint}]
        for a in concept["aliases"]:
            a = str(a).strip()
            if not a or a == concept["name"]:
                continue
            seeds.append({"name": concept["name"], "query": a,
                          "lang": "zh" if _CJK_RE.search(a) else "latin",
                          "top_n_hint": hint})
        return seeds
