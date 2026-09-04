"""collect_v2 投影算子：实例行 → 种子行集（自包含，2026-09-04·十 dict 行化）。

行契约（demiflow 原生 dict 行）：
- 读键：name、aliases?、desc?
- 产行：{name, query, lang}（中文本体 seed 必有；西文投影 seed 依词表/LLM
  判定产出，最多一条）

知识口径（与存量词表可比，勿动）：
- 三态语义：str=合格西文投影；None=判过无合格（认缺不重判）；
  未判=触发 LLM 判定；判定失败（None 返回）不落词表下次重判；
- 防幻觉：选中必须是送判候选之一（不许 LLM 自造新名）；
- 西文候选粗筛：含拉丁字母、去重保序、封顶 MAX_CANDIDATES。

资源：LLM 端点 demiflow 平台注册表（"demiwtg_vlm"，声明在 annotate.py）。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Optional

from demiflow.collect.llm import get_llm_client
from demiflow.data.plan import StreamStage

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_ALIAS_CACHE = os.path.join(
    REPO_ROOT, "datasets", "demiwtg", "meta", "alias_western.json")

JUDGE_SYSTEM = (
    "你是实体别名审核专家。给定一个实体名称、可能的实体背景知识与若干候选西文词，"
    "逐个判断候选是否为**该实体本身的西文名称**（英文名、拉丁学名、"
    "规范音译名），严格输出 JSON，不要输出其他内容。\n"
    '格式：{"picked":"<选中的候选原文>"} 或 {"picked":null}\n'
    "判 picked 的标准（全部满足才可）：\n"
    "1. 指代同一实体：专有名词或学名，不是类目泛词（如跳绳的 'fitness'）、"
    "不是活动/赛事/产品等周边概念（如 'competition'、'equipment'）、"
    "也不是与之相关的另一个独立实体（如机构与其主办/承办的活动）的正式名称；\n"
    "2. 是名称而非描述：短语级专名可以（如 'Mutianyu Great Wall'），"
    "描述性句子不行；\n"
    "3. 多个候选都合格时，选最规范、最常用的一个；都不合格输出 null。"
)

# 实体背景知识段：desc 全量拼入不截断（契约长度 150-350 字，无超长风险）
JUDGE_USER_TPL = "实体：{name}\n{kb_block}候选：{cands}\n请判定。"
KB_BLOCK_TPL = "实体背景知识：{desc}\n"

RETRIES = 3               # 判定重试次数（固定间隔，与 demiflow.collect.net 口径一致）
RETRY_INTERVAL = 1.0
LLM_TIMEOUT = 60.0        # 纯文本短请求，远小于打标的 600s
MAX_CANDIDATES = 8        # 单实例送判的西文候选上限（防超长别名表）

# 西文候选粗筛：含拉丁字母且非纯符号（缩小送判量，判定本身由 LLM 把关）
_LATIN_RE = re.compile(r"[A-Za-z]")


class SeedCache:
    """判定结果词表：落盘 + 增量补判（用户拍板）。

    值语义：str=合格西文投影；None=判过但无合格投影（不重判，认缺）；
    键不存在=未判过（触发 LLM 判定）。
    """

    def __init__(self, path: str):
        self.path = path
        self.table: dict = {}
        if os.path.exists(path):
            try:
                self.table = json.loads(open(path, encoding="utf-8").read())
            except (json.JSONDecodeError, OSError):
                self.table = {}   # 词表损坏从零重建，判定幂等可重跑

    def get(self, name: str) -> tuple:
        """返回 (是否已判过, 投影值)。"""
        if name in self.table:
            return True, self.table[name]
        return False, None

    def put(self, name: str, value: Optional[str]) -> None:
        self.table[name] = value

    def save(self) -> None:
        """原子落盘（临时文件同目录 + os.replace）。"""
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.table, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.path)


def latin_candidates(aliases) -> list:
    """aliases → 西文候选粗筛（含拉丁字母，去重保序，封顶 MAX_CANDIDATES）。"""
    out, seen = [], set()
    for a in aliases or []:
        a = str(a).strip()
        if not a or not _LATIN_RE.search(a) or a.lower() in seen:
            continue
        seen.add(a.lower())
        out.append(a)
        if len(out) >= MAX_CANDIDATES:
            break
    return out


async def _judge(name: str, cands: list, user_prompt: str) -> Optional[str]:
    """LLM 判定：返回合格西文投影；全部不合格返回 ""；判定失败返回 None。

    三态区分是契约要求：""（判过无合格→落词表认缺）≠ None（失败→不落词表）。
    机制（HTTP/参数构造）在 demiflow 平台端点资源；口径（三态语义/
    防幻觉校验/重试循环）在本函数。"""
    messages = [
        {"role": "system", "content": JUDGE_SYSTEM},
        {"role": "user", "content": user_prompt},
    ]
    cand_set = {c.lower() for c in cands}
    for attempt in range(RETRIES):
        try:
            content = await get_llm_client("demiwtg_vlm").chat(
                messages, max_tokens=80, temperature=0.0,
                json_mode=True, thinking=False, timeout=LLM_TIMEOUT)
            m = re.search(r"\{[^{}]*\"picked\"[^{}]*\}", content, re.S)
            if m:
                picked = json.loads(m.group(0)).get("picked")
                if picked is None:
                    return ""
                picked = str(picked).strip()
                # 防幻觉：选中的必须是送判候选之一（不许 LLM 自造新名）
                if picked.lower() in cand_set:
                    return picked
        except Exception:  # noqa: BLE001 - 网络/服务端错误固定间隔重试
            pass
        if attempt < RETRIES - 1:
            await asyncio.sleep(RETRY_INTERVAL)
    return None


async def project(name: str, aliases: list, cache: SeedCache,
                  *, desc: str = "") -> list:
    """单实例投影：返回 [{name, query, lang}] 种子行集。

    中文本体 seed 必有；西文 seed 依词表/LLM 判定产出（最多一条）。
    desc 为实体背景知识（instances.json 的 desc，全量送判不截断），
    拼入判定上下文提升冷门实体判定准确率。
    """
    seeds = [{"name": name, "query": name, "lang": "zh"}]
    judged, value = cache.get(name)
    if not judged:
        cands = latin_candidates(aliases)
        if not cands:
            cache.put(name, None)    # 无西文候选：直接认缺落词表，不费 LLM
        else:
            kb_block = (KB_BLOCK_TPL.format(desc=desc.strip())
                        if desc and desc.strip() else "")
            user_prompt = JUDGE_USER_TPL.format(
                name=name, kb_block=kb_block, cands="、".join(cands))
            value = await _judge(name, cands, user_prompt)
            if value is None:        # 判定失败：不落词表，下次重判（宁缺毋滥）
                return seeds
            cache.put(name, value or None)
    if value:
        seeds.append({"name": name, "query": value, "lang": "latin"})
    return seeds


class SeedStage(StreamStage):
    """投影算子（demiflow 规范）：实例行 → 种子行集。

    策略默认值随算子声明，编排层可按 label 覆盖并发/队列深度。"""
    label = "seed"
    concurrency = 16
    queue_depth = 64

    def __init__(self, cache: SeedCache):
        self.cache = cache

    async def __call__(self, inst):
        return await project(inst["name"], inst.get("aliases") or [],
                             self.cache, desc=inst.get("desc") or "")
