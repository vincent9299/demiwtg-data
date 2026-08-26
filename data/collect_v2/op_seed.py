"""collect_v2 种子算子：中文实例 → 检索种子（语言投影），位于 op_search 之前。

契约（.qoder/handoff_collect_v2.md §4.4 + 2026-08-20 用户拍板）：
- 每实例产 1-2 个 seed：中文本体 seed（必有，query=实例名，lang="zh"）+
  西文投影 seed（最多一条，lang="latin"）；
- 西文投影来源：流式让 LLM 判定存量 aliases 中的西文候选是否**同实体西文名**
  （存量 aliases 混类目泛词，旧拍板「query 零信任」，不清洗不得直接消费）；
- 判定上下文含实体知识（2026-08-20 用户拍板：拼入 desc，冷门实体也判得准）：
  desc 全量送判不截断（契约长度本只有 150-350 字）；查表由调用方负责，
  本算子只消费传入的 desc；
- 中文别名变体（慕田峪/慕田峪关这类）**不产 seed**——守住「不选词」纪律；
- LLM 判定结果**落盘词表 + 增量补判**：判过的查表零 LLM 成本，只对新实例调用；
- 判定失败（重试耗尽）→ 不产西文 seed（宁缺毋滥），**不落词表**下次重判。

词表文件：datasets/demiwtg/meta/alias_western.json，格式 {实例名: 西文投影或 null}；
与 instances.json/taxonomy.json 同目录（taxonomy 只读约定针对采集期消费，词表是本算子
专属持久化产物，在 meta/ 白名单登记）。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Optional

import httpx

from collect_v2.op_search import Seed

DEFAULT_ENDPOINT = "http://localhost:8000/v1/chat/completions"
DEFAULT_MODEL = "qwen3.8-27b"
RETRIES = 3               # 判定重试次数（固定间隔，与 infra 口径一致）
RETRY_INTERVAL = 1.0
LLM_TIMEOUT = 60.0        # 纯文本短请求，远小于打标的 600s
MAX_CANDIDATES = 8        # 单实例送判的西文候选上限（防超长别名表）

# 西文候选粗筛：含拉丁字母且非纯符号（缩小送判量，判定本身由 LLM 把关）
_LATIN_RE = re.compile(r"[A-Za-z]")

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


async def _judge(client: httpx.AsyncClient, name: str, cands: list,
                 user_prompt: str, *,
                 endpoint: str, model: str) -> Optional[str]:
    """LLM 判定：返回合格西文投影；全部不合格返回 ""；判定失败返回 None。

    三态区分是契约要求：""（判过无合格→落词表认缺）≠ None（失败→不落词表）。
    """
    payload = {
        "model": model,
        "max_tokens": 80,
        "temperature": 0.0,
        "chat_template_kwargs": {"enable_thinking": False},
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": JUDGE_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
    }
    cand_set = {c.lower() for c in cands}
    for attempt in range(RETRIES):
        try:
            r = await client.post(endpoint, json=payload, timeout=LLM_TIMEOUT)
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
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


async def project(
    name: str,
    aliases,
    cache: SeedCache,
    *,
    desc: str = "",
    client: Optional[httpx.AsyncClient] = None,
    endpoint: str = DEFAULT_ENDPOINT,
    model: str = DEFAULT_MODEL,
) -> list:
    """单实例投影：返回 [中文 seed] 或 [中文 seed, 西文 seed]。

    中文本体 seed 必有；西文 seed 依词表/LLM 判定产出（最多一条）。
    desc 为实体背景知识（instances.json 的 desc，全量送判不截断），
    拼入判定上下文提升冷门实体判定准确率。
    """
    seeds = [Seed(name=name, query=name, lang="zh")]
    judged, value = cache.get(name)
    if not judged:
        cands = latin_candidates(aliases)
        if not cands:
            cache.put(name, None)    # 无西文候选：直接认缺落词表，不费 LLM
        else:
            http = client or httpx.AsyncClient()
            own = client is None
            kb_block = (KB_BLOCK_TPL.format(desc=desc.strip())
                        if desc and desc.strip() else "")
            user_prompt = JUDGE_USER_TPL.format(
                name=name, kb_block=kb_block, cands="、".join(cands))
            try:
                value = await _judge(http, name, cands, user_prompt,
                                     endpoint=endpoint, model=model)
            finally:
                if own:
                    await http.aclose()
            if value is None:        # 判定失败：不落词表，下次重判（宁缺毋滥）
                return seeds
            cache.put(name, value or None)
    if value:
        seeds.append(Seed(name=name, query=value, lang="latin"))
    return seeds
