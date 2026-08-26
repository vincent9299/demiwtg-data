"""collect_v2 补标算子（2026-08-21 存量迁移新增，op_annotate 零改动）：
输入（存量行字节 + 单实例）→ 只重打 kb_match 并补 identity/focus。

契约（2026-08-21 存量迁移拍板）：
- prompt 与 op_annotate 全量打标**一字不差**（import 复用 SYSTEM_PROMPT，
  五字段定义口径同一，分数与采集链可比），模型仍输出五字段，
  解析层只采信补标子集；
- 补标子集：kb_match（重打，对齐 v2 单实体口径）+ identity/focus（新补）；
  richness/caption 实体无关，沿用存量不重打；
- VLM 失败（编码/重试耗尽）→ 返回 None，调用方写 null 放行（不弃图，
  与 op_annotate 同口径）；
- 实例知识复用 op_annotate.load_instance_kb / build_block（只读查表，
  prompt 只给实体本身不给分类路径）。
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

import httpx

from collect_v2.op_annotate import (
    DEFAULT_ENDPOINT, DEFAULT_MODEL, MAX_TOKENS, RETRIES, RETRY_INTERVAL,
    SYSTEM_PROMPT, USER_PROMPT_TPL, VLM_TIMEOUT, _JSON_RE,
    build_block, encode_for_vlm,
)

# 补标字段子集：kb_match 重打（对齐 v2 单实体口径）+ identity/focus 新补
BACKFILL_FIELDS = ("kb_match", "identity", "focus")


def parse_partial(text: str) -> Optional[dict]:
    """补标解析：只取 kb_match/identity/focus，不校验 richness/caption。"""
    m = _JSON_RE.search(text)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    try:
        km = max(0, min(10, int(d.get("kb_match"))))
        fo = max(0, min(10, int(d.get("focus"))))
    except (TypeError, ValueError):
        return None
    if not isinstance(d.get("identity"), bool):
        return None
    return {"kb_match": km, "identity": d["identity"], "focus": fo}


async def backfill(
    data: bytes,
    instance: str,
    kb: dict,
    *,
    client: Optional[httpx.AsyncClient] = None,
    endpoint: str = DEFAULT_ENDPOINT,
    model: str = DEFAULT_MODEL,
) -> Optional[dict]:
    """对单张存量图按单实例补标，返回 {kb_match, identity, focus} 或 None。

    与 op_annotate._call_vlm 同参数同重试策略（固定间隔、timeout 600s），
    仅解析层换 parse_partial。
    """
    b64 = await asyncio.to_thread(encode_for_vlm, data)
    if b64 is None:
        return None
    http = client or httpx.AsyncClient()
    own_client = client is None
    try:
        payload = {
            "model": model,
            "max_tokens": MAX_TOKENS,
            "temperature": 0.0,
            "chat_template_kwargs": {"enable_thinking": False},
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                    {"type": "text",
                     "text": USER_PROMPT_TPL.format(
                         blocks=build_block(instance, kb))},
                ]},
            ],
        }
        for attempt in range(RETRIES):
            try:
                r = await http.post(endpoint, json=payload, timeout=VLM_TIMEOUT)
                r.raise_for_status()
                content = r.json()["choices"][0]["message"]["content"]
                ann = parse_partial(content)
                if ann is not None:
                    return ann
            except Exception:  # noqa: BLE001 - 与 op_annotate 同：固定间隔重试
                pass
            if attempt < RETRIES - 1:
                await asyncio.sleep(RETRY_INTERVAL)
        return None
    finally:
        if own_client:
            await http.aclose()
