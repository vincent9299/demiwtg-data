"""edit（编辑）赛道出题：按出题 prompt 调 LLM API 对样本出编辑题。

读 edit/data/samples.jsonl（eval_sample.py 产物），每图出一道编辑题；
9 类 edit_type 轮转配额（对齐 ImgEdit Basic 套），每第 5 题强制知识编辑
（知识编辑套），出题 prompt = edit/synthesize_prompt_edit.md
-> edit/data/synth_edit/questions.jsonl

用法：
    GALAXY_API_KEY=sk-... python3 benchmark/edit/eval_synthesize.py [--limit 10]
"""

from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import os
import re
import sys
import time
from pathlib import Path

import requests

SUB_DIR = Path(__file__).resolve().parent             # edit/
EVAL_DIR = SUB_DIR / "data"
PROMPT_FILE = SUB_DIR / "synthesize_prompt_edit.md"
SAMPLES = EVAL_DIR / "samples.jsonl"

API_URL = "https://token.ai-galaxy.com/v1/chat/completions"
MODEL = "qwen3.7-plus"

MAX_TOKENS = 16384
TIMEOUT = (10, 600)              # 连接 10s，读 600s（推理模型输出慢）

# 编辑赛道 9 类（与 edit_score_prompts.json / EDIT_DIMS 一致），轮转出配额
EDIT_TYPES = ["replace", "add", "remove", "adjust", "background",
              "action", "style", "extract", "compose"]
KNOWLEDGE_EDIT_EVERY = 5         # 每第 5 题为知识编辑套


# ---------------------------------------------------------------------------
# 通用：LLM 调用与解析
# ---------------------------------------------------------------------------
def call_api(api_key: str, system_prompt: str,
             user_message: list, tag: str) -> dict:
    payload = {
        "model": MODEL,
        "stream": False,
        "temperature": 0.7,
        "max_tokens": MAX_TOKENS,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    for attempt in range(3):
        try:
            resp = requests.post(API_URL, json=payload, headers=headers,
                                 timeout=TIMEOUT)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
            return resp.json()
        except Exception as e:  # noqa: BLE001
            if attempt == 2:
                raise
            wait = 5 * (attempt + 1)
            print(f"  [warn] {tag} 调用失败（{e}），{wait}s 后重试",
                  file=sys.stderr)
            time.sleep(wait)
    raise AssertionError("unreachable")


def extract_json_array(content: str) -> list[dict]:
    """从模型输出中抠出 JSON 数组（容忍 ```json 围栏与前后杂文）。"""
    text = content.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    start, end = text.find("["), text.rfind("]")
    if start < 0 or end <= start:
        raise ValueError(f"输出中无 JSON 数组: {content[:200]!r}")
    return json.loads(text[start : end + 1])


def encode_image(img_path: Path) -> str:
    mime = mimetypes.guess_type(img_path.name)[0] or "image/jpeg"
    b64 = base64.b64encode(img_path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{b64}"


# ---------------------------------------------------------------------------
# 出题批次
# ---------------------------------------------------------------------------
def load_samples() -> list:
    if not SAMPLES.exists():
        sys.exit(f"样本清单不存在：{SAMPLES}\n"
                 f"请先运行 benchmark/edit/eval_sample.py")
    rows = []
    with SAMPLES.open(encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def audit(questions: list, expect_edit_type: str | None) -> list:
    """字段/词表审计，返回合格题（不合格的只告警不删——保留人工复核）。"""
    kept = []
    for q in questions:
        warns = []
        et = q.get("edit_type")
        if et not in EDIT_TYPES:
            warns.append(f"edit_type 非法: {et!r}")
        elif expect_edit_type and et != expect_edit_type:
            warns.append(f"edit_type 偏移: 要求 {expect_edit_type} 出了 {et}")
        for f in ("edit_instruction", "expected_changes",
                  "preserved_elements"):
            if not q.get(f):
                warns.append(f"缺字段 {f}")
        for f in ("knowledge_dim", "probe_dims", "difficulty",
                  "evidence_audit", "expected_failure_modes"):
            if not q.get(f):
                warns.append(f"缺字段 {f}")
        if warns:
            print(f"  [audit] {q.get('qid', '?')}: {'; '.join(warns)}",
                  file=sys.stderr)
        kept.append(q)
    return kept


def run(api_key: str, limit: int) -> None:
    system_prompt = PROMPT_FILE.read_text(encoding="utf-8")
    system_prompt = system_prompt.replace("{每图题数}", "1")
    samples = load_samples()
    if limit:
        samples = samples[:limit]
    print(f"edit 出题批次：{len(samples)} 个样本", flush=True)

    out_dir = EVAL_DIR / "synth_edit"
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "questions.jsonl"

    n_total = n_skip = 0
    with out_file.open("w", encoding="utf-8") as fout:
        for i, rec in enumerate(samples, 1):
            sid = rec["sample_id"]
            label = rec["instance"]
            img = EVAL_DIR / rec["image"]
            if not img.exists():
                print(f"  [warn] {sid} 图片缺失，跳过", file=sys.stderr)
                continue
            tax = " | ".join(rec.get("mount_paths") or []) or "（未挂载）"
            target_type = EDIT_TYPES[(i - 1) % len(EDIT_TYPES)]
            knowledge = (i % KNOWLEDGE_EDIT_EVERY == 0)
            suite = "knowledge" if knowledge else "basic"
            text = (
                f"样本编号：{sid}\n"
                f"sample_id：{rec['image'].split('/')[-1]}\n"
                f"query_label：{label}\n"
                f"caption：{rec.get('caption', '')}\n"
                f"taxonomy：{tax}\n"
                f"target_edit_type：{target_type}\n"
                f"knowledge_edit：{str(knowledge).lower()}\n\n"
                f"请出 1 道该类型的编辑题，严格执行证据审计。"
                f"只输出 JSON 数组，不要任何其他文字。"
            )
            msg = [{"type": "text", "text": text},
                   {"type": "image_url",
                    "image_url": {"url": encode_image(img)}}]
            tag = f"{sid}({target_type})"
            print(f"[{i}/{len(samples)}] {tag} {label} ...", flush=True)
            t0 = time.time()
            try:
                result = call_api(api_key, system_prompt, msg, tag)
            except Exception as e:  # noqa: BLE001
                print(f"  [error] {tag}: {e}", file=sys.stderr)
                continue
            (raw_dir / f"{sid}.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2),
                encoding="utf-8")
            content = result["choices"][0]["message"]["content"]
            try:
                questions = extract_json_array(content)
            except (ValueError, json.JSONDecodeError) as e:
                print(f"  [error] JSON 解析失败：{e}", file=sys.stderr)
                continue
            questions = audit(questions, target_type)
            for q in questions:
                if q.get("skip"):
                    n_skip += 1
                    continue
                q.setdefault("suite", suite)
                q["_sample_image"] = rec["image"]
                q["_query_label"] = label
                fout.write(json.dumps(q, ensure_ascii=False) + "\n")
                n_total += 1
            print(f"  ok: {len(questions)} 题, {time.time() - t0:.0f}s, "
                  f"tokens={result.get('usage', {}).get('total_tokens', '?')}")
    print(f"\n完成：{n_total} 题（{n_skip} 题类型不适配被 skip）-> {out_file}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=0,
                    help="限样本数；0=全量")
    args = ap.parse_args()

    api_key = os.environ.get("GALAXY_API_KEY")
    if not api_key:
        sys.exit("请通过环境变量 GALAXY_API_KEY 提供 API key")

    run(api_key, args.limit)


if __name__ == "__main__":
    main()
