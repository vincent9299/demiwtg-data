"""t2i（生成）赛道出题：按出题 prompt 调 LLM API 对样本出 T2I 题。

读 t2i/data/samples.jsonl（eval_sample.py 产物），每图出一道生成题
（出题 prompt = t2i/synthesize_prompt_gen.md），产出
-> t2i/data/synth_gen/questions.jsonl

用法：
    GALAXY_API_KEY=sk-... python3 benchmark/t2i/eval_synthesize.py [--limit 10]
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

BENCH_ROOT = Path(__file__).resolve().parent.parent   # t2i/ -> benchmark/
sys.path.insert(0, str(BENCH_ROOT))

from t2i.eval_score import FACET_KEYS                 # noqa: E402  facet 词表权威源

SUB_DIR = Path(__file__).resolve().parent             # t2i/
EVAL_DIR = SUB_DIR / "data"
PROMPT_FILE = SUB_DIR / "synthesize_prompt_gen.md"
SAMPLES = EVAL_DIR / "samples.jsonl"

API_URL = "https://token.ai-galaxy.com/v1/chat/completions"
MODEL = "qwen3.7-plus"

MAX_TOKENS = 16384
TIMEOUT = (10, 600)              # 连接 10s，读 600s（推理模型输出慢）


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
                 f"请先运行 benchmark/t2i/eval_sample.py")
    rows = []
    with SAMPLES.open(encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def audit(questions: list) -> list:
    """字段/权重/词表审计，返回合格题（不合格的只告警不删——保留人工复核）。"""
    kept = []
    for q in questions:
        warns = []
        checks = q.get("implicit_checks") or []
        if not checks:
            warns.append("缺 implicit_checks")
        total = sum(c.get("weight", 0) for c in checks)
        if checks and abs(total - 1.0) > 0.05:
            warns.append(f"implicit_checks 权重和 {total:.2f} != 1.0")
        tags = q.get("facet_tags") or []
        bad = [t for t in tags if t not in FACET_KEYS]
        if bad:
            warns.append(f"facet_tags 含未知词: {bad}")
        if not 3 <= len(tags) <= 6:
            warns.append(f"facet_tags 数量 {len(tags)} 不在 3~6")
        if not q.get("gen_prompt"):
            warns.append("缺 gen_prompt")
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
    print(f"t2i 出题批次：{len(samples)} 个样本", flush=True)

    out_dir = EVAL_DIR / "synth_gen"
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
            text = (
                f"样本编号：{sid}\n"
                f"sample_id：{rec['image'].split('/')[-1]}\n"
                f"query_label：{label}\n"
                f"caption：{rec.get('caption', '')}\n"
                f"taxonomy：{tax}\n\n"
                f"请出 1 道生成题，严格执行证据审计。"
                f"只输出 JSON 数组，不要任何其他文字。"
            )
            msg = [{"type": "text", "text": text},
                   {"type": "image_url",
                    "image_url": {"url": encode_image(img)}}]
            print(f"[{i}/{len(samples)}] {sid} {label} ...", flush=True)
            t0 = time.time()
            try:
                result = call_api(api_key, system_prompt, msg, sid)
            except Exception as e:  # noqa: BLE001
                print(f"  [error] {sid}: {e}", file=sys.stderr)
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
            questions = audit(questions)
            for q in questions:
                if q.get("skip"):
                    n_skip += 1
                    continue
                q.setdefault("suite", "basic")
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
