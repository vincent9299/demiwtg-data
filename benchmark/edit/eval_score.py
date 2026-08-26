"""edit（编辑）赛道判分器（契约 = edit/edit_score_prompts.json，ImgEdit prompts.json 原文）。

- 按题面 edit_type 路由到对应评分 prompt，三视角各 1~5 分；
- 硬约束：二、三维分数不得高于第一维（解析后强制钳制，防质量分虚高）。

responses 契约（每题一行）：{"qid": "...", "image": "产出图路径"}

用法：
    python3 benchmark/edit/eval_score.py --questions Q.jsonl --responses R.jsonl
    python3 benchmark/edit/eval_score.py dump   # 物化 judge prompt 到 data/judge_prompts（审计用）
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import re
import sys
import time
from pathlib import Path

import requests
from PIL import Image

SUB_DIR = Path(__file__).resolve().parent                    # edit/
EVAL_DIR = SUB_DIR / "data"
JUDGE_PROMPTS_DIR = EVAL_DIR / "judge_prompts"
EDIT_PROMPTS_FILE = SUB_DIR / "edit_score_prompts.json"

DEFAULT_ENDPOINT = "http://localhost:8000/v1/chat/completions"
DEFAULT_MODEL = "qwen3.8-27b"

# 每类的三维名称（与 edit_score_prompts.json 的输出格式行一致）
EDIT_DIMS = {
    "replace": ["Prompt Compliance", "Visual Naturalness", "Physical & Detail Integrity"],
    "add": ["Prompt Compliance", "Visual Naturalness", "Physical & Detail Coherence"],
    "adjust": ["Prompt Compliance", "Visual Seamlessness", "Physical & Detail Fidelity"],
    "remove": ["Prompt Compliance", "Visual Naturalness", "Physical & Detail Integrity"],
    "style": ["Style Fidelity", "Content Preservation", "Rendering Quality"],
    "action": ["Action Fidelity", "Identity Preservation", "Visual & Anatomical Coherence"],
    "extract": ["Object Identity", "Mask Precision", "Visual Quality"],
    "background": ["Instruction Compliance", "Visual Seamlessness", "Physical Consistency"],
    "compose": ["Instruction Compliance", "Visual Naturalness", "Physical Consistency & Fine Detail"],
}


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------
def encode_image(path: Path, max_edge: int) -> str:
    img = Image.open(path)
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    w, h = img.size
    if max(w, h) > max_edge:
        k = max_edge / max(w, h)
        img = img.resize((max(1, round(w * k)), max(1, round(h * k))), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def call_judge(endpoint: str, model: str, system: str,
               content: list, api_key: str = "", think: bool = False,
               retries: int = 3, timeout: float = 600.0) -> str:
    payload = {
        "model": model,
        "stream": False,
        "temperature": 0.0,
        "seed": 42,
        "max_tokens": 2048,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": content},
        ],
    }
    if think:
        payload["chat_template_kwargs"] = {"enable_thinking": True}
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    for attempt in range(retries):
        try:
            resp = requests.post(endpoint, json=payload, headers=headers,
                                 timeout=timeout)
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:  # noqa: BLE001
            if attempt == retries - 1:
                raise
            print(f"  [warn] judge 调用失败（{e}），{2 * (attempt + 1)}s 后重试",
                  file=sys.stderr)
            time.sleep(2 * (attempt + 1))
    raise AssertionError("unreachable")


def load_jsonl(path: Path) -> list:
    with path.open(encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def materialize_judge_prompts() -> None:
    """物化判分契约到 data/judge_prompts（审计/调试用；消费者为本脚本自身与人工复核）。"""
    JUDGE_PROMPTS_DIR.mkdir(parents=True, exist_ok=True)
    (JUDGE_PROMPTS_DIR / "edit_score_prompts.json").write_text(
        EDIT_PROMPTS_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"judge prompts 已物化 -> {JUDGE_PROMPTS_DIR}")


# ---------------------------------------------------------------------------
# 编辑赛道判分
# ---------------------------------------------------------------------------
def score_edit(q: dict, img_url: str, src_url: str, edit_prompts: dict,
               args) -> dict:
    etype = q.get("edit_type", "")
    if etype not in edit_prompts:
        raise ValueError(f"未知 edit_type: {etype!r}（qid={q.get('qid')}）")
    instruction = q.get("edit_instruction", "")
    text = edit_prompts[etype].replace("<edit_prompt>", instruction)
    content = [{"type": "text", "text": text},
               {"type": "image_url", "image_url": {"url": src_url}},
               {"type": "image_url", "image_url": {"url": img_url}}]
    raw = call_judge(args.endpoint, args.model,
                     "You are a professional image editing evaluator.",
                     content, api_key=args.api_key, think=args.think)

    dims = EDIT_DIMS[etype]
    scores = []
    for d in dims:
        m = re.search(re.escape(d) + r"\s*[:：]\s*([1-5](?:\.\d)?)", raw)
        scores.append(float(m.group(1)) if m else 1.0)
    # 硬约束：二、三维不得高于第一维（ImgEdit 契约，解析后强制钳制）
    scores[1] = min(scores[1], scores[0])
    scores[2] = min(scores[2], scores[0])
    return {
        "qid": q.get("qid"), "task": "edit", "edit_type": etype,
        "suite": q.get("suite", "basic"),
        "dims": dict(zip(dims, scores)),
        "total": round(sum(scores) / 3, 2),
        "raw": raw,
    }


# ---------------------------------------------------------------------------
# 聚合报表
# ---------------------------------------------------------------------------
def aggregate(rows: list) -> dict:
    by_type: dict = {}
    for r in rows:
        by_type.setdefault(r["edit_type"], []).append(r)
    rep = {"mode": "edit", "n": len(rows),
           "overall": round(sum(r["total"] for r in rows) / len(rows), 2)
           if rows else None,
           "by_type": {
               t: {"n": len(v),
                   "total": round(sum(r["total"] for r in v) / len(v), 2),
                   "dim_means": {
                       d: round(sum(r["dims"].get(d, 0) for r in v) / len(v), 2)
                       for d in EDIT_DIMS[t]}}
               for t, v in sorted(by_type.items())}}
    by_suite: dict = {}
    for r in rows:
        by_suite.setdefault(r.get("suite", "basic"), []).append(r)
    rep["by_suite"] = {s: round(sum(r["total"] for r in v) / len(v), 2)
                       for s, v in by_suite.items()}
    return rep


def run(args) -> None:
    questions = {q["qid"]: q for q in load_jsonl(args.questions)}
    responses = load_jsonl(args.responses)
    edit_prompts = json.loads(EDIT_PROMPTS_FILE.read_text(encoding="utf-8"))
    materialize_judge_prompts()

    out_path = args.out or (EVAL_DIR / "scores_edit.jsonl")
    rows, n_fail = [], 0
    with out_path.open("w", encoding="utf-8") as fout:
        for i, resp in enumerate(responses[: args.limit or None], 1):
            qid = resp.get("qid")
            q = questions.get(qid)
            if q is None:
                print(f"  [warn] responses 中 {qid} 无对应题目，跳过", file=sys.stderr)
                continue
            img = Path(resp["image"])
            if not img.is_absolute():
                img = args.responses.parent / img
            if not img.exists():
                print(f"  [warn] {qid} 产出图缺失: {img}", file=sys.stderr)
                n_fail += 1
                continue
            t0 = time.time()
            try:
                # _sample_image 相对样本数据区（data/）；题库在其子目录，逐级向上找
                src = Path(q["_sample_image"])
                if not src.is_absolute():
                    for base in (args.questions.parent,
                                 args.questions.parent.parent):
                        if (base / src).exists():
                            src = base / src
                            break
                    else:
                        src = args.questions.parent / src
                row = score_edit(q, encode_image(img, args.max_edge),
                                 encode_image(src, args.max_edge),
                                 edit_prompts, args)
            except Exception as e:  # noqa: BLE001
                print(f"  [error] {qid}: {e}", file=sys.stderr)
                n_fail += 1
                continue
            rows.append(row)
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            print(f"[{i}/{len(responses)}] {qid} -> {row['total']} "
                  f"({time.time() - t0:.0f}s)", flush=True)

    rep = aggregate(rows)
    rep["judge_fail"] = n_fail
    rep_path = out_path.with_suffix(".report.json")
    rep_path.write_text(json.dumps(rep, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    print(f"\n=== edit 判分完成 ===")
    print(json.dumps(rep, ensure_ascii=False, indent=2))
    print(f"逐题分数 -> {out_path}\n报表 -> {rep_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="mode")
    p = sub.add_parser("score", help="edit 赛道判分（默认子命令）")
    p.add_argument("--questions", type=Path, required=True)
    p.add_argument("--responses", type=Path, required=True)
    p.add_argument("--out", type=Path, default=None)
    p.add_argument("--limit", type=int, default=0)
    for x in (ap, p):
        x.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
        x.add_argument("--model", default=DEFAULT_MODEL)
        x.add_argument("--api-key", default="")
        x.add_argument("--max-edge", type=int, default=1024)
        x.add_argument("--think", action="store_true",
                       help="启用 thinking（chat_template_kwargs）")
    sub.add_parser("dump", help="只物化 judge prompts")
    args = ap.parse_args()
    if args.mode == "dump":
        materialize_judge_prompts()
        return
    if args.mode is None:
        ap.error("请指定子命令：score 或 dump")
    run(args)


if __name__ == "__main__":
    main()
