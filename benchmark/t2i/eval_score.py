"""t2i（生成）赛道判分器（参照 Qwen-Image-Bench 2605.28091 §3.3-3.4）。

- 双线判分一次 judge 调用：知识线（本题 implicit_checks，逐条 {0,1,2}）
  + 通用线（本模块 FACETS 裁剪子集，按题面 facet_tags 激活，{0,1,2,NA}）；
- 非线性映射 phi：0->0、1->60、2->100（Pass 定在及格线，放大不合格/合格落差）；
- 单题总分 = 0.5*知识线 + 0.5*通用线；判分器判定主体缺失/主题跑偏时封顶 30。

responses 契约（每题一行）：{"qid": "...", "image": "产出图路径"}

用法：
    python3 benchmark/t2i/eval_score.py --questions Q.jsonl --responses R.jsonl
    python3 benchmark/t2i/eval_score.py dump   # 物化 judge prompt 到 data/judge_prompts（审计用）
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

SUB_DIR = Path(__file__).resolve().parent                    # t2i/
EVAL_DIR = SUB_DIR / "data"
JUDGE_PROMPTS_DIR = EVAL_DIR / "judge_prompts"

DEFAULT_ENDPOINT = "http://localhost:8000/v1/chat/completions"
DEFAULT_MODEL = "qwen3.8-27b"
PHI = {0: 0.0, 1: 60.0, 2: 100.0}       # QIB 式非线性映射（1=及格线 60）
GATE_CAP = 30.0                          # 主体缺失/主题跑偏总分封顶
KNOWLEDGE_WEIGHT = 0.5                   # 知识线 : 通用线 = 1 : 1

# ---------------------------------------------------------------------------
# 通用线 facet 表（权威源在此，出题 prompt 的 facet_tags 词表与它一致；
# eval_synthesize 导入本表做审计）。按 Qwen-Image-Bench（2605.28091）Tab.7
# 的 L1 支柱→L2 子能力→L3 细则三级结构重组（41 项）：
#   - 剔除 Creative Generation 支柱（用户拍板）；其 Logical Resolution 细则以
#     causal_reasoning 名义升格入 Real-world Fidelity/知识推理；
#   - 剔除 Fairness 与 Safety & Compliance（论文自报各模型均匀贴 60 分，
#     对知识探针题无区分度）；
#   - Real-world Fidelity 重点扩充：World Knowledge 新增地标/角色/自然/器械 4 项，
#     并新增知识推理子能力（因果/关系/反事实）3 项；
#   - Alignment 新增 Subject 子能力 1 项（subject_prominence 主体性：QIB 未覆盖的
#     「主体在场但不主导」灰区，与 gate 的缺失/跑偏判定互补）。
# ---------------------------------------------------------------------------
FACETS = [
    # key, 所属 L1 支柱, L2 子能力, 判分准则（中文化自 QIB Tab.7）
    # —— Quality ——
    ("physical_logic", "Quality", "Realism",
     "图像是否遵循真实世界物理规律（重力、反射、阴影方向、物体稳定性）？"),
    ("material_texture", "Quality", "Realism",
     "物体表面材质（皮肤、织物、金属、木材等）是否呈现真实的质感与材料属性？"),
    ("noise", "Quality", "Detail",
     "图像是否细节丰富且无过度噪点或不自然平滑？"),
    ("edge_clarity", "Quality", "Detail",
     "物体轮廓与边缘是否清晰锐利，无模糊或锯齿？"),
    ("naturalness", "Quality", "Detail",
     "图像是否自然，无 AI 生成常见的塑料感/油腻感？"),
    ("resolution", "Quality", "Resolution",
     "图像整体分辨率是否高清，无可见像素化或压缩伪影？"),
    # —— Aesthetics ——
    ("composition", "Aesthetics", "Composition",
     "构图是否平衡、有视觉引导、符合审美？"),
    ("color_harmony", "Aesthetics", "Color Harmony",
     "整体色彩搭配是否和谐统一、契合画面情绪？"),
    ("lighting_atmosphere", "Aesthetics", "Lighting",
     "光影氛围（明暗对比、整体光效）是否与提示词的场景设定匹配？"),
    ("anatomical_fidelity", "Aesthetics", "Anatomical Portraiture",
     "人物/动物的五官比例、骨骼结构、肢体关节是否符合解剖学？皮肤是否有毛孔细纹等真实微观质感？"),
    ("emotional_expression", "Aesthetics", "Emotional Expression",
     "图像整体审美基调是否有效传达提示词意图的情绪与氛围？"),
    ("style_control", "Aesthetics", "Style Control",
     "图像是否准确呈现提示词要求的特定艺术风格？"),
    # —— Alignment ——
    ("subject_prominence", "Alignment", "Subject",
     "提示词明写的主体是否占据画面主导地位（体量、位置、视觉焦点）？主体存在却被边缘化或被背景喧宾夺主应降分；主体缺失/主题跑偏由 gate 接管，不在本维判。"),
    ("quantity", "Alignment", "Attributes",
     "画面中物体数量是否与提示词指定的数量一致？"),
    ("facial_expression", "Alignment", "Attributes",
     "人物/动物的面部表情是否准确反映提示词指定的情绪状态？"),
    ("material_properties", "Alignment", "Attributes",
     "物体的材质是否与提示词的材质描述一致？"),
    ("color", "Alignment", "Attributes",
     "物体颜色是否与提示词的颜色指定一致？"),
    ("shape", "Alignment", "Attributes",
     "物体形状是否与提示词的形状描述一致？"),
    ("size", "Alignment", "Attributes",
     "物体尺寸是否与提示词的规格一致？"),
    ("contact_interaction", "Alignment", "Actions",
     "若提示词涉及主体间物理接触，接触交互是否描绘自然真实？"),
    ("noncontact_interaction", "Alignment", "Actions",
     "若提示词涉及主体间非接触关系，空间与社会关系是否描绘自然合乎逻辑？"),
    ("fullbody_action", "Alignment", "Actions",
     "主体（人/动物）的整体姿态与肢体动作是否准确执行提示词描述的活动？"),
    ("spatial_2d", "Alignment", "Layout",
     "物体在 2D 平面上的相对位置（左右/上下/前后景）是否符合提示词空间指令？"),
    ("spatial_3d", "Alignment", "Layout",
     "物体在 3D 空间中的布局、遮挡与相对位置是否符合提示词或空间逻辑？"),
    ("composition_relationship", "Alignment", "Relations",
     "多个元素是否被整合为视觉连贯、逻辑一致的整体？"),
    ("difference_similarity", "Alignment", "Relations",
     "物体间被指定的形状/颜色/材质差异或相似是否被准确表现？"),
    ("containment", "Alignment", "Relations",
     "物体间的包含或围合关系是否被正确描绘？"),
    ("real_world_scene", "Alignment", "Scene",
     "场景类型与环境设定是否与提示词描述的地点一致？"),
    ("virtual_scene", "Alignment", "Scene",
     "虚构/奇幻场景内的元素是否内部自洽、逻辑连贯？"),
    # —— Real-world Fidelity ——
    ("animals", "Real-world Fidelity", "World Knowledge",
     "真实动物的解剖特征与生物细节（肢体结构、毛羽分布、物种特征）是否准确？"),
    ("objects", "Real-world Fidelity", "World Knowledge",
     "真实世界物品的典型外观、结构、标志或标志性特征是否被准确再现？"),
    ("information_visualization", "Real-world Fidelity", "World Knowledge",
     "抽象或科学概念的可视化转译是否准确、清晰、易于理解？"),
    ("temporal_characteristics", "Real-world Fidelity", "World Knowledge",
     "图像是否准确体现特定历史时期的标志性元素（技术、服饰、建筑、生活方式）？"),
    ("cultural_elements", "Real-world Fidelity", "World Knowledge",
     "文化元素（符号、传统服饰、仪式、习俗）是否与真实世界文化实践一致？"),
    ("landmark_identity", "Real-world Fidelity", "World Knowledge",
     "著名建筑、地标与地理景观的形制、结构与标志性细节是否与真实世界一致？"),
    ("character_likeness", "Real-world Fidelity", "World Knowledge",
     "知名人物/虚构角色的标志性外观特征（形象、服饰、道具、符号）是否凭世界知识准确呈现？"),
    ("nature_morphology", "Real-world Fidelity", "World Knowledge",
     "植物形态、天象、地质构造等自然事物的形态特征是否符合自然科学事实？"),
    ("tech_machinery", "Real-world Fidelity", "World Knowledge",
     "车辆、机械、航天器等技术装备的结构与形制是否符合真实工程逻辑？"),
    ("causal_reasoning", "Real-world Fidelity", "Knowledge Reasoning",
     "事件间因果关系是否被准确描绘（如玻璃碎→碎片飞溅、雨→地面湿润）？"),
    ("relational_reasoning", "Real-world Fidelity", "Knowledge Reasoning",
     "实体间隐含的数量比较、比例、次序等关系是否需一步推理后仍描绘正确？"),
    ("counterfactual_coherence", "Real-world Fidelity", "Knowledge Reasoning",
     "反事实/假设前提下，衍生视觉细节是否保持逻辑自洽（如光影、倒影随设定相应变化）？"),
]
FACET_KEYS = [f[0] for f in FACETS]

T2I_SYSTEM = "你是文生图评测专家。给定生成提示词与生成图，你按结构化清单逐项判分，严格输出 JSON，不输出其他内容。"

T2I_USER_TPL = """# 生成提示词
{gen_prompt}

# 生成图
<image>

# 打分规则
每个校验点打分：0（Fail：明显缺陷/未达成）、1（Pass：基本达成，无可见缺陷）、2（Excel：出色，有具体可察的优秀表现）；不适用的通用维度打 "N/A"。

# 一、知识校验点（逐条判，按题面定义）
{checks_block}

# 二、门槛判定
主体是否缺失、主题是否跑偏（是则 gate=true）。

# 三、通用维度清单（仅判列出的维度）
{facets_block}

# 输出格式（只输出合法 JSON，不要 markdown 围栏）：
{{"gate": {{"subject_missing": true或false, "reason": "10字内"}}, "knowledge_checks": [{{"index": 0, "score": 0}}, {{"index": 1, "score": 2}}], "facets": {{"physical_logic": 1, "color": "N/A"}}}}"""


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


def extract_json(content: str) -> dict:
    """从 judge 输出中抠出第一个合法 JSON 对象（容忍前后杂文/多次拼接）。"""
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip())
    start = text.find("{")
    if start < 0:
        raise ValueError(f"输出无 JSON 对象: {content[:150]!r}")
    dec = json.JSONDecoder()
    while start >= 0:
        try:
            obj, _ = dec.raw_decode(text[start:])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
        start = text.find("{", start + 1)
    raise ValueError(f"输出无合法 JSON 对象: {content[:150]!r}")


def load_jsonl(path: Path) -> list:
    with path.open(encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def materialize_judge_prompts() -> None:
    """物化判分契约到 data/judge_prompts（审计/调试用；消费者为本脚本自身与人工复核）。"""
    JUDGE_PROMPTS_DIR.mkdir(parents=True, exist_ok=True)
    (JUDGE_PROMPTS_DIR / "facet_taxonomy.json").write_text(
        json.dumps([{"key": k, "pillar": p, "sub": s, "criterion": c}
                    for k, p, s, c in FACETS],
                   ensure_ascii=False, indent=2),
        encoding="utf-8")
    (JUDGE_PROMPTS_DIR / "t2i_judge_template.md").write_text(
        f"SYSTEM:\n{T2I_SYSTEM}\n\nUSER:\n{T2I_USER_TPL}", encoding="utf-8")
    print(f"judge prompts 已物化 -> {JUDGE_PROMPTS_DIR}")


# ---------------------------------------------------------------------------
# 生成赛道判分
# ---------------------------------------------------------------------------
def score_t2i(q: dict, img_url: str, args) -> dict:
    checks = q.get("implicit_checks") or []
    checks_block = "\n".join(
        f"{i}. {c.get('check', '')}（考察知识：{c.get('knowledge', '')}，权重 {c.get('weight', 0)}）"
        for i, c in enumerate(checks))
    tags = [t for t in (q.get("facet_tags") or []) if t in FACET_KEYS]
    criteria = {k: c for k, _, _, c in FACETS}
    facets_block = "\n".join(f"- {k}: {criteria[k]}" for k in tags) or "-（本题未激活通用维度）"
    user = T2I_USER_TPL.format(gen_prompt=q.get("gen_prompt", ""),
                               checks_block=checks_block or "（无）",
                               facets_block=facets_block)
    content = [{"type": "text", "text": user.replace("<image>", "")},
               {"type": "image_url", "image_url": {"url": img_url}}]
    raw = call_judge(args.endpoint, args.model, T2I_SYSTEM, content,
                     api_key=args.api_key, think=args.think)
    parsed = extract_json(raw)

    # 知识线：权重加和（题面权重合计 1.0）
    kscores = {c["index"]: c["score"] for c in parsed.get("knowledge_checks", [])
               if isinstance(c.get("score"), int) and c["score"] in (0, 1, 2)}
    ksum_w = sum(c.get("weight", 0) for c in checks) or 1.0
    knowledge = sum(checks[i].get("weight", 0) * PHI[kscores.get(i, 0)]
                    for i in range(len(checks))) / ksum_w

    # 通用线：激活且非 N/A 的 facet 均值
    fscores = parsed.get("facets", {})
    vals = [PHI[s] for t in tags
            if isinstance((s := fscores.get(t)), int) and s in (0, 1, 2)]
    general = sum(vals) / len(vals) if vals else None

    total = (KNOWLEDGE_WEIGHT * knowledge
             + (1 - KNOWLEDGE_WEIGHT) * (general if general is not None else knowledge))
    gate = (parsed.get("gate") or {}).get("subject_missing") is True
    if gate:
        total = min(total, GATE_CAP)
    return {
        "qid": q.get("qid"), "task": "t2i",
        "knowledge_score": round(knowledge, 2),
        "general_score": None if general is None else round(general, 2),
        "total": round(total, 2),
        "gate_capped": gate,
        "facet_scores": fscores,
        "check_scores": kscores,
        "raw": raw,
    }


# ---------------------------------------------------------------------------
# 聚合报表
# ---------------------------------------------------------------------------
def aggregate(rows: list) -> dict:
    def mean(key):
        vals = [r[key] for r in rows if r.get(key) is not None]
        return round(sum(vals) / len(vals), 2) if vals else None
    rep = {"mode": "t2i", "n": len(rows),
           "overall": mean("total"),
           "knowledge": mean("knowledge_score"),
           "general": mean("general_score"),
           "gate_capped": sum(1 for r in rows if r.get("gate_capped"))}
    return rep


def run(args) -> None:
    questions = {q["qid"]: q for q in load_jsonl(args.questions)}
    responses = load_jsonl(args.responses)
    materialize_judge_prompts()

    out_path = args.out or (EVAL_DIR / "scores_t2i.jsonl")
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
                row = score_t2i(q, encode_image(img, args.max_edge), args)
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
    print(f"\n=== t2i 判分完成 ===")
    print(json.dumps(rep, ensure_ascii=False, indent=2))
    print(f"逐题分数 -> {out_path}\n报表 -> {rep_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="mode")
    p = sub.add_parser("score", help="t2i 赛道判分（默认子命令）")
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
