"""collect_v2 标注算子：输入 Item（已下载）→ 在同一 Item 上追加标注字段。

契约（.qoder/handoff_collect_v2.md §4.2 + 2026-08-21 拍板）：
- 端到端含 VLM 消费方，位于 sink 之前；
- prompt 沿用旧系统口径（与 31.3 万存量记录同口径、分数可比），
  追加 identity 字段（主体是否即该实体，独立于 kb_match 的吻合度裁决）；
- 2026-08-20 用户拍板追加 focus（主体显著度）与 quality（综合分，算子内派生）：
  背景——源质量实验发现活动照（多主体之一）在 kb_match/identity 双高下仍被高估，
  focus 补齐「画面是否主要在表达该实体」维度；quality=0.4*kb+0.4*focus+0.2*richness，
  权重经 49 图原型实验验证（华航剪彩照 10/5→7.6，独占立绘→9.6+）；
  只打分不把关的口径不变：quality 供消费层排序/分层，链上不做阈值拒收；
- VLM 失败（网络/解析重试耗尽）→ **无标注放行**（字段留 None），不弃图；
- 只打分不把关：不做任何阈值拒收（kb_match 分段已定性不是归属验收闸门）；
- 实例知识来自 datasets/demiwtg/meta/instances.json 只读查表（name→desc/aliases），
  prompt 只给实体本身不给分类路径（旧约定）；
- 预处理（缩最长边 + JPEG base64）只影响模型输入，不动 Item.data 原始字节。
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import re
from typing import Optional

import httpx
from PIL import Image

from collect_v2.op_search import Item

DEFAULT_ENDPOINT = "http://localhost:8000/v1/chat/completions"
DEFAULT_MODEL = "qwen3.8-27b"
MAX_EDGE = 768            # 送模型前最长边缩放阈值；2026-08-22 由 1024 降至 768（用户拍板）：
                          # 40 图 A/B 实测 85%+ 打分完全一致、均值偏移 ≤±0.25、identity 仅 1 翻转，
                          # 质量代价可忽略，prefill 视觉 token 减 ~44% 换打标吞吐（只影响新图口径）
JPEG_QUALITY = 85
MAX_TOKENS = 600
RETRIES = 3               # VLM 调用重试次数（固定间隔，不做指数退避）
RETRY_INTERVAL = 1.0
VLM_TIMEOUT = 600.0       # 旧系统实测：并发下单请求可达 100-300s，给足
DESC_CHARS = 250          # desc 截断长度（旧口径）
CAPTION_MIN = 40          # caption 低于该字数视为解析失败（旧口径）

# 综合分权重（2026-08-20 用户拍板，49 图原型实验验证区分度）
QUALITY_WEIGHTS = (0.4, 0.4, 0.2)     # kb_match / focus / richness

# 沿用旧 SYSTEM_PROMPT，追加 identity 字段（用户拍板：沿用 + 新增）；
# 2026-08-20 再追加 focus（主体显著度，源质量实验拍板）
SYSTEM_PROMPT = (
    "你是 IP 图片数据集的打标专家。对每张图结合所给实体知识完成五项标注，"
    "严格按 JSON 输出，不要输出其他内容。\n"
    '格式：{"kb_match":0-10的整数,"richness":0-10的整数,'
    '"identity":true或false,"focus":0-10的整数,"caption":"详细中文描述"}\n'
    "kb_match（实体匹配度）：图中内容与所述实体的吻合程度。\n"
    "  9-10=主体即该实体且核心特征完全吻合；7-8=主体吻合但细节/版本有出入；"
    "4-6=相关但主体不明确（周边、局部、二创、示意图）；1-3=几乎无关；0=完全无关。\n"
    "identity（身份判定）：图中主体是否就是该实体本身（true/false）。\n"
    "  与 kb_match 独立：周边、二创、示意图等可 kb_match 中高分但 identity=false；\n"
    "  只有主体即该实体（真人/实物/角色本体/官方形象）才给 true。\n"
    "focus（主体显著度）：实体在画面中的主体地位，只看构图不看语义匹配。\n"
    "  9-10=实体独占画面或为绝对视觉主体（官方立绘、角色特写、单体清晰影像）；\n"
    "  7-8=实体是明确主角，但有少量陪衬元素；\n"
    "  4-6=实体是多个主体之一（多角色合影、群像中较突出、与人物互动的活动照）；\n"
    "  1-3=实体仅为背景、点缀或客串（活动现场摆设、周边陈列、人群中模糊可见）；\n"
    "  0=画面中几乎看不到该实体。\n"
    "richness（信息丰富度）：与实体无关，只看图片本身的视觉信息量。\n"
    "  9-10=主体突出且细节丰富，构图完整有场景/语境，风格表现力强"
    "（精细插画、官方海报、高质量场景图）；\n"
    "  7-8=主体清晰、细节较多，有一定场景或设计元素；\n"
    "  5-6=主体可辨但画面简单（素色背景、单一元素、常规截图）；\n"
    "  3-4=信息偏少（严重裁剪、大面积留白、轮廓模糊、图标式简化）；\n"
    "  0-2=几乎无信息（纯色、极简线条、接近空白、画质严重退化）。\n"
    "caption（详细描述）：80-200字中文，客观描述画面：主体及其外观特征、姿态或动作、"
    "场景与背景、风格与媒介（插画/照片/截图/周边实物等）。不要复述实体知识，"
    "不要写评价性套话。"
)

USER_PROMPT_TPL = "以下是该图应描绘的实体信息：\n{blocks}\n请标注这张图。"

# 从模型回复中提取 JSON 对象（容忍 thinking 前缀/```json 包裹）
_JSON_RE = re.compile(r"\{[^{}]*\"kb_match\"[^{}]*\}", re.S)


def load_instance_kb(path) -> dict:
    """instances.json → {name: {"desc":..., "aliases":[...]}}（只读查表）。"""
    doc = json.loads(open(path, encoding="utf-8").read())
    kb: dict[str, dict] = {}
    for it in doc.get("instances", []):
        name = it.get("name", "")
        if not name:
            continue
        kb[name] = {
            "desc": (it.get("desc") or "").strip(),
            "aliases": [str(a).strip() for a in (it.get("aliases") or [])
                        if str(a).strip()],
        }
    return kb


def build_block(instance: str, kb: dict) -> str:
    """单实例知识块（V2 一条 Item 只对应一个种子实例）。"""
    rec = kb.get(instance) or {"desc": "", "aliases": []}
    lines = [f"实体：{instance}"]
    if rec["aliases"]:
        lines.append("别名：" + "、".join(rec["aliases"][:5]))
    desc = rec["desc"]
    if desc:
        lines.append("知识：" + (desc[:DESC_CHARS] + ("…" if len(desc) > DESC_CHARS else "")))
    else:
        lines.append("知识：（暂无，仅凭实体名称判断）")
    return "\n".join(lines)


def encode_for_vlm(data: bytes, max_edge: int = MAX_EDGE) -> Optional[str]:
    """原始字节 → 缩最长边 → JPEG base64（只影响模型输入，不动原始字节）。"""
    try:
        with Image.open(io.BytesIO(data)) as im:
            im = im.convert("RGB")
            w, h = im.size
            scale = max_edge / max(w, h)
            if scale < 1.0:
                im = im.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
            buf = io.BytesIO()
            im.save(buf, "JPEG", quality=JPEG_QUALITY)
            return base64.b64encode(buf.getvalue()).decode()
    except Exception:  # noqa: BLE001 - 编码失败按无标注放行
        return None


def parse_annotation(text: str) -> Optional[dict]:
    """解析 VLM 回复；不合规返回 None（触发重试，重试耗尽则无标注放行）。"""
    m = _JSON_RE.search(text)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None

    def clamp(v) -> Optional[int]:
        try:
            return max(0, min(10, int(v)))
        except (TypeError, ValueError):
            return None

    km, ri = clamp(d.get("kb_match")), clamp(d.get("richness"))
    fo = clamp(d.get("focus"))
    cap = str(d.get("caption") or "").strip()
    ident = d.get("identity")
    if km is None or ri is None or fo is None or len(cap) < CAPTION_MIN or \
            not isinstance(ident, bool):
        return None
    return {"kb_match": km, "richness": ri, "caption": cap,
            "identity": ident, "focus": fo}


async def _call_vlm(client: httpx.AsyncClient, b64: str, blocks: str, *,
                    endpoint: str, model: str) -> Optional[dict]:
    """调 VLM，解析成功返回标注 dict；网络/解析失败重试耗尽返回 None。"""
    payload = {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "temperature": 0.0,
        # Qwen3.8 默认开 thinking，会把 token 预算耗在推理链上；批量打标关掉
        "chat_template_kwargs": {"enable_thinking": False},
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "image_url",
                 "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                {"type": "text", "text": USER_PROMPT_TPL.format(blocks=blocks)},
            ]},
        ],
    }
    for attempt in range(RETRIES):
        try:
            r = await client.post(endpoint, json=payload, timeout=VLM_TIMEOUT)
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
            ann = parse_annotation(content)
            if ann is not None:
                return ann
        except Exception:  # noqa: BLE001 - 网络/服务端错误统一固定间隔重试
            pass
        if attempt < RETRIES - 1:
            await asyncio.sleep(RETRY_INTERVAL)
    return None


async def annotate(
    item: Item,
    kb: dict,
    *,
    client: Optional[httpx.AsyncClient] = None,
    endpoint: str = DEFAULT_ENDPOINT,
    model: str = DEFAULT_MODEL,
) -> Item:
    """对单条已下载 Item 打标，在同一 Item 上追加标注字段并返回。

    VLM 失败（或字节编码失败）→ 标注字段留 None 原样放行（用户拍板不弃图）。
    """
    if item.data is None:
        return item     # 未下载的 Item 不打标，原样流转
    # PIL 解码/缩放是同步阻塞的，丢线程池避免卡死事件循环
    b64 = await asyncio.to_thread(encode_for_vlm, item.data)
    if b64 is None:
        return item
    http = client or httpx.AsyncClient()
    own_client = client is None
    try:
        ann = await _call_vlm(http, b64, build_block(item.instance, kb),
                              endpoint=endpoint, model=model)
    finally:
        if own_client:
            await http.aclose()
    if ann is not None:
        item.kb_match = ann["kb_match"]
        item.richness = ann["richness"]
        item.caption = ann["caption"]
        item.identity = ann["identity"]
        item.focus = ann["focus"]
        w_kb, w_fo, w_ri = QUALITY_WEIGHTS
        item.quality = round(w_kb * ann["kb_match"] + w_fo * ann["focus"]
                             + w_ri * ann["richness"], 1)
    return item
