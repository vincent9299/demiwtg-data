"""collect_v2 标注算子：图像行 → 追加标注键（dict 行，自包含）。

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
- 预处理（缩最长边 + JPEG base64）只影响模型输入，不动行内 data 原始字节。
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import os
import re
from typing import Optional

import httpx
from PIL import Image

from demiflow.collect.llm import (AsyncLLMClient, get_llm_client,
                                  register_endpoint)
from demiflow.collect.store import AppendManifestStore
from demiflow.data.plan import StreamStage

# ---------------------------------------------------------------------------
# LLM 端点资源声明（demiflow 平台注册表：算子只给配置，客户端/连接池/
# 生命周期归平台；跨机器部署走 env 覆盖零代码改动）
# ---------------------------------------------------------------------------
register_endpoint(
    "demiwtg_vlm",
    base_url="http://localhost:8000/v1/chat/completions",
    model="qwen3.8-27b",
    max_connections=56,          # 池上限给足（夜跑 CLOSE-WAIT 教训），编排可 reconfigure
    timeout=600.0,
    base_url_env="DEMIFLOW_VLM_BASE_URL",
    model_env="DEMIFLOW_VLM_MODEL",
)

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
    """单实例知识块（一条行只对应一个种子实例）。"""
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


async def _call_vlm(client: AsyncLLMClient, b64: str,
                    blocks: str) -> Optional[dict]:
    """调 VLM，解析成功返回标注 dict；网络/解析失败重试耗尽返回 None。

    机制（HTTP/参数构造）在 demiflow 平台端点资源（get_llm_client）
    （单次尝试失败上抛）；本函数持有口径：prompt、json_mode+关 thinking、
    解析重试循环（网络失败与解析失败统一固定间隔重试，是数据可比性契约）。"""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": [
            {"type": "image_url",
             "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
            {"type": "text", "text": USER_PROMPT_TPL.format(blocks=blocks)},
        ]},
    ]
    for attempt in range(RETRIES):
        try:
            content = await client.chat(
                messages, max_tokens=MAX_TOKENS, temperature=0.0,
                json_mode=True, thinking=False, timeout=VLM_TIMEOUT)
            ann = parse_annotation(content)
            if ann is not None:
                return ann
        except Exception:  # noqa: BLE001 - 网络/服务端错误统一固定间隔重试
            pass
        if attempt < RETRIES - 1:
            await asyncio.sleep(RETRY_INTERVAL)
    return None


def _read_blob(dataset_dir: str, rel: str):
    try:
        with open(os.path.join(dataset_dir, rel), "rb") as f:
            return f.read()
    except OSError:
        return None


async def annotate(row: dict, kb: dict, *, dataset_dir: str = "") -> dict:
    """对单条已下载图像行打标，就地追加标注键并返回。

    字节按行内 blob_path 引用从数据集读取（D1 引用化：行不携 data）；
    未下载（无 blob_path）/读失败/编码失败的行原样流转（缺列=未打标）；
    打标失败重试耗尽也原样流转（null=打过失败，两者由读端区分）。
    """
    rel = row.get("blob_path")
    if not rel:
        return row
    data = await asyncio.to_thread(_read_blob, dataset_dir, rel)
    if data is None:
        return row
    # PIL 解码/缩放是同步阻塞的，丢线程池避免卡死事件循环
    b64 = await asyncio.to_thread(encode_for_vlm, data)
    if b64 is None:
        return row
    ann = await _call_vlm(get_llm_client("demiwtg_vlm"), b64,
                          build_block(row["name"], kb))
    if ann is not None:
        w_kb, w_fo, w_ri = QUALITY_WEIGHTS
        row.update(ann)
        row["quality"] = round(w_kb * ann["kb_match"] + w_fo * ann["focus"]
                               + w_ri * ann["richness"], 1)
    return row


# ---------------------------------------------------------------------------
# 清单契约（原 op_sink 迁入：字段最小兼容面 + 去重键 + blob 布局）
# ---------------------------------------------------------------------------

import time as _time

# metadata.jsonl 字段集（最小兼容面，对齐读端口径；有新消费者再加）
RECORD_FIELDS = (
    "sha256", "ext", "source", "license", "author",
    "width", "height", "orig_width", "orig_height",
    "size_bytes", "mime", "instances", "queries", "query_langs",
    "content_url", "landing_url", "fetched_at", "path",
    "kb_match", "richness", "caption", "identity", "focus", "quality",
)


def _row_keys(rec: dict) -> list:
    """清单行 → 去重键集（引擎 store 的 key_of 注入；存量兼容空实例名）。"""
    return [(rec.get("sha256"), inst) for inst in rec.get("instances") or [""]]


def _record_for(row: dict) -> dict:
    """图像行 → 最小兼容字段集的清单行（字段映射是 demiwtg 契约）。"""
    rec = {
        "sha256": row["sha256"],
        "ext": row["ext"],
        "source": row["source"],
        "license": row.get("license"),
        "author": row.get("author"),
        # 实测尺寸（下载解码）优先，缺则声明尺寸
        "width": row.get("actual_width") or row.get("width"),
        "height": row.get("actual_height") or row.get("height"),
        "orig_width": row.get("width"),
        "orig_height": row.get("height"),
        "size_bytes": row.get("size_bytes"),
        "mime": row.get("mime"),
        "instances": [row["name"]],
        "queries": {row["name"]: row.get("query")},
        "query_langs": {row["name"]: row.get("lang")},
        "content_url": row.get("content_url"),
        "landing_url": row.get("landing"),
        "fetched_at": _time.time(),
        "path": row.get("blob_path"),
        "kb_match": row.get("kb_match"),
        "richness": row.get("richness"),
        "caption": row.get("caption"),
        "identity": row.get("identity"),
        "focus": row.get("focus"),
        "quality": row.get("quality"),
    }
    return {k: rec.get(k) for k in RECORD_FIELDS}


class ManifestSink:
    """数据集落盘器：blobs/<sha前2>/<sha>.<ext> 内容寻址 + 清单追加幂等。

    机制在 demiflow.collect.store.AppendManifestStore（原子写/跨进程幂等/
    吸收式尾扫）；本类只持布局与字段契约。
    """

    def __init__(self, dataset_dir: str, manifest_name: str = "metadata.jsonl"):
        import os
        self.dataset_dir = dataset_dir
        self.manifest = os.path.join(dataset_dir, "meta", manifest_name)
        os.makedirs(os.path.dirname(self.manifest), exist_ok=True)
        os.makedirs(os.path.join(dataset_dir, "blobs"), exist_ok=True)
        self._store = AppendManifestStore(
            manifest=self.manifest,
            lock_path=os.path.join(os.path.dirname(self.manifest),
                                   f".{manifest_name}.lock"),
        )

    def load_index(self) -> int:
        """启动期全量扫清单建 (sha256, instance) 索引（续跑依据）。"""
        return self._store.load_index(_row_keys)

    def contains(self, row: dict) -> bool:
        """无锁快查：该行是否已落盘（撞车前置省 VLM 打标）。"""
        sha = row.get("sha256")
        return bool(sha) and self._store.contains((sha, row.get("name") or ""))

    async def sink(self, row: dict) -> bool:
        """追加清单单行；返回 False = 同 (sha,name) 已存在（幂等跳过）。

        D1 引用化：blob 已由下载算子原子落盘（row.blob_path），本方法
        只负责清单幂等追加——单写者分片文件下连接 fcntl 都是冗余防线。
        """
        rel = row.get("blob_path")
        if not rel or not row.get("sha256"):
            return False
        return await self._store.write(
            data=b"",                       # blob 不重写（引用化）
            blob_path=os.path.join(self.dataset_dir, rel),
            key=(row["sha256"], row.get("name") or ""),
            record=_record_for(row))


class AnnotateSinkStage(StreamStage):
    """标注+落盘算子（合并一级：sink 是毫秒级本地 IO 不值得独立队列级；
    contains 前置快查省 VLM——撞车高发场景 9 成打标开销在此省掉）。

    行契约：读键 name/data/sha256/ext/...；追加标注键（annotate）；
    落盘成功行继续流转（engine emitted=sunk）。catch=()：真异常终止整链。
    queue_depth 缺省=并发（字节上界语义：内存上界 = vlm并发 × 20MB）。
    """
    label = "annotate_sink"
    concurrency = 48

    def __init__(self, sink: ManifestSink, kb: dict):
        self.sink, self.kb = sink, kb
        self.annotated = 0        # 业务计数自持（编排只读打印）

    async def __call__(self, row: dict):
        if self.sink.contains(row):
            return None
        await annotate(row, self.kb, dataset_dir=self.sink.dataset_dir)
        if row.get("kb_match") is not None:
            self.annotated += 1
        if await self.sink.sink(row):
            return row
        return None


# ---------------------------------------------------------------------------
# 分片清单合并（2026-09-04·D1：分布式分片的离线汇合点）
# ---------------------------------------------------------------------------

def merge_manifests(dataset_dir: str, *, pattern: str = "metadata-shard-*.jsonl",
                    output: str = "metadata.jsonl",
                    dry_run: bool = False) -> dict:
    """合并分片清单：行级去重键 (sha256, instances 元组)，先到先得。

    输入：meta/ 下匹配 pattern 的分片文件（每分片单写者，天然无冲突）；
    输出：meta/<output> 全量清单（pid 唯一临时文件 + os.replace 原子替换）。
    blobs 不动（内容寻址天然去重）；坏行容忍（与读端口径一致）；
    dry_run 只统计不落盘。返回 {shards, input_rows, output_rows, dup_dropped}。
    """
    import glob as _glob
    meta = os.path.join(dataset_dir, "meta")
    shard_files = sorted(_glob.glob(os.path.join(meta, pattern)))
    seen: set = set()
    out_lines: list = []
    total = 0
    for sf in shard_files:
        with open(sf, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total += 1
                key = (rec.get("sha256"),
                       tuple(rec.get("instances") or [""]))
                if key in seen:
                    continue
                seen.add(key)
                out_lines.append(line)
    if not dry_run and out_lines:
        os.makedirs(meta, exist_ok=True)
        tmp = os.path.join(meta, f".{output}.merge.tmp.{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines) + "\n")
        os.replace(tmp, os.path.join(meta, output))
    return {"shards": len(shard_files), "input_rows": total,
            "output_rows": len(out_lines), "dup_dropped": total - len(out_lines)}
