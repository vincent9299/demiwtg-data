"""kb 线 Wikipedia dump 解析算子：dump 页行 → 页面记录行（2026-09-08）。

链路定位（kb 线第一阶段）：zhwiki/enwiki multistream dump → 页面结构化
记录（正文分节/Redirect/分类/内链/页面图片/QID 线索），落 kb/pages-{lang}
清单——Concept 的 Wikipedia 正文来源；身份（QID 权威值）与关系由
Phase 2 wikidata.py 补充，此处 QID 只收 dump 内线索（interwiki d:/模板），
缺失留 None 由 API 兜底。

行契约（demiflow 原生 dict 行）：
- 源头行（iter_dump_pages 产出，from_iter 惰性喂入）：
  {lang, title, ns, page_id, revision_id, is_redirect, redirect_target, text}
- 页面记录行（WikiParseStage 产出，PagesSinkStage 落盘）：
  {lang, title, page_id, revision_id, is_redirect, redirect_target,
   text_sha256, byte_len, is_disambig, qid, sections, categories,
   links, link_count, images, parser_version}
  sections=[{title, level, text}]（导语节 title=""；text 为原始 wikitext，
  清洗是离线再解析口径——随 parser_version 可重放）

机制与策略分工：
- 机制在 demiflow：from_iter 惰性源头（dump 全量不物化）、run_stream
  有界队列背压（内存上界=队列深度×页行载荷）、AppendManifestStore
  幂等追加（(lang,title) 去重键，重跑不产生重复数据）；
- 策略在本文件：dump XML 流式抽取（iterparse + elem 清扫，常数内存）、
  wikitext 结构解析（分节/内链分桶/前缀表）、页记录字段契约。

multistream 说明：multistream .xml.bz2 是多个独立 bz2 流拼接，
bz2.BZ2File 透明跨流顺序读——单机顺序全量解析不依赖 index（index 按
块随机读是后续分布式分片的备选路径）。
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from typing import Iterator, Optional
from xml.etree import ElementTree as ET

from demiflow.collect.store import AppendManifestStore
from demiflow.data.plan import StreamStage

PARSER_VERSION = "wiki_dump/2026-09-08"

KEEP_NS = (0,)           # 只收主名字空间条目（Talk/Template 等不进正文源）

LINK_CAP = 2000          # 内链截取封顶（枢纽页防行膨胀；真计数另记 link_count）
LINK_RE = re.compile(r"\[\[([^\[\]|]+)(?:\|[^\[\]]*)?\]\]")
HEADING_RE = re.compile(r"^(={1,6})\s*(.+?)\s*\1\s*$", re.M)

# 分类/文件/消歧义前缀表（zh 家族 + en，dump 实测口径；新语种在此扩表）
CATEGORY_PREFIXES = ("Category:", "category:", "CAT:", "分类:", "分類:")
FILE_PREFIXES = ("File:", "file:", "Image:", "image:", "IMAGE:",
                 "文件:", "檔案:")
DISAMBIG_TMPL = ("{{disambig", "{{消歧义", "{{消歧義")      # 模板态：正文前 4KB 小写匹配
DISAMBIG_TITLE = ("（消歧义）", "（消歧義）", "(消歧义)")     # 标题态：页首 200 字符匹配

# QID 线索形态：interwiki 链接 [[d:Q42]] / [[wikidata:Q42]]、模板 {{Wikidata|Q42}}
_QID_LINK_RE = re.compile(r"\[\[(?:d|wikidata):\s*(Q\d+)", re.I)
_QID_TMPL_RE = re.compile(r"\{\{[Ww]ikidata\|\s*(Q\d+)")

# interwiki 前缀：不进普通内链桶（d: 前缀是 QID 线索源，其余跨站链
# 不是本站概念图的边）
INTERWIKI_PREFIXES = ("d:", "wikidata:", "w:", "en:", "zh:", "commons:",
                      "meta:", "m:", "s:", "q:", "b:", "v:", "c:")

# 节标题剥链接显示文本（[[a|b]]→b、[[a]]→a）：标题是结构字段非 wikitext
_LINK_IN_TITLE = re.compile(r"\[\[(?:[^\[\]|]+\|)?([^\[\]|]+)\]\]")


def _localname(tag: str) -> str:
    """'{ns}page' → 'page'（dump xmlns 各版本号不同，按局部名匹配）。"""
    return tag.rsplit("}", 1)[-1]


def iter_dump_pages(dump_path: str, *, lang: str,
                    keep_ns: tuple = KEEP_NS) -> Iterator[dict]:
    """dump 文件 → 源头页行生成器（常数内存；from_iter 的 factory 体）。

    每页产出最小原始行，wikitext 解析留给下游算子（线程池并发）；
    ElementTree end 事件按页消费，elem/root 双清扫防树膨胀。
    """
    import bz2
    with bz2.open(dump_path, "rb") as f:
        context = ET.iterparse(f, events=("start", "end"))
        _, root = next(context)          # 首事件：根元素 <mediawiki>
        for event, elem in context:
            if event != "end" or _localname(elem.tag) != "page":
                continue
            try:
                # dump xmlns 使子元素全带命名空间，find 系用 {*} 通配
                # （按局部名匹配，xmlns 版本号无关）
                ns = int(elem.findtext("{*}ns") or "-1")
                if ns in keep_ns:
                    red = elem.find("{*}redirect")
                    rev = elem.find("{*}revision")
                    yield {
                        "lang": lang,
                        "title": elem.findtext("{*}title") or "",
                        "ns": ns,
                        "page_id": int(elem.findtext("{*}id") or 0),
                        "revision_id": int(rev.findtext("{*}id") or 0)
                        if rev is not None else 0,
                        "is_redirect": red is not None,
                        "redirect_target": red.get("title")
                        if red is not None else None,
                        "text": rev.findtext("{*}text")
                        if rev is not None else "",
                    }
            finally:
                elem.clear()             # 已消费页出树；root 清扫防兄弟累积
                root.clear()


# ---------------------------------------------------------------------------
# wikitext 结构解析（纯函数：正文 → 结构化字段）
# ---------------------------------------------------------------------------

def parse_wikitext(text: str) -> dict:
    """wikitext → {sections, categories, links, link_count, images, qid,
    is_disambig}。分节保原始 wikitext（清洗口径离线随版本重放）。"""
    low = text[:4000].lower()
    is_disambig = (any(m in low for m in DISAMBIG_TMPL)
                   or any(m in text[:200] for m in DISAMBIG_TITLE))

    # 分节：首个标题前为导语节（title=""，level 约定 2 与正文节对齐）
    sections: list[dict] = []
    heads = [(m.start(), m.end(), len(m.group(1)), m.group(2))
             for m in HEADING_RE.finditer(text)]
    if not heads:
        sections.append({"title": "", "level": 2, "text": text})
    else:
        if heads[0][0] > 0:
            sections.append({"title": "", "level": 2,
                             "text": text[:heads[0][0]]})
        for i, (s, e, level, title) in enumerate(heads):
            body_end = heads[i + 1][0] if i + 1 < len(heads) else len(text)
            sections.append({"title": _LINK_IN_TITLE.sub(r"\1", title),
                             "level": level, "text": text[e:body_end]})

    # 内链一遍扫：按目标前缀分桶（分类/文件/普通链），去重保序
    categories: list[str] = []
    images: list[str] = []
    links: list[str] = []
    seen: dict[str, str] = {}            # lower(目标) → 原case目标
    for m in LINK_RE.finditer(text):
        target = m.group(1).strip()
        if not target:
            continue
        if target.startswith(CATEGORY_PREFIXES):
            cat = target.split(":", 1)[1].strip()
            if cat and cat not in categories:
                categories.append(cat)
        elif target.startswith(FILE_PREFIXES):
            fname = target.split(":", 1)[1].strip()
            if fname and fname not in images:
                images.append(fname)
        else:
            if target.startswith(INTERWIKI_PREFIXES):
                continue                      # 跨站链不进本站概念边
            target = target.split("#", 1)[0].strip()   # 去锚点，与归一并径
            key = target.lower()
            if target and key not in seen:
                seen[key] = target
    links = list(seen.values())

    qid: Optional[str] = None
    qm = _QID_LINK_RE.search(text) or _QID_TMPL_RE.search(text)
    if qm:
        qid = qm.group(1)

    return {"sections": sections, "categories": categories,
            "links": links[:LINK_CAP], "link_count": len(links),
            "images": images, "qid": qid, "is_disambig": is_disambig}


class WikiParseStage(StreamStage):
    """wikitext 解析算子：源头页行 → 页面记录行。

    CPU 密集（正则+分节），丢线程池避免卡事件循环；redirect 页无正文
    解析直通（身份由 redirect_target 承载）。catch=()：解析异常是真
    bug，终止整链暴露，不认缺。
    """

    label = "wikiparse"
    concurrency = 4
    queue_depth = 8

    async def __call__(self, row: dict):
        text = row.get("text") or ""
        rec = {
            "lang": row["lang"],
            "title": row["title"],
            "page_id": row["page_id"],
            "revision_id": row["revision_id"],
            "is_redirect": row["is_redirect"],
            "redirect_target": row["redirect_target"],
            "text_sha256": hashlib.sha256(
                text.encode("utf-8", errors="replace")).hexdigest(),
            "byte_len": len(text.encode("utf-8", errors="replace")),
            "parser_version": PARSER_VERSION,
        }
        if row["is_redirect"]:
            rec.update({"is_disambig": False, "qid": None, "sections": [],
                        "categories": [], "links": [], "link_count": 0,
                        "images": []})
        else:
            rec.update(await asyncio.to_thread(parse_wikitext, text))
        return rec


# ---------------------------------------------------------------------------
# 清单契约（页记录落盘：AppendManifestStore 机制 + 本线字段面）
# ---------------------------------------------------------------------------

PAGE_RECORD_FIELDS = (
    "lang", "title", "page_id", "revision_id", "is_redirect",
    "redirect_target", "text_sha256", "byte_len", "is_disambig", "qid",
    "sections", "categories", "links", "link_count", "images",
    "parser_version",
)


def _page_keys(rec: dict) -> list:
    """清单行 → 去重键集（引擎 store 的 key_of 注入）。"""
    return [(rec.get("lang"), rec.get("title"))]


class PagesSinkStage(StreamStage):
    """页记录落盘算子：kb/pages[-shard].jsonl 幂等追加。

    机制在 AppendManifestStore（fcntl 跨进程 + 吸收式尾扫）；本类只持
    布局（kb/ 目录、按 lang/分片命名）与字段面。本线无 blob（页记录
    即数据本体），store.write 的 blob 路径传清单自身占位——首写建空
    文件、已存在即跳过，无副作用。重跑续传省的是写不是解析（解析幂等
    无副作用；要省解析把 contains 挪上游是后续优化位）。
    """

    label = "kb_sink"
    concurrency = 1          # 单写者：清单顺序即 dump 顺序，锁内零竞争
    queue_depth = 4

    def __init__(self, dataset_dir: str, lang: str,
                 manifest_name: Optional[str] = None):
        import os
        self.dataset_dir = dataset_dir
        self.manifest_name = manifest_name or f"pages-{lang}.jsonl"
        self.manifest = os.path.join(dataset_dir, "kb", self.manifest_name)
        os.makedirs(os.path.dirname(self.manifest), exist_ok=True)
        self._store = AppendManifestStore(
            manifest=self.manifest,
            lock_path=os.path.join(os.path.dirname(self.manifest),
                                   f".{self.manifest_name}.lock"),
        )
        self._store.load_index(_page_keys)
        self.sunk = 0         # 业务计数自持（编排只读打印）

    async def __call__(self, rec: dict):
        if not rec.get("title"):
            return None
        written = await self._store.write(
            data=b"",
            blob_path=self.manifest,   # 无 blob：占位路径（见类注释）
            key=(rec["lang"], rec["title"]),
            record={k: rec.get(k) for k in PAGE_RECORD_FIELDS},
        )
        if written:
            self.sunk += 1
            return rec
        return None


# ---------------------------------------------------------------------------
# 分片清单合并（多机部署的离线汇合点，annotate.merge_manifests 同款口径）
# ---------------------------------------------------------------------------

def merge_page_shards(dataset_dir: str, lang: str, *,
                      output: str = "pages-{lang}.jsonl",
                      dry_run: bool = False) -> dict:
    """合并 kb/pages-{lang}-shard-*-of-*.jsonl → kb/pages-{lang}.jsonl。

    去重键 (lang,title) 先到先得；坏行容忍（与读端口径一致）；pid 唯一
    临时文件 + os.replace 原子发布。返回 {shards, input_rows, output_rows,
    dup_dropped}。分片文件不动（保留溯源，重合并幂等）。
    """
    import glob
    import json as _json
    import os as _os
    kb = _os.path.join(dataset_dir, "kb")
    shard_files = sorted(glob.glob(
        _os.path.join(kb, f"pages-{lang}-shard-*-of-*.jsonl")))
    seen: set = set()
    out_lines: list[str] = []
    total = 0
    for sf in shard_files:
        with open(sf, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = _json.loads(line)
                except _json.JSONDecodeError:
                    continue
                total += 1
                key = (rec.get("lang"), rec.get("title"))
                if key in seen:
                    continue
                seen.add(key)
                out_lines.append(line)
    if not dry_run and out_lines:
        tmp = _os.path.join(kb, f".{output.format(lang=lang)}.merge.tmp."
                          f"{_os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines) + "\n")
        _os.replace(tmp, _os.path.join(kb, output.format(lang=lang)))
    return {"shards": len(shard_files), "input_rows": total,
            "output_rows": len(out_lines), "dup_dropped": total - len(out_lines)}
