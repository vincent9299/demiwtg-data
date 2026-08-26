"""collect_v2/op_sink.py 最小冒烟：临时数据湖验证落盘、撞车跳过与并发安全。

运行：python3 -m collect_v2.smoke_sink
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import multiprocessing
import os
import shutil
import tempfile

from PIL import Image

from collect_v2 import op_search, op_sink


def png_bytes(w: int, h: int, color=(200, 30, 30)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (w, h), color).save(buf, "PNG")
    return buf.getvalue()


def downloaded_item(name: str, data: bytes, *, query=None,
                    declared=(88, 66)) -> op_search.Item:
    """模拟 download+annotate 之后的 Item。"""
    it = op_search.Item(instance=name, query=query or name,
                        source="wikimedia_zh", rank=0,
                        content_urls=["https://img.example/a.png"],
                        content_url="https://img.example/a.png",   # 获胜候选（download 记回）
                        landing_url="https://commons.example/page",
                        declared_width=declared[0], declared_height=declared[1],
                        mime="image/png", license="CC BY-SA 4.0", author="tester")
    it.data = data
    it.sha256 = hashlib.sha256(data).hexdigest()
    it.ext = "png"
    it.actual_width, it.actual_height = Image.open(io.BytesIO(data)).size
    it.size_bytes = len(data)
    return it


def read_manifest(path: str) -> list:
    """容忍坏行（与算子读端一致；测试 6 会故意追加一行坏行）。"""
    recs = []
    with open(path, encoding="utf-8") as f:
        for l in f:
            l = l.strip()
            if not l:
                continue
            try:
                recs.append(json.loads(l))
            except json.JSONDecodeError:
                continue
    return recs


def _mp_worker(dataset_dir: str, specs: list) -> None:
    """子进程 worker：各自持一份内存索引，验跨进程去重靠锁内尾部吸收。"""
    sink = op_sink.Sink(dataset_dir)
    sink.load_index()

    async def run():
        for name, data in specs:
            it = op_search.Item(instance=name, query=name, source="baidu")
            it.data = data
            it.sha256 = hashlib.sha256(data).hexdigest()
            it.ext = "png"
            it.actual_width, it.actual_height = 30, 30
            it.size_bytes = len(data)
            await sink.sink(it)
    asyncio.run(run())


async def main() -> None:
    tmp = tempfile.mkdtemp(prefix="sink_smoke_")
    try:
        sink = op_sink.Sink(os.path.join(tmp, "dataset"))
        # 默认目标清单 metadata.jsonl（v2 专属，2026-08-20 拍板；legacy 不碰）
        assert os.path.basename(sink.manifest) == "metadata.jsonl"
        good = png_bytes(320, 240)
        item = downloaded_item("慕田峪长城", good, query="慕田峪长城 照片")

        # 1) 正常落盘：blob 内容寻址 + 清单行字段为最小兼容集
        assert sink.load_index() == 0
        assert await sink.sink(item) is True
        blob = os.path.join(sink.dataset_dir, item.local_path)
        assert item.local_path == f"blobs/{item.sha256[:2]}/{item.sha256}.png"
        assert open(blob, "rb").read() == good
        recs = read_manifest(sink.manifest)
        assert len(recs) == 1
        rec = recs[0]
        assert rec["sha256"] == item.sha256 and rec["path"] == item.local_path
        assert (rec["width"], rec["height"]) == (320, 240)          # 实测值
        assert (rec["orig_width"], rec["orig_height"]) == (88, 66)  # 声明值
        assert rec["instances"] == ["慕田峪长城"]
        assert rec["queries"] == {"慕田峪长城": "慕田峪长城 照片"}    # 真实检索词透传
        assert rec["query_langs"] == {"慕田峪长城": "zh"}              # lang 随 Item 透传
        assert isinstance(rec["fetched_at"], float)
        # 最小兼容集：V2 无信息源的旧概念字段不写（query_langs 已随 lang 补写）
        for gone in ("tiers", "source_rank", "source_score", "source_kind",
                     "source_authorized", "credit"):
            assert gone not in rec
        assert item.fetched_at is not None
        print("[PASS] 正常落盘（blob 内容寻址 + 最小兼容字段集）")

        # 2) sha 撞车 → 直接跳过：不追加行、不合并、不重写 blob
        other = downloaded_item("八达岭长城", good)   # 同字节同 sha，不同实例
        assert await sink.sink(other) is False
        assert len(read_manifest(sink.manifest)) == 1
        assert other.local_path is None
        print("[PASS] sha 撞车直接跳过")

        # 3) 无标注落盘：四字段键存在值为 null
        plain = downloaded_item("无标注实例", png_bytes(100, 90, (9, 9, 9)))
        assert await sink.sink(plain) is True
        rec = read_manifest(sink.manifest)[-1]
        for fld in ("kb_match", "richness", "caption", "identity"):
            assert fld in rec and rec[fld] is None
        print("[PASS] 无标注记录写 null")

        # 4) 有标注记录原样落盘
        scored = downloaded_item("打分实例", png_bytes(100, 90, (9, 90, 9)))
        scored.kb_match, scored.richness, scored.identity = 9, 7, True
        scored.caption = "一段足够长的中文描述，用来表示 caption 正常落盘。"
        assert await sink.sink(scored) is True
        rec = read_manifest(sink.manifest)[-1]
        assert (rec["kb_match"], rec["richness"], rec["identity"]) == (9, 7, True)
        assert rec["caption"] == scored.caption
        print("[PASS] 标注记录落盘")

        # 5) 并发 sink（进程内多 worker）：无丢行无坏行
        batch = [downloaded_item(f"并发实例{i}", png_bytes(50 + i, 40, (i, i, i)))
                 for i in range(20)]
        ok = await asyncio.gather(*(sink.sink(it) for it in batch))
        assert all(ok) and len(read_manifest(sink.manifest)) == 23
        print("[PASS] 并发 sink 无丢行")

        # 6) 索引重建：能读回已落 sha；坏行容忍
        with open(sink.manifest, "a", encoding="utf-8") as f:
            f.write('{"broken json\n')
        fresh = op_sink.Sink(sink.dataset_dir)
        assert fresh.load_index() == 23
        assert await fresh.sink(item) is False   # 断点续传不重复落
        print("[PASS] 索引重建与坏行容忍")

        # 7) 防御性跳过：无字节/无 sha
        empty = op_search.Item(instance="空", query="空")
        assert await sink.sink(empty) is False
        print("[PASS] 无字节防御性跳过")

        # 7.5) 清单名参数化：可切 legacy 名（仅测试/迁移用，不做双写）
        legacy = op_sink.Sink(sink.dataset_dir, manifest_name="images.jsonl")
        assert os.path.basename(legacy.manifest) == "images.jsonl"
        assert legacy.load_index() == 0   # 与 metadata.jsonl 互不可见
        print("[PASS] 清单名参数化")

        # 8) 跨进程并发（多 worker 拍板要求）：撞车只落一行
        # 构造两次撞车：共享图两进程都写；A0 与 B0 字节相同（30x30 纯黑）
        shared = png_bytes(30, 30, (1, 2, 3))
        specs_a = [(f"A{i}", png_bytes(30 + i, 30, (i, 0, 0))) for i in range(15)]
        specs_a.append(("共享图", shared))
        specs_b = [(f"B{i}", png_bytes(30, 30 + i, (0, i, 0))) for i in range(15)]
        specs_b.append(("共享图", shared))
        assert specs_a[0][1] == specs_b[0][1]   # A0/B0 同字节，预置撞车
        ps = [multiprocessing.Process(target=_mp_worker, args=(sink.dataset_dir, s))
              for s in (specs_a, specs_b)]
        for p in ps:
            p.start()
        for p in ps:
            p.join()
        assert all(p.exitcode == 0 for p in ps)
        lines = read_manifest(sink.manifest)
        # 新增 = 16+16 条投入 - 2 次撞车（共享图、A0/B0）= 30；此前 23 行
        assert len(lines) == 23 + 30, f"跨进程撞车未去重：{len(lines)} 行"
        assert len({r["sha256"] for r in lines}) == len(lines)   # 无重复 sha 行
        for r in lines:
            p = os.path.join(sink.dataset_dir, r["path"])
            assert os.path.getsize(p) == r["size_bytes"]
        print("[PASS] 跨进程并发（fcntl 锁内复查去重，无坏行无重复行）")

        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
