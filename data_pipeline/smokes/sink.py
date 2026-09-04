"""collect_v2/annotate.py ManifestSink 最小冒烟（原 smoke_sink 迁移）：
幂等去重、并发无丢行、跨进程 fcntl、索引重建。
运行：python3 -m data_pipeline.smoke_sink
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing
import os
import shutil
import tempfile


def png_bytes(w: int, h: int, rgb) -> bytes:
    import io
    from PIL import Image
    im = Image.new("RGB", (w, h), rgb)
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()


def downloaded_row(name: str, data: bytes, **kw) -> dict:
    row = {"name": name, "query": name, "lang": "zh", "source": "baidu",
           "tiers": ["https://x/a.png"], "data": data,
           "sha256": hashlib.sha256(data).hexdigest(), "ext": "png",
           "mime": "image/png", "content_url": "https://x/a.png",
           "size_bytes": len(data), "width": w_of(data), "height": h_of(data),
           "actual_width": w_of(data), "actual_height": h_of(data),
           "kb_match": 8, "richness": 7,
           "identity": True, "focus": 8, "caption": "描述", "quality": 7.8}
    row.update(kw)
    return row


def w_of(d):
    from PIL import Image
    import io
    return Image.open(io.BytesIO(d)).width


def h_of(d):
    from PIL import Image
    import io
    return Image.open(io.BytesIO(d)).height


def read_manifest(sink) -> list:
    return [json.loads(l) for l in open(sink.manifest, encoding="utf-8")]


def _mp_worker(dataset_dir: str, specs: list) -> None:
    async def run():
        from data_pipeline.operators import annotate
        sink = annotate.ManifestSink(dataset_dir)
        sink.load_index()
        for name, data in specs:
            await sink.sink(downloaded_row(name, data))
    asyncio.run(run())


async def main() -> None:
    from data_pipeline.operators import annotate
    tmp = tempfile.mkdtemp(prefix="smoke_sink_")
    try:
        ds = os.path.join(tmp, "demiwtg")
        sink = annotate.ManifestSink(ds)
        assert sink.load_index() == 0
        good = png_bytes(60, 40, (9, 9, 9))

        # 1) 正常落盘：行数/字段/内容寻址
        row = downloaded_row("慕田峪长城", good)
        assert await sink.sink(row)
        lines = read_manifest(sink)
        assert len(lines) == 1
        rec = lines[0]
        assert rec["instances"] == ["慕田峪长城"]
        assert rec["queries"] == {"慕田峪长城": "慕田峪长城"}
        assert rec["kb_match"] == 8 and rec["identity"] is True
        assert rec["width"] == 60 and rec["orig_width"] == 60
        blob = os.path.join(ds, rec["path"])
        assert os.path.getsize(blob) == rec["size_bytes"]
        assert rec["path"] == f"blobs/{row['sha256'][:2]}/{row['sha256']}.png"
        print("[PASS] 落盘行/字段/内容寻址")

        # 2) 同 sha 异实例追加；同键幂等跳过
        assert await sink.sink(downloaded_row("八达岭长城", good))
        assert not await sink.sink(downloaded_row("八达岭长城", good))
        assert len(read_manifest(sink)) == 2
        print("[PASS] 跨实例追加/同键幂等")

        # 3) 并发无丢行
        batch = [downloaded_row(f"B{i}", png_bytes(30, 30, (i, 0, 0)))
                 for i in range(20)]
        assert all(await asyncio.gather(*(sink.sink(dict(b)) for b in batch)))
        assert len(read_manifest(sink)) == 22
        print("[PASS] 并发 sink 无丢行")

        # 4) 索引重建（续跑）
        fresh = annotate.ManifestSink(ds)
        assert fresh.load_index() == 22
        assert not await fresh.sink(dict(batch[0]))       # 续跑不重复落
        print("[PASS] 索引重建续跑")

        # 5) 跨进程并发：共享键只落一行、无坏行
        shared = png_bytes(30, 30, (1, 2, 3))
        specs_a = [(f"C{i}", png_bytes(30 + i, 30, (i, 0, 0))) for i in range(15)]
        specs_a.append(("共享图", shared))
        specs_b = [(f"D{i}", png_bytes(30, 30 + i, (0, i, 0))) for i in range(15)]
        specs_b.append(("共享图", shared))
        ps = [multiprocessing.Process(target=_mp_worker, args=(ds, s))
              for s in (specs_a, specs_b)]
        for p in ps:
            p.start()
        for p in ps:
            p.join()
        assert all(p.exitcode == 0 for p in ps)
        lines = read_manifest(sink)
        # 32 投入 - 1 次同实例撞车（共享图）；此前 22 行
        assert len(lines) == 22 + 31, len(lines)
        pairs = {(r["sha256"], tuple(r["instances"])) for r in lines}
        assert len(pairs) == len(lines)
        assert sum(1 for r in lines
                   if r["sha256"] == hashlib.sha256(shared).hexdigest()) == 1
        print("[PASS] 跨进程并发（fcntl 锁内去重，无坏行无重复行）")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
