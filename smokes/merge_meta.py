"""merge_meta 纯逻辑冒烟（本地 fake 目录，不 ssh）。

覆盖：images 增量的首扫追加/键去重/偏移推进、二轮零追加（幂等）、
尾部残行不消费、轮转归零重扫不重复、断点偏移续传、既有账坏行容忍；
docs 全量重合并的跨文件去重与原子替换。
运行：.venv/bin/python smokes/merge_meta.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import merge_meta  # noqa: E402


def _w(path: str, rows: list, mode: str = "w") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode, encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _row(sha, insts):
    return {"sha256": sha, "instances": insts, "ext": "jpg"}


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="mm_")
    try:
        for attr, val in [("MIRROR_ROOT", f"{tmp}/sync/manifests"),
                          ("META_ROOT", f"{tmp}/meta"),
                          ("DOCS_OUT", f"{tmp}/meta/docs.jsonl"),
                          ("IMAGES_OUT", f"{tmp}/meta/images.jsonl"),
                          ("STATE_PATH", f"{tmp}/sync/merge_state.json"),
                          ("LOCK_PATH", f"{tmp}/sync/.merge_meta.lock")]:
            setattr(merge_meta, attr, val)

        # 既有大账：2 键 + 1 坏行
        _w(f"{tmp}/meta/images.jsonl",
           [_row("a" * 64, ["长城"]), _row("b" * 64, ["故宫"]), None]
           if False else [_row("a" * 64, ["长城"]), _row("b" * 64, ["故宫"])])
        with open(f"{tmp}/meta/images.jsonl", "a") as f:
            f.write("{坏行\n")
        # 镜像：sg 节点 2 行（1 新 1 旧键）+ 尾部残行；cn 节点 1 新行
        _w(f"{tmp}/sync/manifests/sg-master/image-shard-0-of-2.jsonl",
           [_row("a" * 64, ["长城"]),          # 既有键：跳过
            _row("c" * 64, ["故宫"])])         # 新键：追加
        with open(f"{tmp}/sync/manifests/sg-master/image-shard-0-of-2.jsonl", "a") as f:
            f.write('{"sha256":"' + "d" * 64 + '"')   # 残行：不消费
        _w(f"{tmp}/sync/manifests/pipeline-e/image-shard-1-of-2.jsonl",
           [_row("e" * 64, ["长城"])])

        r1 = merge_meta.merge_images()
        assert r1["increment_rows"] == 3 and r1["appended"] == 2, r1
        # 二轮：零新增（幂等；残行仍不算）
        r2 = merge_meta.merge_images()
        assert r2["increment_rows"] == 0 and r2["appended"] == 0, r2
        # 补全残行成完整行 → 三轮吃进
        with open(f"{tmp}/sync/manifests/sg-master/image-shard-0-of-2.jsonl", "a") as f:
            f.write(', "instances": ["新"]}\n')
        r3 = merge_meta.merge_images()
        assert r3["increment_rows"] == 1 and r3["appended"] == 1, r3
        # 轮转：文件重建变小 → 归零重扫，键集防重复追加
        _w(f"{tmp}/sync/manifests/sg-master/image-shard-0-of-2.jsonl",
           [_row("c" * 64, ["故宫"])])         # 同键重写
        r4 = merge_meta.merge_images()
        assert r4["appended"] == 0, r4
        # 追加行真在账里（读侧坏行容忍，与真实读端口径一致）
        shas = set()
        for l in open(f"{tmp}/meta/images.jsonl", encoding="utf-8"):
            try:
                rec = json.loads(l)
            except json.JSONDecodeError:
                continue
            if "sha256" in rec:
                shas.add(rec["sha256"])
        assert {"a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64} <= shas, shas

        # docs：跨文件去重 + 原子替换 + 幂等
        _w(f"{tmp}/sync/manifests/sg-master/docs.jsonl",
           [{"page_sha": "p1", "concepts": ["长城"], "path": "pages/aa/p1.md"},
            {"page_sha": "p1", "concepts": ["长城"], "path": "pages/aa/p1.md"}])
        _w(f"{tmp}/sync/manifests/pipeline-e/docs-shard-0-of-2.jsonl",
           [{"page_sha": "p1", "concepts": ["长城"], "path": "pages/aa/p1.md"},
            {"page_sha": "p2", "concepts": ["故宫"], "path": "pages/bb/p2.md"}])
        d1 = merge_meta.merge_docs()
        assert d1["output_rows"] == 2 and d1["dup_dropped"] == 2, d1
        d2 = merge_meta.merge_docs()
        assert d2["output_rows"] == 2, d2
        # merge_all 汇总（锁路径走 fake）
        allr = merge_meta.merge_all()
        assert set(allr) == {"docs", "images"}
        print("[merge_meta] OK：偏移/残行/轮转/去重/断点/坏行容忍全过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
