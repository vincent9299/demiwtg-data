"""merge_shards 最小冒烟：分片去重合并、坏行容忍、原子写与 dry-run。
运行：python3 -m smokes.merge
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile

from operators.annotate import merge_manifests


def _shard(meta: str, name: str, rows: list) -> None:
    with open(os.path.join(meta, name), "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _row(sha: str, name: str, q: int = 8) -> dict:
    return {"sha256": sha, "concepts": [name], "quality": q, "path": f"blobs/x/{sha}.png"}


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="smoke_merge_")
    try:
        meta = os.path.join(tmp, "demiwtg", "meta")
        os.makedirs(meta)
        _shard(meta, "image-shard-0-of-2.jsonl",
               [_row("a", "甲"), _row("b", "乙")])
        _shard(meta, "image-shard-1-of-2.jsonl",
               [_row("b", "乙"),          # 完全重复（跨分片同键）
                _row("b", "丙"),          # 同 sha 跨实例：合法保留
                _row("c", "甲")])
        with open(os.path.join(meta, "image-shard-1-of-2.jsonl"),
                  "a", encoding="utf-8") as f:
            f.write('{"broken\n')          # 坏行容忍

        # dry-run：只统计不落盘
        r = merge_manifests(os.path.join(tmp, "demiwtg"), dry_run=True)
        assert r == {"shards": 2, "input_rows": 5, "output_rows": 4,
                     "dup_dropped": 1}, r
        assert not os.path.exists(os.path.join(meta, "image.jsonl"))
        print(f"[PASS] dry-run 统计：{r}")

        # 正式合并：原子写、行数与键集合
        r = merge_manifests(os.path.join(tmp, "demiwtg"))
        out = os.path.join(meta, "image.jsonl")
        lines = [json.loads(l) for l in open(out, encoding="utf-8")]
        assert len(lines) == 4
        keys = {(x["sha256"], x["concepts"][0]) for x in lines}
        assert keys == {("a", "甲"), ("b", "乙"), ("b", "丙"), ("c", "甲")}
        assert r["output_rows"] == 4 and r["dup_dropped"] == 1
        print("[PASS] 合并落盘：先到先得去重、同 sha 跨实例保留、坏行容忍")

        # 幂等：重复合并不增行
        r2 = merge_manifests(os.path.join(tmp, "demiwtg"),
                             pattern="metadata*.jsonl")
        assert r2["output_rows"] == 4
        print("[PASS] 幂等重合并")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
