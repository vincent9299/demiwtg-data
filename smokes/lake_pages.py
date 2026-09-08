"""lake_sync pages 逻辑冒烟（纯本地，不 ssh）：needed_pages 的行闸门/
跨组去重/实存跳过 + _publish_page 的版本化闸门（强门/宽松门/异常路径）。

运行：.venv/bin/python smokes/lake_pages.py
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lake_sync  # noqa: E402


def _write(path: str, rows: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="lake_pages_")
    try:
        sync_root = os.path.join(tmp, "sync")
        store_root = os.path.join(tmp, "store")
        lake_sync.SYNC_ROOT = sync_root
        lake_sync.STORE_ROOT = store_root
        state = lake_sync.State(sync_root)

        # 镜像布置：sg 节点 3 行（新/旧行），cn 节点 2 行（同 rel 重复claim
        # + 行闸门不符 + 湖侧已存在）
        url_a, url_b, url_c = "https://a/1", "https://b/1", "https://c/1"
        rel_a = f"pages/{_sha(url_a)[:2]}/{_sha(url_a)}.md"
        rel_b = f"pages/{_sha(url_b)[:2]}/{_sha(url_b)}.md"
        rel_c = f"pages/{_sha(url_c)[:2]}/{_sha(url_c)}.md"
        _write(os.path.join(sync_root, "manifests/sg-master/docs.jsonl"), [
            {"page_sha": _sha(url_a), "url": url_a, "path": rel_a,
             "content_sha": "c" * 64},                      # 新行：强门料
            {"page_sha": _sha(url_b), "url": url_b, "path": rel_b},
            {"page_sha": "deadbeef", "url": url_a, "path": rel_a},  # 闸门不符
        ])
        _write(os.path.join(sync_root, "manifests/pipeline-e/docs.jsonl"), [
            {"page_sha": _sha(url_a), "url": url_a, "path": rel_a},
            {"page_sha": _sha(url_c), "url": url_c, "path": rel_c},
        ])
        # 湖侧已存在 rel_c（实存=已同步）
        os.makedirs(os.path.join(store_root, os.path.dirname(rel_c)),
                    exist_ok=True)
        open(os.path.join(store_root, rel_c), "wb").write(b"x")

        need, mm = lake_sync.needed_pages(state)
        assert mm == 1, f"行闸门不符应 1，实际 {mm}"
        assert set(need["sg"]) == {rel_a, rel_b}, need       # 全部 sg claim
        assert not need["cn"], "同 rel 应被跨组去重，cn 无缺集"
        assert need["sg"][rel_a] == "c" * 64                 # content_sha 透传
        assert need["sg"][rel_b] is None                     # 旧行宽松门

        # _publish_page 五路
        td = os.path.join(tmp, "pub")
        os.makedirs(td, exist_ok=True)
        good = "正文" * 100

        def put(name: str, data: bytes):
            open(os.path.join(td, name), "wb").write(data)
            return name

        # 强门通过
        m = put("m1.md", good.encode())
        assert lake_sync._publish_page(
            state, "sg", td, m, rel_a,
            hashlib.sha256(good.encode()).hexdigest()) == "ok"
        assert os.path.exists(os.path.join(store_root, rel_a))
        # 强门不符（新数据内容漂移 → 拒发布）
        m = put("m2.md", b"tampered")
        assert lake_sync._publish_page(
            state, "sg", td, m, rel_b, "f" * 64) == "bad"
        # 宽松门（旧行无 content_sha）：非空即过
        m = put("m3.md", good.encode())
        assert lake_sync._publish_page(state, "sg", td, m, rel_b, None) == "ok"
        # 空文件 / 缺成员 / 湖侧已占位（empty/fail 用全新 rel，避开已发布路径）
        url_d = "https://d/1"
        rel_d = f"pages/{_sha(url_d)[:2]}/{_sha(url_d)}.md"
        m = put("m4.md", b"")
        assert lake_sync._publish_page(state, "sg", td, m, rel_d, None) == "empty"
        assert lake_sync._publish_page(
            state, "sg", td, "missing.md", rel_d, None) == "fail"
        m = put("m5.md", good.encode())
        assert lake_sync._publish_page(state, "sg", td, m, rel_a, None) == "dup"
        # 审计账本落了 2 条（ok×2），got 哈希与内容一致
        lines = [json.loads(l) for l in
                 open(os.path.join(sync_root, "verified_pages.jsonl"))]
        assert len(lines) == 2 and lines[0]["got"] == \
            hashlib.sha256(good.encode()).hexdigest()
        print("[lake_pages] OK：行闸门/跨组去重/实存跳过/版本化发布门全过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
