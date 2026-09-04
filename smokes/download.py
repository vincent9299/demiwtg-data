"""collect_v2/download.py 最小冒烟：MockTransport 验证档位轮转/拒收/
dict 行追加键。运行：python3 -m smoke_download
"""

from __future__ import annotations

import asyncio
import io

import httpx
from PIL import Image

from operators import download
from demiflow.collect import net


def png(w: int, h: int, rgb=(120, 120, 120)) -> bytes:
    im = Image.new("RGB", (w, h), rgb)
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()


def cand(tiers, source="baidu", **kw):
    row = {"name": "测试", "query": "测试", "lang": "zh",
           "source": source, "tiers": tiers}
    row.update(kw)
    return row


async def main() -> None:
    net.RETRY_INTERVAL = 0.05
    import tempfile
    tmp_ds = tempfile.mkdtemp(prefix="smoke_dl_")
    stage = download.DownloadStage(tmp_ds)

    good = png(100, 80)
    bigger = png(200, 160)

    # 1) 档位轮转：首档 404 换次档
    def h(req):
        if "bad" in str(req.url):
            return httpx.Response(404)
        return httpx.Response(200, content=good)
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(h)))
    row = await stage(cand(["https://x/bad.png", "https://x/ok.png"]))
    assert row is not None
    assert row["content_url"] == "https://x/ok.png"
    assert row["sha256"] == __import__("hashlib").sha256(good).hexdigest()
    assert row["ext"] == "png" and row["mime"] == "image/png"
    assert row["actual_width"] == 100 and row["actual_height"] == 80
    assert row["size_bytes"] == len(good)
    # 引用化：行不携字节，blob 已原子落盘
    import os
    assert "data" not in row
    assert row["blob_path"] == f"blobs/{row['sha256'][:2]}/{row['sha256']}.png"
    blob = os.path.join(tmp_ds, row["blob_path"])
    assert open(blob, "rb").read() == good
    print("[PASS] 档位轮转 + 引用化（blob 即时落盘、行不携字节）")

    # 2) 非图拒收不轮转
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(200, content=b"not image"))))
    assert await stage(cand(["https://x/a", "https://x/b"])) is None
    print("[PASS] 非图拒收")

    # 3) 超限拒收
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(200, content=bigger))))
    assert await stage(cand(["https://x/a"], ), ) is not None   # 200x160 未超限
    big = b"x" * (download.MAX_DOWNLOAD_BYTES + 1)
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(200, content=big))))
    assert await stage(cand(["https://x/a"])) is None
    print("[PASS] 字节上限拒收")

    # 4) 全档确定性失败上抛（编排 catch 认缺）
    net.set_download_client(httpx.AsyncClient(transport=httpx.MockTransport(
        lambda req: httpx.Response(403))))
    try:
        await stage(cand(["https://x/a"]))
        raise AssertionError("应上抛")
    except net.DeterministicError:
        pass
    print("[PASS] 全档失败上抛")

    # 5) 无候选行直接 None
    assert await stage(cand([])) is None
    print("[PASS] 无候选拒收")
    await net.close_client()
    print("冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
