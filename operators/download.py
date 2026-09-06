"""data_pipeline 下载算子：候选行 → 图像行（引用化，2026-09-04·D1）。

行契约：
- 读键：source、tiers（档位大到小）
- 追加键：content_url（获胜档）、sha256、mime、ext、
  actual_width/actual_height（实测）、size_bytes、
  **blob_path**（数据集内相对路径 blobs/<aa>/<sha>.<ext>）
- **不追加 data**：字节在下载成功即原子写入 blob（内容寻址 + pid 唯一
  临时名，跨节点并发同内容写安全），下游按 blob_path 读——分布式传输
  缝的行引用化（行跨节点传输不携字节，只传引用）。

机制：fetch_tiers（档位轮转/字节封顶/硬超时）+ verify_image（Pillow
全量解码+mime/ext 规范表）+ atomic_write_bytes（引擎原子写）。
本算子只持业务面：按源防盗链头表 + 行键追加。

崩溃窗口：blob 已写、清单未记 → 孤儿 blob（重跑幂等覆盖；合并去重以
清单为准），可由清理任务回收——单机强一致换分布式最终一致的代价，
VLM/清单永不双写。
"""

from __future__ import annotations

import asyncio
import os

import httpx

from operators.search import API_UA
from demiflow.collect import net
from demiflow.collect.fetch import fetch_tiers
from demiflow.collect.images import verify_image
from demiflow.collect.store import atomic_write_bytes
from demiflow.data.plan import StreamStage

MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024    # 单图字节上限（用户拍板 20MB）
# 每请求硬超时：read=30s 只管单次读，慢渗连接可以一直通过单读超时；
# 包总时长封顶把永久阻塞降级成丢一张图。
DOWNLOAD_HARD_TIMEOUT = 90.0

# 下载头按源登记（站点防盗链知识，实网实证）：
# - baidu CDN 防盗链必须带来源 Referer；wikimedia 系按礼仪带身份 UA；
# - pixiv i.pximg.net 防盗链必须带站内 Referer；
# - 国内爬虫三源 CDN 防盗链，Referer 带各站搜索页；未登记源浏览器 UA 兜底。


def download_headers_for(source: str) -> dict:
    """按源取下载头（身份 UA 复用 search.API_UA，浏览器 UA 用引擎规范值）。"""
    browser = net.BROWSER_UA
    table = {
        "baidu": {"User-Agent": browser,
                  "Referer": "https://image.baidu.com/",
                  "Accept": "image/avif,image/webp,image/*,*/*;q=0.8"},
        "wikimedia": {"User-Agent": API_UA},
        "wikimedia_zh": {"User-Agent": API_UA},
        "pixiv": {"User-Agent": browser,
                  "Referer": "https://www.pixiv.net/",
                  "Accept": "image/avif,image/webp,image/*,*/*;q=0.8"},
        "huaban_api": {"User-Agent": browser,
                       "Referer": "https://huaban.com/",
                       "Accept": "image/avif,image/webp,image/*,*/*;q=0.8"},
        "toutiao": {"User-Agent": browser,
                    "Referer": "https://so.toutiao.com/",
                    "Accept": "image/avif,image/webp,image/*,*/*;q=0.8"},
        "so360": {"User-Agent": browser,
                  "Referer": "https://image.so.com/",
                  "Accept": "image/avif,image/webp,image/*,*/*;q=0.8"},
    }
    return table.get(source, {"User-Agent": browser, "Accept": "image/*,*/*;q=0.8"})


class DownloadStage(StreamStage):
    """下载算子（demiflow 规范）：候选行 → 图像行（引用化）。"""

    label = "download"
    concurrency = 32
    queue_depth = 48
    catch = (net.InfraError, httpx.HTTPError)   # 下载级认缺白名单

    def __init__(self, dataset_dir: str):
        self.dataset_dir = dataset_dir

    async def __call__(self, row: dict):
        tiers = [u for u in (row.get("tiers") or [])
                 if str(u).startswith(("http://", "https://"))]
        if not tiers:
            return None
        got = await fetch_tiers(
            tiers,
            source=row["source"],
            max_bytes=MAX_DOWNLOAD_BYTES,
            hard_timeout=DOWNLOAD_HARD_TIMEOUT,
            headers=download_headers_for(row["source"]),
            verify=verify_image,
        )
        if got is None:
            return None
        rel = f"blobs/{got.sha256[:2]}/{got.sha256}.{got.extra['ext']}"
        # 即时落盘：内容寻址原子写（磁盘 IO 丢线程池）
        await asyncio.to_thread(
            atomic_write_bytes, os.path.join(self.dataset_dir, rel), got.data)
        row["content_url"] = got.url
        row["sha256"] = got.sha256
        row["mime"] = got.extra["mime"]
        row["ext"] = got.extra["ext"]
        row["actual_width"] = got.extra["width"]
        row["actual_height"] = got.extra["height"]
        row["size_bytes"] = got.size_bytes
        row["blob_path"] = rel
        return row
