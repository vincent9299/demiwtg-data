"""D2 分片并行最小冒烟：2 个 flow 子进程（真实进程隔离）各自分片运行，
验证：分片单写者清单、跨进程同图并发 blob 原子写、限速等分、合并=全量。
运行：python3 -m smokes.shard
"""

from __future__ import annotations

import hashlib
import io
import json
import multiprocessing
import os
import shutil
import sys
import tempfile

import httpx
from PIL import Image

CASES = ["甲实体", "乙实体", "丙实体"]      # 甲乙同图（跨分片 blob 撞写用）
GOOD_ANN = json.dumps({
    "kb_match": 8, "richness": 7, "identity": True, "focus": 8,
    "caption": "一段足够长的中文描述，用来满足 caption 最小字数校验要求，确保解析成功。",
}, ensure_ascii=False)


def _png(tag: int) -> bytes:
    im = Image.new("RGB", (64 + tag, 48), (10 * tag % 255, 20, 30))
    buf = io.BytesIO()
    im.save(buf, "PNG")
    return buf.getvalue()


def _install_mocks():
    """进程内 mock：searxng 检索 + 图片下载 + VLM（每子进程独立安装）。"""
    from demiflow.collect import net
    from demiflow.collect.llm import AsyncLLMClient, inject_endpoint_client

    net.RETRY_INTERVAL = 0.05
    bytes_by_url: dict = {}

    def images(q: str) -> list:
        # 甲/乙同图集（跨分片并发写同 blob）；丙独立
        tag_base = 0 if q in CASES[:2] else 50
        out = []
        for i in range(5):
            data = _png(tag_base + i)
            sha = hashlib.sha256(data).hexdigest()
            url = f"https://mock.cdn/{sha}.png"
            bytes_by_url[url] = data
            out.append({"img_src": url, "thumbnail_src": f"{sha}_t",
                        "url": f"https://mock.page/{tag_base}/{i}",
                        "engine": "mock", "title": f"{q}-{i}"})
        return out

    def search_handler(req):
        q = str(req.url.params.get("q", ""))
        return httpx.Response(200, json={"results": images(q)})

    def dl_handler(req):
        data = bytes_by_url.get(str(req.url).replace("_t", ""))
        return httpx.Response(200, content=data) if data else httpx.Response(404)

    mock = httpx.AsyncClient(transport=httpx.MockTransport(search_handler))
    dl = httpx.AsyncClient(transport=httpx.MockTransport(dl_handler))
    net.set_client(mock)
    net.set_client(mock, proxy=True)
    net.set_download_client(dl)
    net.set_download_client(dl, proxy=True)
    inject_endpoint_client("demiwtg_vlm", AsyncLLMClient(
        base_url="http://mock/v1", model="mock-model",
        http=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda req: httpx.Response(200, json={
                "choices": [{"message": {"content": GOOD_ANN}}]})))))

    from operators import search
    search.ROUTE_TABLE = {"zh": ["searxng"], "latin": ["searxng"]}


def _child(shard: str, inst_path: str, dataset_dir: str, alias_dir: str) -> None:
    """子进程：装 mock → flow.main(--shard)。异常即非零退出。"""
    _install_mocks()
    sys.argv = ["flow", "--instances", inst_path, "--dataset", dataset_dir,
                "--alias-cache", os.path.join(alias_dir, f"alias.{shard}.json"),
                "--top-n", "2", "--vlm-concurrency", "4",
                "--search-concurrency", "2", "--download-concurrency", "4",
                "--instance-concurrency", "2", "--log-every", "1",
                "--shard", shard]
    import flow
    flow.main()


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="smoke_shard_")
    try:
        inst_path = os.path.join(tmp, "instances.json")
        json.dump({"instances": [{"name": n, "desc": "测试。"} for n in CASES]},
                  open(inst_path, "w", encoding="utf-8"), ensure_ascii=False)
        dataset_dir = os.path.join(tmp, "demiwtg")

        # 1) 两个真实子进程并行分片运行
        procs = [multiprocessing.Process(
            target=_child, args=(f"{i}/2", inst_path, dataset_dir, tmp))
            for i in range(2)]
        for p_ in procs:
            p_.start()
        for p_ in procs:
            p_.join()
        assert all(p_.exitcode == 0 for p_ in procs), \
            [p_.exitcode for p_ in procs]
        print("[PASS] 双进程分片并行运行（退出码全 0）")

        meta = os.path.join(dataset_dir, "meta")
        shards = sorted(f for f in os.listdir(meta)
                        if f.startswith("metadata-shard"))
        assert shards == ["metadata-shard-0-of-2.jsonl",
                          "metadata-shard-1-of-2.jsonl"]
        print("[PASS] 每分片单写者清单")

        # 2) 跨进程 blob 撞写：甲(分片0)与乙(分片1)同图集 → 同 blob 路径
        #    并发原子写；校验全部 blob 完整（sha 与内容一致）
        from operators.annotate import merge_manifests
        r = merge_manifests(dataset_dir)
        assert r["shards"] == 2 and r["dup_dropped"] == 0, r
        rows = [json.loads(l) for l in open(
            os.path.join(meta, "metadata.jsonl"), encoding="utf-8")]
        keys = {(x["sha256"], x["instances"][0]) for x in rows}
        shas = {x["sha256"] for x in rows}
        assert len(rows) == 6 and len(keys) == 6      # 3 实例 × top2
        assert len(shas) == 4                          # 甲乙同 2 图 + 丙 2 图
        for x in rows:
            blob = os.path.join(dataset_dir, x["path"])
            assert hashlib.sha256(open(blob, "rb").read()).hexdigest() == x["sha256"]
        inst_set = {i for _, i in keys}
        assert inst_set == set(CASES)
        assert all(x["kb_match"] == 8 for x in rows)
        print(f"[PASS] 跨进程并发 blob 原子写 + 合并=全量"
              f"（6 行 / 4 唯一图，甲乙跨分片同图零损坏）")

        # 3) 限速等分（父进程内单测口径）
        from demiflow.collect import net
        from operators import search
        saved = dict(net.SOURCE_LIMITS)
        b0 = net.SOURCE_LIMITS["baidu"]
        search.scale_engine_limits(3)
        b3 = net.SOURCE_LIMITS["baidu"]
        assert (b3.rate, b3.concurrency) == (b0.rate / 3, b0.concurrency // 3)
        assert net.SOURCE_LIMITS["dl:searxng"].proxy is True   # 代理归属不被等分破坏
        net.register_limits(saved)                              # 还原
        print("[PASS] 限速预算等分（rate/concurrency ÷N，代理归属保留）")
        print("冒烟全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
