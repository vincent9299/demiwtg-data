"""增量回湖管线（lake 常驻，2026-09-08）：采集节点 → 湖侧资产店。

模型（对齐 demi 理念：内容寻址即账本 / 幂等现算 / 断点续跑 / 单写者）：
- 清单增量镜像：每 (节点, 清单文件) 记字节偏移，tail 取增量，只消费完整行
  （轮转/重建检测：远端小于偏移则归零重读）；mirror 落 sync/manifests/<node>/
- 缺集现算：mirror 行取 blob_path/sha256；**湖侧 blob 实存 = 已同步**
  （内容寻址天然幂等，无独立传输账本）
- 拉取：tar 流（blob 已压缩，不 gzip）；两组均按哈希桶前缀 aa 分摊到组内
  全部 readers（SG 五口、CN 五口——CN 五机共享 GZ 桶，实测 g 的 blob 在
  e 上可见）；先落临时区，逐文件 sha256 复验后原子发布
- 回执清理（防重下闭环，**仅 blobs；pages 暂不清理**——集群侧 pages
  去重账本口径未确认前保守）：
  1) sha 写组共享桶 meta/synced_shas.jsonl（**先记账后删**，采集端凭它跳过）
  2) 再删组 COS 原对象（cosfs rm = 组内全可见）
  仅清理「已校验 + 过宽限期 + 湖侧仍在」的 blob，每轮限量
- 采集端对接：backfill --synced-ledger 读 synced_shas.jsonl 跳过；
  flow 的概念覆盖按清单计数，天然不受删除影响

2026-09-09 pages 回湖（docs 线正文，知识优先于图片）：
- 两类资产两套寻址：blobs=内容寻址（sha=内容哈希，强复验）；
  pages=URL 寻址（page.py：page_sha=sha256(url)，同 URL 重抓覆盖同名文件）
- 闸门版本化：docs 行自 2026-09-09 起带 content_sha/page_bytes
  （operators/page.py 记录），湖侧**强门**=sha256(内容)==content_sha；
  旧行缺 content_sha → 宽松门（非空 + 文件名 stem==page_sha 的身份自洽
  + sha256(url)==page_sha 行校验）。URL 寻址下内容本可漂移，强门只对
  新数据成立（闸门与数据同期升级，可追溯）
- 跨组全局去重：同 rel（=同 URL 的页面文件）先到组先得（GROUPS 序，
  sg 优先），修「同页被 sg/cn 各拉一次」的重复账（首轮实测 29 页）
- verified_pages.jsonl：拉取审计 + 未来 pages 清理的 pending 基底
  （与 verified.jsonl 同构——拉取路径只认湖侧实存，不读它）
- cycle 内 pages 先于 blobs：知识正文不该排在图片积压之后（当前串行，
  pages 积压变大时再议分轮/并发——会撞 state.json 单写者假设）

状态（sync/）：state.json（偏移）/ verified.jsonl + verified_pages.jsonl
（审计追加）/ deleted.jsonl / sync.log（轮摘要 jsonl）。全 stdlib，湖 pod 直跑。

用法：
    python3 lake_sync.py --once            # 单轮（测试）
    python3 lake_sync.py                   # 常驻：每小时一轮
    python3 lake_sync.py --once --pages-only               # 只补 pages
    python3 lake_sync.py --once --max-batches 2 --no-cleanup   # 冒烟
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import threading
import time

# ---------------------------------------------------------------------------
# 拓扑与常量（部署期拍板面）
# ---------------------------------------------------------------------------

LAKE_ROOT = "/yzp/zhaozy/yangzepeng/0905/demiwtg"
STORE_ROOT = f"{LAKE_ROOT}/datasets/demiwtg"     # blob_path 相对此根（blobs/aa/x）
SYNC_ROOT = f"{LAKE_ROOT}/sync"

# 组定义：readers=可读该组共享桶的节点（lake 视角 ssh 别名），
# blob_root=该组桶挂载内的 blob 根，ledger=组共享防重下账本（桶内）
GROUPS = {
    "sg": {"readers": ["sg-master", "pipeline-a", "pipeline-b",
                       "pipeline-c", "pipeline-d"],
           "blob_root": "/lhcos-data/demiwtg-data/datasets/demiwtg/blobs",
           "pages_root": "/lhcos-data/demiwtg-data/datasets/demiwtg/pages",
           "ledger": "/lhcos-data/demiwtg-data/meta/synced_shas.jsonl"},
    "cn": {"readers": ["pipeline-e", "pipeline-f", "pipeline-g",
                       "pipeline-h", "pipeline-i"],   # 五机共享 GZ 桶，五口分摊
           "blob_root": "/lhcos-data/demiwtg-data/datasets/demiwtg/blobs",
           "pages_root": "/lhcos-data/demiwtg-data/datasets/demiwtg/pages",
           "ledger": "/lhcos-data/demiwtg-data/meta/synced_shas.jsonl"},
}
NODE_GROUP = {  # 清单来自哪台 → blob 在哪个组的桶
    "sg-master": "sg", "pipeline-a": "sg", "pipeline-b": "sg",
    "pipeline-c": "sg", "pipeline-d": "sg",
    "pipeline-e": "cn", "pipeline-f": "cn",
    "pipeline-g": "cn", "pipeline-h": "cn", "pipeline-i": "cn",
}
REMOTE_META = "~/lake/meta"                   # 各节点清单根（本地盘）

MANIFEST_GLOBS = ("image-shard-*.jsonl", "docs*.jsonl",
                  "backfill-shard-*.jsonl", "dead-shard-*.jsonl")

BATCH_FILES = 400          # 每 tar 流文件数（argv 与失败重试粒度的折中）
GRACE_HOURS = 24           # 校验后保留宽限（小时）再清理源端
CLEAN_MAX = 20000          # 每轮清理上限（谨慎 ramp）
CYCLE_SECONDS = 3600
SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=20"]


def ssh_run(alias: str, cmd: str, stdin: bytes | None = None,
            timeout: int = 1800) -> subprocess.CompletedProcess:
    return subprocess.run(["ssh", *SSH_OPTS, alias, cmd],
                          input=stdin, capture_output=True, timeout=timeout)


# ---------------------------------------------------------------------------
# 状态
# ---------------------------------------------------------------------------

class State:
    """断点续跑状态：偏移（state.json）+ 已校验/已删除（追加 jsonl）。"""

    def __init__(self, root: str):
        os.makedirs(f"{root}/manifests", exist_ok=True)
        os.makedirs(f"{root}/tmp", exist_ok=True)
        self._wlock = threading.Lock()   # 组并行拉取下的审计追加互斥
        self.path = f"{root}/state.json"
        self.offsets: dict = {}
        if os.path.exists(self.path):
            self.offsets = json.load(open(self.path))["offsets"]
        self.verified: dict = {}      # sha -> {"ts","group"}（发布成功即记）
        vf = f"{root}/verified.jsonl"
        if os.path.exists(vf):
            for line in open(vf, encoding="utf-8"):
                try:
                    r = json.loads(line)
                    self.verified.setdefault(r["sha"], {"ts": r["ts"],
                                                        "group": r.get("group", "sg")})
                except (json.JSONDecodeError, KeyError):
                    continue
        self.verified_pages: dict = {}   # sha -> {"ts","group"}（pages 审计；
        vp = f"{root}/verified_pages.jsonl"   # 拉取路径只认湖侧实存，同 verified 同构）
        if os.path.exists(vp):
            for line in open(vp, encoding="utf-8"):
                try:
                    r = json.loads(line)
                    self.verified_pages.setdefault(
                        r["sha"], {"ts": r["ts"], "group": r.get("group", "sg")})
                except (json.JSONDecodeError, KeyError):
                    continue
        self.deleted: set = set()
        df = f"{root}/deleted.jsonl"
        if os.path.exists(df):
            for line in open(df, encoding="utf-8"):
                try:
                    self.deleted.add(json.loads(line)["sha"])
                except (json.JSONDecodeError, KeyError):
                    continue

    def save_offsets(self) -> None:
        json.dump({"offsets": self.offsets}, open(self.path, "w"))

    def log(self, record: dict) -> None:
        record["t"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(f"{SYNC_ROOT}/sync.log", "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def append_verified(self, sha: str, group: str) -> None:
        with self._wlock:
            self.verified[sha] = {"ts": time.time(), "group": group}
            with open(f"{SYNC_ROOT}/verified.jsonl", "a") as f:
                f.write(json.dumps({"sha": sha, "ts": self.verified[sha]["ts"],
                                    "group": group}) + "\n")

    def append_verified_page(self, sha: str, got_sha: str, group: str) -> None:
        """pages 拉取审计：got_sha=实收内容的哈希（URL 寻址下内容可漂移，
        收到什么记什么，供对账/未来清理 pending 用）。"""
        with self._wlock:
            self.verified_pages[sha] = {"ts": time.time(), "group": group}
            with open(f"{SYNC_ROOT}/verified_pages.jsonl", "a") as f:
                f.write(json.dumps({"sha": sha, "got": got_sha,
                                    "ts": self.verified_pages[sha]["ts"],
                                    "group": group}) + "\n")

    def append_deleted(self, sha: str) -> None:
        with self._wlock:
            self.deleted.add(sha)
            with open(f"{SYNC_ROOT}/deleted.jsonl", "a") as f:
                f.write(json.dumps({"sha": sha}) + "\n")


# ---------------------------------------------------------------------------
# ① 清单增量镜像
# ---------------------------------------------------------------------------

def sync_manifests(state: State) -> dict:
    """各节点清单 tail 增量 → lake mirror；返回 {node: 新行数}。"""
    got = {}
    for node in NODE_GROUP:
        r = ssh_run(node, (
            f'cd {REMOTE_META} 2>/dev/null || exit 0; for f in '
            + " ".join(MANIFEST_GLOBS) + '; do [ -e "$f" ] && '
            f'echo "$f $(stat -c %s "$f")"; done'), timeout=60)
        if r.returncode != 0:
            print(f"[sync] {node} 清单枚举失败：{r.stderr.decode()[:120]}",
                  flush=True)
            continue
        mdir = f"{SYNC_ROOT}/manifests/{node}"
        os.makedirs(mdir, exist_ok=True)
        offs = state.offsets.setdefault(node, {})
        new_lines = 0
        for ln in r.stdout.decode().splitlines():
            try:
                fname, size = ln.rsplit(" ", 1)
                size = int(size)
            except ValueError:
                continue
            off = offs.get(fname, 0)
            if size < off:            # 轮转/重建：归零重读
                off, offs[fname] = 0, 0
            if size == off:
                continue
            chunk = ssh_run(
                node, f'tail -c +{off + 1} {REMOTE_META}/{fname}', timeout=300)
            if chunk.returncode != 0:
                continue
            data = chunk.stdout
            # 只消费完整行：尾部残行留给下一轮（偏移不推进）
            if not data.endswith(b"\n"):
                cut = data.rfind(b"\n") + 1
                if cut == 0:
                    continue
                data = data[:cut]
            offs[fname] = off + len(data)
            with open(f"{mdir}/{fname}", "ab") as f:
                f.write(data)
            new_lines += data.count(b"\n")
        got[node] = new_lines
    state.save_offsets()
    return got


# ---------------------------------------------------------------------------
# ② 缺集现算
# ---------------------------------------------------------------------------

def needed_blobs(state: State) -> dict:
    """mirror 全扫 → {group: {rel_path}}（湖侧实存与已删除的剔除）。"""
    out: dict = {"sg": set(), "cn": set()}
    for node, group in NODE_GROUP.items():
        mdir = f"{SYNC_ROOT}/manifests/{node}"
        if not os.path.isdir(mdir):
            continue
        bucket = out[group]
        for fname in os.listdir(mdir):
            with open(f"{mdir}/{fname}", "rb") as f:
                for line in f:
                    try:
                        rel = json.loads(line)["blob_path"]
                    except (json.JSONDecodeError, KeyError):
                        continue
                    sha = rel.rsplit("/", 1)[-1].split(".")[0]
                    if sha in state.deleted:
                        continue
                    if os.path.exists(f"{STORE_ROOT}/{rel}"):
                        continue
                    bucket.add(rel)
    return out


# ---------------------------------------------------------------------------
# ③ 拉取（tar 流 + sha 复验 + 原子发布）
# ---------------------------------------------------------------------------

def reader_for(group: str, rel: str) -> str:
    aa = int(rel.split("/")[1][:2], 16)       # 哈希桶前缀分摊（确定性）
    return GROUPS[group]["readers"][aa % len(GROUPS[group]["readers"])]


def pull_group(state: State, group: str, rels: set, max_batches: int) -> dict:
    """一组缺集 → 按 reader 分桶批量 tar 流拉取。返回 {ok, bad, fail}。"""
    by_reader: dict = {}
    for rel in rels:
        by_reader.setdefault(reader_for(group, rel), []).append(rel)
    stat = {"ok": 0, "bad": 0, "fail": 0}
    batches = 0
    blob_root = GROUPS[group]["blob_root"]
    for reader, items in by_reader.items():
        for i in range(0, len(items), BATCH_FILES):
            if max_batches and batches >= max_batches:
                return stat
            batches += 1
            batch = items[i:i + BATCH_FILES]
            # blob_path 形如 blobs/aa/x（相对 STORE_ROOT）；tar -C 已指到
            # 组 blobs 根，成员去掉 blobs/ 前缀
            members = [r_[len("blobs/"):] for r_ in batch]
            payload = ("\n".join(members) + "\n").encode()
            t_batch = time.time()
            r = ssh_run(reader, f'tar -C {blob_root} --ignore-failed-read '
                                '-cf - -T -', stdin=payload, timeout=900)
            print(f"[sync] blob批 {reader} #{batches}（{len(batch)}）："
                  f"{time.time() - t_batch:.0f}s "
                  f"ok={stat['ok']} bad={stat['bad']} fail={stat['fail']}",
                  flush=True)
            if r.returncode != 0:
                stat["fail"] += len(batch)
                print(f"[sync] {reader} tar 流失败（{len(batch)} 文件）："
                      f"{r.stderr.decode()[:150]}", flush=True)
                continue
            tmpdir = f"{SYNC_ROOT}/tmp/{group}_{reader.replace('-', '_')}"
            os.makedirs(tmpdir, exist_ok=True)
            p = subprocess.run(["tar", "-xf", "-", "-C", tmpdir],
                               input=r.stdout, capture_output=True)
            if p.returncode != 0:
                stat["fail"] += len(batch)
                continue
            for rel, member in zip(batch, members):
                src = f"{tmpdir}/{member}"
                if not os.path.exists(src):
                    stat["fail"] += 1
                    continue
                sha = rel.rsplit("/", 1)[-1].split(".")[0]
                h = hashlib.sha256()
                with open(src, "rb") as f:
                    for c in iter(lambda: f.read(1 << 20), b""):
                        h.update(c)
                if h.hexdigest() != sha:
                    stat["bad"] += 1
                    os.unlink(src)
                    continue
                dst = f"{STORE_ROOT}/{rel}"
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                os.replace(src, dst)
                state.append_verified(sha, group)
                stat["ok"] += 1
    return stat


# ---------------------------------------------------------------------------
# ③' pages 缺集现算与拉取（URL 寻址；docs 线正文，2026-09-09）
# ---------------------------------------------------------------------------

def needed_pages(state: State) -> tuple:
    """镜像 docs* 行 → ({group: {rel: content_sha|None}}, 行闸门不符数)。

    行闸门（身份自洽，URL 寻址的验法）：page_sha == sha256(url)，不符
    计数跳过（镜像脏行/字段漂移）。湖侧实存 = 已同步（与 blobs 同口径，
    不读 verified_pages）。**跨组全局去重**：同 rel（同 URL 的页面文件，
    sg/cn 两队都可能抓过）先到组先得——按 GROUPS 序 sg 优先，修首轮
    实测的同页双拉（29/10,049）。content_sha 缺省（2026-09-09 前旧行）
    → 拉取端降级宽松门。
    """
    claims: dict = {}                # rel -> (group, content_sha|None)
    mismatch = 0
    for group in GROUPS:             # GROUPS 序即组优先序（sg 先claim）
        for node, node_group in NODE_GROUP.items():
            if node_group != group:
                continue
            mdir = f"{SYNC_ROOT}/manifests/{node}"
            if not os.path.isdir(mdir):
                continue
            for fname in os.listdir(mdir):
                if not (fname.startswith("docs") and fname.endswith(".jsonl")):
                    continue
                with open(f"{mdir}/{fname}", encoding="utf-8") as f:
                    for line in f:
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        rel = row.get("path")
                        if not rel or not str(rel).startswith("pages/"):
                            continue
                        sha, url = row.get("page_sha"), row.get("url")
                        if not sha or not url or \
                                hashlib.sha256(str(url).encode()).hexdigest() != sha:
                            mismatch += 1
                            continue
                        if os.path.exists(f"{STORE_ROOT}/{rel}") or rel in claims:
                            continue
                        claims[rel] = (group, row.get("content_sha"))
    out = {g: {} for g in GROUPS}
    for rel, (g, csha) in claims.items():
        out[g][rel] = csha
    return out, mismatch


def _publish_page(state: State, group: str, tmpdir: str, member: str,
                  rel: str, expect_sha) -> str:
    """单页发布：解包成员 → 闸门 → 原子发布。返回计数键（ok/bad/empty/
    dup/fail）。闸门版本化：expect_sha 在（新数据）→ sha256(内容) 强复验；
    缺省（旧行）→ 宽松门（非空 + 文件名 stem==page_sha 身份自洽）。"""
    src = f"{tmpdir}/{member}"
    if not os.path.exists(src):
        return "fail"
    sha = os.path.basename(rel).split(".")[0]
    if os.path.basename(rel) != f"{sha}.md":   # 名字不自洽（畸形 rel）
        os.unlink(src)
        return "bad"
    h = hashlib.sha256()
    with open(src, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    got = h.hexdigest()
    dst = f"{STORE_ROOT}/{rel}"
    if os.path.exists(dst):                    # 防御：本周期内已被占位
        os.unlink(src)
        return "dup"
    size = os.path.getsize(src)
    if size == 0:
        os.unlink(src)
        return "empty"
    if expect_sha and got != expect_sha:
        os.unlink(src)
        return "bad"
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.replace(src, dst)
    state.append_verified_page(sha, got, group)
    return "ok"


def pull_pages(state: State, group: str, want: dict, max_batches: int) -> dict:
    """一组 pages 缺集 → 按 reader 分桶批量 tar 流拉取（pull_group 同构）。

    want: {rel: content_sha|None}；返回 {ok, bad, empty, dup, fail}。"""
    by_reader: dict = {}
    for rel in want:
        by_reader.setdefault(reader_for(group, rel), []).append(rel)
    stat = {"ok": 0, "bad": 0, "empty": 0, "dup": 0, "fail": 0}
    batches = 0
    root = GROUPS[group]["pages_root"]
    for reader, items in by_reader.items():
        for i in range(0, len(items), BATCH_FILES):
            if max_batches and batches >= max_batches:
                return stat
            batches += 1
            batch = items[i:i + BATCH_FILES]
            members = [r[len("pages/"):] for r in batch]   # 剥 pages/ 前缀
            payload = ("\n".join(members) + "\n").encode()
            t_batch = time.time()
            r = ssh_run(reader, f'tar -C {root} --ignore-failed-read '
                                '-cf - -T -', stdin=payload, timeout=900)
            print(f"[sync] pages批 {reader} #{batches}（{len(batch)}）："
                  f"{time.time() - t_batch:.0f}s "
                  f"ok={stat['ok']} bad={stat['bad']} fail={stat['fail']}",
                  flush=True)
            if r.returncode != 0:
                stat["fail"] += len(batch)
                print(f"[sync] {reader} pages tar 流失败（{len(batch)} 文件）："
                      f"{r.stderr.decode()[:150]}", flush=True)
                continue
            tmpdir = f"{SYNC_ROOT}/tmp/pages_{group}_{reader.replace('-', '_')}"
            os.makedirs(tmpdir, exist_ok=True)
            p = subprocess.run(["tar", "-xf", "-", "-C", tmpdir],
                               input=r.stdout, capture_output=True)
            if p.returncode != 0:
                stat["fail"] += len(batch)
                continue
            for rel, member in zip(batch, members):
                stat[_publish_page(state, group, tmpdir, member, rel,
                                   want[rel])] += 1
    return stat


# ---------------------------------------------------------------------------
# ④ 回执清理（先记账后删，宽限期 + 每轮限量）
# ---------------------------------------------------------------------------

def cleanup_group(state: State, group: str) -> int:
    """已校验且过宽限的 blob：组桶账本追加 → COS 删除。返回删除数。"""
    now = time.time()
    pend = sorted(sha for sha, v in state.verified.items()
                  if v["group"] == group and now - v["ts"] > GRACE_HOURS * 3600
                  and sha not in state.deleted)
    if not pend:
        return 0
    pend = pend[:CLEAN_MAX]
    g = GROUPS[group]
    # 1) 防重下账本（组共享桶内，一台写全组可见；jsonl {"s":sha} 与采集端
    #    load_sha_ledger 同构）
    r = ssh_run(g["readers"][0], f'mkdir -p {os.path.dirname(g["ledger"])} && '
                                 f'cat >> {g["ledger"]}',
                stdin="".join(json.dumps({"s": s}) + "\n" for s in pend).encode(),
                timeout=600)
    if r.returncode != 0:
        print(f"[sync] {group} 账本写入失败，跳过本轮清理", flush=True)
        return 0
    # 2) 删 COS 原对象（湖侧实存复核后逐批）
    done = 0
    for i in range(0, len(pend), 2000):
        chunk = pend[i:i + 2000]
        rels = []
        for sha in chunk:
            hit = None
            for ext in ("jpg", "png", "gif", "webp", "jpeg", "bin"):
                rel = f"blobs/{sha[:2]}/{sha}.{ext}"
                if os.path.exists(f"{STORE_ROOT}/{rel}"):
                    hit = rel[len("blobs/"):]
                    break
            if hit:
                rels.append(hit)
        if not rels:
            continue
        r = ssh_run(g["readers"][0],
                    f'cd {g["blob_root"]} && xargs -d "\\n" rm -f',
                    stdin=("\n".join(rels) + "\n").encode(), timeout=1200)
        if r.returncode != 0:
            print(f"[sync] {group} 删除失败：{r.stderr.decode()[:120]}",
                  flush=True)
            break
        for sha in chunk:
            state.append_deleted(sha)
        done += len(chunk)
    return done


# ---------------------------------------------------------------------------
# 轮次与入口
# ---------------------------------------------------------------------------

def cycle(state: State, max_batches: int, no_cleanup: bool,
          pages_only: bool = False, merge_meta_off: bool = False) -> dict:
    t0 = time.time()
    mans = sync_manifests(state)
    # pages 先于 blobs：知识正文不该排在图片积压后（知识库主线优先）
    pages_need, page_mm = needed_pages(state)
    pulled_pages = {g: pull_pages(state, g, rels, max_batches)
                    for g, rels in pages_need.items() if rels}
    rec = {"new_manifest_lines": mans,
           "needed_pages": {g: len(v) for g, v in pages_need.items()},
           "pulled_pages": pulled_pages,
           "page_url_mismatch": page_mm}
    if pages_only:
        rec["minutes"] = round((time.time() - t0) / 60, 1)
        state.log(rec)
        print(f"[sync] 轮完成（pages-only）："
              f"{json.dumps(rec, ensure_ascii=False)}", flush=True)
        return rec
    need = needed_blobs(state)
    # 两组并行拉取（吞吐×2；积压 47 万时串行单轮十小时级，小时节拍会被
    # 单轮吞掉）。State 审计追加有锁；tmpdir 按 组+reader 隔离
    pulls: dict = {}
    threads: list = []

    def _pull(g, rels):
        pulls[g] = pull_group(state, g, rels, max_batches)
    for g, rels in need.items():
        if rels:
            t = threading.Thread(target=_pull, args=(g, rels))
            t.start()
            threads.append(t)
    for t in threads:
        t.join()
    cleaned = {} if no_cleanup else {g: cleanup_group(state, g)
                                     for g in GROUPS}
    # 镜像 → 真 meta（例行合并，收口「镜像清单→总账」这一步；独立 try
    # ——账面合并不应拖垮同步轮，失败下轮自然重试幂等）
    meta_merged = None
    if not merge_meta_off:
        try:
            import merge_meta
            meta_merged = merge_meta.merge_all()
        except Exception as exc:      # noqa: BLE001 - 合并失败只记账不断轮
            print(f"[sync] meta 合并异常：{type(exc).__name__}: {exc}",
                  flush=True)
    rec.update({"needed": {g: len(v) for g, v in need.items()},
                "pulled": pulls, "cleaned": cleaned,
                "meta_merged": meta_merged,
                "minutes": round((time.time() - t0) / 60, 1)})
    state.log(rec)
    print(f"[sync] 轮完成：{json.dumps(rec, ensure_ascii=False)}", flush=True)
    return rec


def main() -> None:
    p = argparse.ArgumentParser(description="增量回湖管线（lake 常驻）")
    p.add_argument("--once", action="store_true", help="单轮后退出（测试）")
    p.add_argument("--cycle-seconds", type=int, default=CYCLE_SECONDS)
    p.add_argument("--max-batches", type=int, default=0,
                   help="每组每轮最多 tar 流批数（0=不限；测试用）")
    p.add_argument("--no-cleanup", action="store_true", help="本轮不清理源端")
    p.add_argument("--pages-only", action="store_true",
                   help="只跑清单镜像+pages 回湖（不动 blobs 与清理）")
    p.add_argument("--no-merge-meta", action="store_true",
                   help="轮末不合并镜像→meta 总账（merge_meta.py）")
    args = p.parse_args()
    state = State(SYNC_ROOT)
    while True:
        try:
            cycle(state, args.max_batches, args.no_cleanup,
                  pages_only=args.pages_only,
                  merge_meta_off=args.no_merge_meta)
        except Exception as exc:      # noqa: BLE001 - 单轮失败不倒常驻
            print(f"[sync] 轮异常：{type(exc).__name__}: {exc}", flush=True)
            state.log({"error": f"{type(exc).__name__}: {exc}"})
        if args.once:
            break
        time.sleep(args.cycle_seconds)


if __name__ == "__main__":
    main()
