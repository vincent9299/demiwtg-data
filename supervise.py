"""flow 守护：托管 flow 子进程（单进程或分片多进程），停摆自动重启
（2026-08-21 定案；2026-09-04·D2 升级分片并行托管）。

背景：旧链一夜三次静默停摆（HTTP 连接池半读连接复用死锁，
net.get_client 已禁 keep-alive 缓解，但不保证根绝）。停摆特征是
「进程活着、CPU 近零、清单不再增长」，无人值守时只能靠重启恢复；
--skip-covered 保证重启续跑无损，故自愈代价仅一个启动周期。

D2 分片形态（--shards N）：
- 每分片一个 flow 子进程（自动追加 --shard i/N）：输入切片、各自
  单写者清单（image-shard-i-of-N.jsonl）、各自分片词表与日志；
- 限速预算由 flow 侧按分片数等分（scale_engine_limits），N 进程
  合计不超发；
- 停摆判定按各分片自己的清单行数，独立重启互不影响；
- 跑完用 merge_shards.py 合并分片清单。

职责边界：只做「看门狗」——拉起、盯清单行数、停摆则 kill+重拉；
不做任何数据处理，flow 的全部参数原样透传。

用法：
    python3 -m supervise -- --top-n 2                     # 单进程（同旧）
    python3 -m supervise --shards 3 -- --skip-covered 8   # 3 分片并行
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATASET = os.path.join(REPO_ROOT, "datasets", "demiwtg")
DEFAULT_MANIFEST = os.path.join(
    REPO_ROOT, "datasets", "demiwtg", "meta", "image.jsonl")


def manifest_lines(path: str) -> int:
    try:
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0


def main() -> None:
    p = argparse.ArgumentParser(description="flow 停摆自愈守护（含分片并行）")
    p.add_argument("--stall-minutes", type=int, default=12,
                   help="清单零增长超过该时长判定停摆并重启（默认 12）")
    p.add_argument("--check-seconds", type=int, default=60,
                   help="清单采样间隔（默认 60）")
    p.add_argument("--dataset", default=DEFAULT_DATASET,
                   help="分片清单推导根（默认 datasets/demiwtg）")
    p.add_argument("--shards", type=int, default=1,
                   help="分片并行数：每分片一个 flow 子进程（默认 1=单进程）")
    p.add_argument("--flow-log", "--chain-log", default=None,
                   help="子进程日志（默认 logs/supervised_flow[_shardN].log）")
    p.add_argument("flow_args", nargs=argparse.REMAINDER,
                   help="'--' 之后原样透传给 flow")
    args = p.parse_args()

    flow_args = [a for a in args.flow_args if a != "--"]
    if not flow_args:
        p.error("需要在 '--' 后给出 flow 的完整参数")
    n = max(1, args.shards)

    # 透传参数里的 --dataset / --shard（单进程 supervise 挂分片时，
    # 停摆监看应指向分片清单而非默认清单——2026-09-06 实跑踩坑）
    def _passthrough(name, default=None):
        try:
            i = flow_args.index(name)
            return flow_args[i + 1]
        except (ValueError, IndexError):
            return default
    shard_arg = _passthrough("--shard", "")
    if n == 1 and shard_arg:
        try:
            si, sn = (int(x) for x in shard_arg.split("/"))
            assert 0 <= si < sn
            n_s = sn          # 仅为推导清单名；子进程仍由 flow 自己切分片
        except Exception:
            si = 0
        dataset = _passthrough("--dataset", args.dataset)
        manifest = os.path.join(
            dataset, "meta", f"image-shard-{si}-of-{shard_arg.split('/')[1]}.jsonl")
    else:
        manifest = None

    os.makedirs(os.path.join(REPO_ROOT, "logs"), exist_ok=True)
    children: list[dict] = []
    for i in range(n):
        cmd = [sys.executable, "-m", "flow", *flow_args]
        if n > 1:
            cmd += ["--shard", f"{i}/{n}"]
        if manifest is not None and n == 1:
            pass               # 单 supervise 挂分片：用透传推导的清单
        else:
            manifest = (DEFAULT_MANIFEST if n == 1 else os.path.join(
                args.dataset, "meta", f"image-shard-{i}-of-{n}.jsonl"))
        log = args.flow_log or os.path.join(
            REPO_ROOT, "logs",
            "supervised_flow.log" if n == 1
            else f"supervised_flow_shard{i}.log")
        children.append({"idx": i, "cmd": cmd, "manifest": manifest,
                         "log": log, "proc": None, "logf": None,
                         "last_lines": 0, "last_growth": time.time(),
                         "restarts": 0})

    shutting_down = False

    def _term(signum, frame):  # noqa: ANN001 - 信号回调固定签名
        nonlocal shutting_down
        shutting_down = True
        print(f"[supervise] 收到信号 {signum}，带走全部子进程", flush=True)
        for c in children:
            if c["proc"] and c["proc"].poll() is None:
                c["proc"].terminate()

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    def _spawn(c: dict) -> None:
        c["logf"] = open(c["log"], "ab", buffering=0)
        c["proc"] = subprocess.Popen(c["cmd"], cwd=REPO_ROOT,
                                     stdout=c["logf"],
                                     stderr=subprocess.STDOUT)
        c["last_lines"] = manifest_lines(c["manifest"])
        c["last_growth"] = time.time()
        print(f"[supervise] 分片{c['idx']} 已拉起 pid={c['proc'].pid}"
              f"（第 {c['restarts']} 次重启；清单 {c['manifest']}）", flush=True)

    print(f"[supervise] 守护启动：{n} 分片，stall>{args.stall_minutes}min 重启；"
          f"参数 {flow_args}", flush=True)
    for c in children:
        _spawn(c)

    while not shutting_down:
        time.sleep(args.check_seconds)
        for c in children:
            proc = c["proc"]
            if proc is None:
                continue
            rc = proc.poll()
            if rc is not None:
                print(f"[supervise] 分片{c['idx']} 自行退出 rc={rc}，"
                      f"5 秒后重拉", flush=True)
                time.sleep(5)
                c["restarts"] += 1
                _spawn(c)
                continue
            lines = manifest_lines(c["manifest"])
            if lines > c["last_lines"]:
                c["last_lines"], c["last_growth"] = lines, time.time()
            elif time.time() - c["last_growth"] > args.stall_minutes * 60:
                print(f"[supervise] 分片{c['idx']} 清单 "
                      f"{args.stall_minutes} 分钟零增长"
                      f"（停在 {c['last_lines']} 行），判定停摆，kill 重拉",
                      flush=True)
                proc.kill()
                proc.wait()
                c["restarts"] += 1
                _spawn(c)

    for c in children:
        if c["proc"] and c["proc"].poll() is None:
            c["proc"].terminate()
            c["proc"].wait()
        if c["logf"]:
            c["logf"].close()
    total_restarts = sum(c["restarts"] for c in children)
    print(f"[supervise] 退出（累计重启 {total_restarts} 次）", flush=True)


if __name__ == "__main__":
    main()
