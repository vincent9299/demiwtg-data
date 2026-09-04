"""flow 守护：托管 flow 子进程，落盘停摆自动重启（2026-08-21 定案；2026-09-04·六 起托管目标由 chain 换为 flow，看门狗语义不变）。

背景：旧链一夜三次静默停摆（HTTP 连接池半读连接复用死锁，
net.get_client 已禁 keep-alive 缓解，但不保证根绝）。停摆特征是
「进程活着、CPU 近零、清单不再增长」，无人值守时只能靠重启恢复；
--skip-covered 保证重启续跑无损，故自愈代价仅一个启动周期。

职责边界：只做「看门狗」——拉起 flow、盯清单行数、停摆则 kill+重拉；
不做任何数据处理，flow 的全部参数原样透传。

用法（与 flow 相同参数，前面可加守护参数）：
    python3 -m supervise [--stall-minutes 12] [--flow-log logs/flow_x.log] \
        -- --top-n 2 --vlm-concurrency 48 ... --instances state/collect/xxx.json
"""

import argparse
import os
import signal
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_MANIFEST = os.path.join(
    REPO_ROOT, "datasets", "demiwtg", "meta", "metadata.jsonl")


def manifest_lines(path: str) -> int:
    try:
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0


def main() -> None:
    p = argparse.ArgumentParser(description="flow 停摆自愈守护")
    p.add_argument("--stall-minutes", type=int, default=12,
                   help="清单零增长超过该时长判定停摆并重启（默认 12）")
    p.add_argument("--check-seconds", type=int, default=60,
                   help="清单采样间隔（默认 60）")
    p.add_argument("--manifest", default=DEFAULT_MANIFEST,
                   help="被盯的清单文件（默认 demiwtg 湖 metadata.jsonl）")
    p.add_argument("--flow-log", "--chain-log", default=None,
                   help="flow 子进程 stdout/stderr 追加目标（默认 logs/supervised_flow.log）")
    p.add_argument("flow_args", nargs=argparse.REMAINDER,
                   help="'--' 之后原样透传给 data_pipeline.flow")
    args = p.parse_args()

    flow_args = [a for a in args.flow_args if a != "--"]
    if not flow_args:
        p.error("需要在 '--' 后给出 flow 的完整参数")
    flow_log = args.flow_log or os.path.join(REPO_ROOT, "logs", "supervised_flow.log")

    cmd = [sys.executable, "-m", "data_pipeline.flow", *flow_args]
    logf = open(flow_log, "ab", buffering=0)

    child: subprocess.Popen = None  # type: ignore
    shutting_down = False

    def _term(signum, frame):  # noqa: ANN001 - 信号回调固定签名
        nonlocal shutting_down
        shutting_down = True
        print(f"[supervise] 收到信号 {signum}，带走子进程", flush=True)
        if child and child.poll() is None:
            child.terminate()

    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)

    print(f"[supervise] 守护启动：stall>{args.stall_minutes}min 重启；"
          f"flow 日志 {flow_log}", flush=True)
    restarts = 0
    while not shutting_down:
        child = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=logf,
                                 stderr=subprocess.STDOUT)
        print(f"[supervise] flow 已拉起 pid={child.pid}（第 {restarts} 次重启）",
              flush=True)
        last_lines = manifest_lines(args.manifest)
        last_growth = time.time()
        while not shutting_down:
            time.sleep(args.check_seconds)
            rc = child.poll()
            if rc is not None:
                print(f"[supervise] flow 自行退出 rc={rc}，5 秒后重拉", flush=True)
                time.sleep(5)
                break
            lines = manifest_lines(args.manifest)
            if lines > last_lines:
                last_lines, last_growth = lines, time.time()
            elif time.time() - last_growth > args.stall_minutes * 60:
                print(f"[supervise] 清单 {args.stall_minutes} 分钟零增长"
                      f"（停在 {last_lines} 行），判定停摆，kill 重拉", flush=True)
                child.kill()
                child.wait()
                restarts += 1
                break
        else:
            break
    if child and child.poll() is None:
        child.terminate()
        child.wait()
    print("[supervise] 退出", flush=True)


if __name__ == "__main__":
    main()
