#!/usr/bin/env python3
"""webgate 看门狗：searxng 端口探活，挂了自动 start.sh 重拉（2026-09-07）。

折源终态后全部召回归经 searxng，其可用性从"单源之一"升级为"全局依赖"——
守护等级对齐 flow 的 supervise（探端口 + 重拉 + 日志记档）。

用法：nohup python3 webgate/guard.py > /dev/null 2>&1 &
日志：webgate/log/guard.log（一行一事件，无事件零输出）。
"""

from __future__ import annotations

import os
import subprocess
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = "http://127.0.0.1:8080/"
CHECK_SECONDS = 30
COOLDOWN = 60          # 重拉失败后的最小重试间隔（防快循环）
LOG = os.path.join(HERE, "log", "guard.log")


def alive() -> bool:
    try:
        with urllib.request.urlopen(PROBE, timeout=5):
            return True
    except Exception:
        return False


def log(msg: str) -> None:
    os.makedirs(os.path.dirname(LOG), exist_ok=True)
    with open(LOG, "a") as f:
        f.write(f"[guard] {time.strftime('%F %T')} {msg}\n")


def main() -> None:
    log("守护启动（探活 {}s，重拉冷却 {}s）".format(CHECK_SECONDS, COOLDOWN))
    last_try = 0.0
    while True:
        time.sleep(CHECK_SECONDS)
        if alive():
            continue
        if time.time() - last_try < COOLDOWN:
            continue
        last_try = time.time()
        log("探活失败，执行 start.sh 重拉")
        try:
            r = subprocess.run(["bash", os.path.join(HERE, "start.sh")],
                               capture_output=True, text=True, timeout=120)
            log(f"start.sh rc={r.returncode}: {r.stdout.strip()[-200:]}")
        except Exception as exc:  # pylint: disable=broad-except
            log(f"start.sh 执行异常: {exc!r}")


if __name__ == "__main__":
    main()
