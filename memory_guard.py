#!/usr/bin/env python3
"""内存哨兵：docs 线浏览器泄漏的运行时护栏（2026-09-08 宕机复盘产物）。

机理：/proc/meminfo 每分钟采样；CommitRatio > 120% 或 MemFree < 500MB
→ pkill flow（supervise ≤60s 自动重拉新进程，skip-covered 续跑无损）。
冷却 10 分钟防连环杀。日志写本地盘（不用 cosfs——颠簸期 FUSE 写入
本身就可能挂起，哨兵不能依赖它）。

根因修复在 demiflow PageCrawler 定量回收（每 50 页重建浏览器）；
本哨兵是兜底层——泄漏若再积累到危险水位，先泄压保机器再查因。

运行：常驻（systemd/nohup/persistent bgp 均可）。
"""

from __future__ import annotations

import subprocess
import time

LOG = "/home/ubuntu/demi/demiwtg-data/logs/memory_guard.log"
INTERVAL_S = 60          # 采样间隔
COOLDOWN_S = 600         # 泄压冷却
TH_COMMIT = 1.20         # Committed_AS / (MemTotal+SwapTotal)
TH_MEMFREE_MB = 500.0


def _log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} [memory_guard] {msg}\n"
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass                     # 本地盘写失败也绝不阻塞哨兵


def _meminfo() -> dict:
    out = {}
    with open("/proc/meminfo", encoding="ascii") as f:
        for line in f:
            k, _, v = line.partition(":")
            out[k] = int(v.strip().split()[0])    # kB
    return out


def main() -> None:
    _log(f"哨兵启动：{INTERVAL_S}s/次，阈值 commit>{TH_COMMIT:.0%} 或 "
         f"memfree<{TH_MEMFREE_MB:.0f}MB，冷却 {COOLDOWN_S}s")
    last_kill = 0.0
    while True:
        try:
            m = _meminfo()
            total = m.get("MemTotal", 0) + m.get("SwapTotal", 0)
            ratio = m.get("Committed_AS", 0) / total if total else 0.0
            free_mb = m.get("MemFree", 0) / 1024.0
            breach = ratio > TH_COMMIT or free_mb < TH_MEMFREE_MB
            if breach and time.time() - last_kill > COOLDOWN_S:
                _log(f"泄压触发：commit={ratio:.0%} memfree={free_mb:.0f}MB"
                     f" → pkill flow（supervise 将自动重拉）")
                subprocess.run(["pkill", "-f", "[p]ython -m flow"],
                               check=False)
                last_kill = time.time()
            elif breach:
                _log(f"高压但冷却中：commit={ratio:.0%} "
                     f"memfree={free_mb:.0f}MB")
        except Exception as exc:                 # noqa: BLE001
            _log(f"采样异常: {exc!r}")
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
