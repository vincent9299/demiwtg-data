#!/usr/bin/env bash
# 停 SearXNG（按 run/searxng.pid）。
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -f run/searxng.pid ]; then
  echo "[webgate] 无 pid 文件（未在运行？）"
  exit 0
fi
PID=$(cat run/searxng.pid)
if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  for i in $(seq 1 10); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 1
  done
  kill -0 "$PID" 2>/dev/null && kill -9 "$PID" || true
  echo "[webgate] 已停止 pid $PID"
else
  echo "[webgate] pid $PID 已不在（清掉 pid 文件）"
fi
rm -f run/searxng.pid
