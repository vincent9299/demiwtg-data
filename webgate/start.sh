#!/usr/bin/env bash
# SearXNG 启动（免 docker 裸进程，127.0.0.1:8080）。
# 首次运行自动：克隆上游源码 → 建 venv 装依赖 → 从模板生成 settings.yml（随机 secret）。
set -euo pipefail
cd "$(dirname "$0")"

PYSRC="${PYSRC:-https://pypi.tuna.tsinghua.edu.cn/simple}"   # 海外机可 PYSRC=https://pypi.org/simple

# 1) 上游源码（浅克隆，不入 git）
if [ ! -d searxng ]; then
  echo "[webgate] 克隆 searxng 上游源码……"
  git clone --depth 1 https://github.com/searxng/searxng.git searxng
fi

# 2) 独立 venv（不碰主仓 .venv）
if [ ! -x .venv/bin/python ]; then
  echo "[webgate] 建 venv 并安装依赖（首次约几分钟）……"
  python3 -m venv .venv
  .venv/bin/pip install -U pip -q -i "$PYSRC"
  .venv/bin/pip install -r searxng/requirements.txt -q -i "$PYSRC"
fi

# 3) 运行配置（模板 + 随机 secret；settings.yml 不入 git）
if [ ! -f settings.yml ]; then
  SECRET=$(.venv/bin/python -c "import secrets; print(secrets.token_hex(32))")
  sed "s/REPLACE_ME/$SECRET/" settings.yml.example > settings.yml
  echo "[webgate] 已生成 settings.yml（随机 secret_key）"
fi

# 4) 已在跑则跳过
if [ -f run/searxng.pid ] && kill -0 "$(cat run/searxng.pid)" 2>/dev/null; then
  echo "[webgate] 已在运行 pid $(cat run/searxng.pid)（127.0.0.1:8080）"
  exit 0
fi

mkdir -p log run
PYTHONPATH=searxng SEARXNG_SETTINGS_PATH="$PWD/settings.yml" \
  nohup .venv/bin/python -m searx.webapp > log/searxng.log 2>&1 &
echo $! > run/searxng.pid

# 5) 等就绪（端口可接受连接；SearXNG 无专用 healthz，探端口即可）
for i in $(seq 1 30); do
  if curl -s -o /dev/null -m 2 "http://127.0.0.1:8080/"; then
    echo "[webgate] 就绪 pid $(cat run/searxng.pid)（127.0.0.1:8080；冒烟: bash smoke.sh）"
    exit 0
  fi
  sleep 1
done
echo "[webgate] 启动超时，看日志: log/searxng.log" >&2
exit 1
