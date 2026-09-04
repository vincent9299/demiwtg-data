#!/usr/bin/env bash
# SearXNG JSON API 冒烟：images 类目真实检索一条，断言有结果。
set -euo pipefail
cd "$(dirname "$0")"

[ -f run/searxng.pid ] && kill -0 "$(cat run/searxng.pid)" 2>/dev/null \
  || { echo "[FAIL] 服务未在运行（先 bash start.sh）"; exit 1; }

# 用 duckduckgo images 单引擎（全引擎聚合慢、弱引擎偶发挂起会拖过 curl 超时）
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
curl -s -m 60 -o "$TMP" \
  'http://127.0.0.1:8080/search?q=%E6%85%95%E7%94%B0%E5%B3%AA%E9%95%BF%E5%9F%8E&categories=images&format=json&language=zh-CN&safesearch=1&engines=duckduckgo%20images'
.venv/bin/python - "$TMP" <<'EOF'
import json, sys
doc = json.load(open(sys.argv[1], encoding="utf-8"))
results = doc.get("results", [])
assert results, f"无结果：{json.dumps(doc, ensure_ascii=False)[:300]}"
engines = sorted({r.get("engine") for r in results if r.get("engine")})
with_img = [r for r in results if r.get("img_src")]
assert with_img, "结果里无 img_src（images 类目异常）"
print(f"[PASS] JSON 检索 {len(results)} 条；引擎 {engines}；"
      f"首条 img_src={with_img[0]['img_src'][:60]}…")
EOF
echo "冒烟通过"
