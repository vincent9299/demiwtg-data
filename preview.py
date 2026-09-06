"""采集结果轻量预览：manifest 行 → 自包含 HTML（缩略图 base64 内嵌，
单文件免服务免依赖，浏览器直接打开；适合小批验证与夜跑抽查）。

用法：python3 preview.py [--dataset DIR] [--manifest metadata.jsonl]
                         [--limit 200] [--out preview.html]
默认 dataset = 共享存储数据湖；输出文件写在当前目录（本地盘）。
"""

from __future__ import annotations

import argparse
import base64
import html
import io
import json
import os
from collections import defaultdict

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATASET = "/lhcos-data/demiwtg-data/datasets/demiwtg"


def thumb_b64(blob_path: str, max_edge: int = 240):
    """缩略图 JPEG base64；读不出返回 None（占位灰块）。"""
    from PIL import Image
    try:
        with Image.open(blob_path) as im:
            im = im.convert("RGB")
            im.thumbnail((max_edge, max_edge))
            buf = io.BytesIO()
            im.save(buf, "JPEG", quality=60)
            return base64.b64encode(buf.getvalue()).decode()
    except Exception:  # noqa: BLE001 - 坏图占位
        return None


def esc(x) -> str:
    return html.escape(str(x if x is not None else "—"))


def card(row: dict, dataset: str) -> str:
    b64 = thumb_b64(os.path.join(dataset, row.get("path") or ""))
    img = (f'<img src="data:image/jpeg;base64,{b64}">'
           if b64 else '<div class="bad">读图失败</div>')
    q = row.get("quality")
    ann = f"q={q:g} kb={row.get('kb_match')}" if q is not None else "未标注"
    links = []
    if row.get("content_url"):
        links.append(f'<a href="{esc(row["content_url"])}">源图</a>')
    if row.get("landing_url"):
        links.append(f'<a href="{esc(row["landing_url"])}">来源页</a>')
    return f'''<div class="card">{img}
<div class="meta"><b>{esc(row.get("source"))}</b> · {esc(row.get("queries", {}).get(row["instances"][0], ""))}<br>
{row.get("width")}×{row.get("height")} · {int(row.get("size_bytes") or 0)//1024}KB · {row.get("sha256", "")[:8]}<br>
{ann} · {" ".join(links)}</div></div>'''


def main() -> None:
    p = argparse.ArgumentParser(description="采集结果自包含 HTML 预览")
    p.add_argument("--dataset", default=DEFAULT_DATASET)
    p.add_argument("--manifest", default="metadata.jsonl")
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--out", default="preview.html")
    args = p.parse_args()

    path = os.path.join(args.dataset, "meta", args.manifest)
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    rows = rows[:args.limit]

    by_inst: dict = defaultdict(list)
    for r in rows:
        for name in r.get("instances") or [""]:
            by_inst[name].append(r)

    annotated = sum(1 for r in rows if r.get("quality") is not None)
    stats = (f"{len(rows)} 行 · {len({r['sha256'] for r in rows})} 唯一图 · "
             f"{len(by_inst)} 实例 · 来源 {sorted({r.get('source') for r in rows})} · "
             f"已标注 {annotated}")
    sections = []
    for name, rs in by_inst.items():
        cards = "\n".join(card(r, args.dataset) for r in rs)
        sections.append(f"<h2>{esc(name)}<span>{len(rs)} 张</span></h2>"
                        f'<div class="grid">{cards}</div>')

    doc = f'''<!doctype html><html><head><meta charset="utf-8">
<title>采集预览 {esc(args.manifest)}</title><style>
body{{font:13px/1.5 -apple-system,sans-serif;margin:20px;background:#fafafa}}
h1{{font-size:18px}} h2{{font-size:15px;margin:24px 0 8px}}
h2 span{{color:#888;font-weight:normal;margin-left:8px;font-size:12px}}
.grid{{display:flex;flex-wrap:wrap;gap:10px}}
.card{{background:#fff;border:1px solid #e5e5e5;border-radius:8px;
overflow:hidden;width:250px}}
.card img{{width:250px;height:187px;object-fit:cover;display:block;background:#eee}}
.bad{{width:250px;height:187px;display:flex;align-items:center;
justify-content:center;color:#aaa}}
.meta{{padding:6px 8px;font-size:11px;color:#555}}
a{{color:#06c;text-decoration:none}}
</style></head><body>
<h1>采集预览 · {esc(args.manifest)}</h1><p>{stats}</p>
{chr(10).join(sections)}
</body></html>'''
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(doc)
    print(f"[preview] {stats}\n[preview] 写出 {args.out}"
          f"（{os.path.getsize(args.out)//1024} KB，自包含可直接打开）")


if __name__ == "__main__":
    main()
