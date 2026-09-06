"""采集结果活预览：本机 HTTP 服务，DuckDB 实时过滤 + 缩略图按需渲染。

不用拷数据：浏览器直连（推荐 SSH 隧道），刷新即见湖的最新状态。

用法：
    python3 preview.py serve [--port 8901] [--bind 127.0.0.1]   # 默认
    python3 preview.py export [--out preview.html] [--limit 200] # 离线单文件（旧形态）

笔记本访问（免开防火墙）：
    ssh -N -L 8901:localhost:8901 ubuntu@43.160.250.196
    浏览器打开 http://localhost:8901
（或在安全组放行端口后 --bind 0.0.0.0 直连公网 IP）

过滤参数（URL 查询串，页面表单同款）：instance= / source= / min_q= /
limit=（默认 100）。
"""

from __future__ import annotations

import argparse
import base64
import html
import io
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATASET = "/lhcos-data/demiwtg-data/datasets/demiwtg"


# ---------------------------------------------------------------------------
# 行渲染（serve 与 export 共用）
# ---------------------------------------------------------------------------

def esc(x) -> str:
    return html.escape(str(x if x is not None else "—"))


def card(row: dict, img_url: str) -> str:
    q = row.get("quality")
    ann = f"q={q:g} kb={row.get('kb_match')}" if q is not None else "未标注"
    query = (row.get("queries") or {}).get(row["instances"][0], "")
    links = []
    if row.get("content_url"):
        links.append(f'<a href="{esc(row["content_url"])}">源图</a>')
    if row.get("landing_url"):
        links.append(f'<a href="{esc(row["landing_url"])}">来源页</a>')
    href = img_url.split("&w=")[0] if "&w=" in img_url else img_url
    return (f'<div class="card"><a href="{href}" target="_blank">'
            f'<img loading="lazy" src="{img_url}"></a>'
            f'<div class="meta"><b>{esc(row.get("source"))}</b> · {esc(query)}<br>'
            f'{row.get("width")}×{row.get("height")} · '
            f'{int(row.get("size_bytes") or 0) // 1024}KB · '
            f'{row.get("sha256", "")[:8]}<br>'
            f'{ann} · {" ".join(links)}</div></div>')


PAGE = '''<!doctype html><html><head><meta charset="utf-8">
<title>采集湖预览</title><style>
body{{font:13px/1.5 -apple-system,sans-serif;margin:16px;background:#fafafa}}
form{{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0 4px;align-items:center}}
input,select{{font:inherit;padding:3px 6px;border:1px solid #ccc;border-radius:5px}}
button{{padding:4px 12px;cursor:pointer}}
#stats{{color:#666;margin:4px 0 12px;font-size:12px}}
h2{{font-size:15px;margin:20px 0 8px}}
h2 span{{color:#888;font-weight:normal;margin-left:8px;font-size:12px}}
.grid{{display:flex;flex-wrap:wrap;gap:10px}}
.card{{background:#fff;border:1px solid #e5e5e5;border-radius:8px;overflow:hidden;
width:250px}}
.card img{{width:250px;height:187px;object-fit:cover;display:block;background:#eee}}
.meta{{padding:6px 8px;font-size:11px;color:#555}}
a{{color:#06c;text-decoration:none}}
</style></head><body>
<h1>采集湖预览</h1>
<form method="get">
实例 <input name="instance" value="{instance}" size="10" placeholder="包含匹配">
来源 <input name="source" value="{source}" size="9" placeholder="如 baidu">
min_q <input name="min_q" value="{min_q}" size="4">
行数 <input name="limit" value="{limit}" size="4">
<button>过滤</button> <a href="/">重置</a>
</form>
<div id="stats">{stats}</div>
{sections}
</body></html>'''


# ---------------------------------------------------------------------------
# serve 模式：DuckDB 实时过滤 + 缩略图端点
# ---------------------------------------------------------------------------

def thumb_jpeg(blob_path: str, max_edge: int = 300):
    from PIL import Image
    with Image.open(blob_path) as im:
        im = im.convert("RGB")
        im.thumbnail((max_edge, max_edge))
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=65)
        return buf.getvalue()


def run_serve(args) -> None:
    import duckdb
    manifest = os.path.join(args.dataset, "meta", args.manifest)
    if not os.path.exists(manifest):
        raise SystemExit(f"清单不存在：{manifest}")
    lock = threading.Lock()
    con = duckdb.connect()

    def q(sql: str, params: tuple = ()):
        with lock:
            return con.execute(sql, params).fetchall()

    def rows_where(p) -> tuple:
        conds, params = ["1=1"], []
        if p.get("instance"):
            conds.append("list_contains(instances, ?)")
            params.append(f"%{p['instance'][0]}%")
        if p.get("source"):
            conds.append("source = ?")
            params.append(p["source"][0])
        if p.get("min_q"):
            conds.append("quality >= ?")
            params.append(float(p["min_q"][0]))
        limit = min(int(p.get("limit", ["100"])[0] or 100), 2000)
        return " AND ".join(conds), tuple(params), limit

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # 静默访问日志
            pass

        def do_GET(self):
            u = urlparse(self.path)
            p = parse_qs(u.query)
            try:
                if u.path in ("/", "/index.html"):
                    self._index(p)
                elif u.path == "/blob":
                    self._blob(p)
                else:
                    self.send_error(404)
            except Exception as exc:  # noqa: BLE001
                self.send_error(500, str(exc))

        def _index(self, p):
            where, params, limit = rows_where(p)
            rows = q(f"""SELECT * FROM read_json_auto('{manifest}')
                         WHERE {where} LIMIT {limit}""", params)
            cols = [d[0] for d in q(
                f"DESCRIBE SELECT * FROM read_json_auto('{manifest}')")]
            total, uniq, insts = q(f"""
                SELECT count(*), count(DISTINCT sha256),
                       count(DISTINCT instances[1])
                FROM read_json_auto('{manifest}') WHERE {where}""", params)[0]
            recs = [dict(zip(cols, r)) for r in rows]
            by_inst = {}
            for r in recs:
                for name in r.get("instances") or [""]:
                    by_inst.setdefault(name, []).append(r)
            sections = "\n".join(
                f'<h2>{esc(n)}<span>{len(rs)} 张</span></h2>'
                + '<div class="grid">' + "\n".join(
                    card(r, f"/blob?path={esc(r.get('path') or '')}&w=280")
                    for r in rs)
                + "</div>"
                for n, rs in by_inst.items())
            stats = (f"匹配 {total} 行 · {uniq} 唯一图 · {insts} 实例"
                     f"（清单 {args.manifest}，实时）")
            doc = PAGE.format(
                instance=esc(p.get("instance", [""])[0]),
                source=esc(p.get("source", [""])[0]),
                min_q=esc(p.get("min_q", [""])[0]),
                limit=esc(p.get("limit", [str(limit)])[0]),
                stats=stats, sections=sections)
            body = doc.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        _MIME = {".png": "image/png", ".jpg": "image/jpeg",
                 ".jpeg": "image/jpeg", ".gif": "image/gif",
                 ".webp": "image/webp", ".bmp": "image/bmp"}
        _thumb_sem = threading.Semaphore(4)   # 并发解码上限（防同时解大图）

        def _blob(self, p):
            rel = (p.get("path", [""])[0] or "").lstrip("/")
            full = os.path.join(args.dataset, rel)
            if (not rel.startswith("blobs/") or ".." in rel
                    or not os.path.exists(full)):
                self.send_error(404)
                return
            w = p.get("w", [None])[0]
            if w:      # 缩略图（网格快览）：PIL 现缩，限最大边与并发
                with Handler._thumb_sem:
                    data = thumb_jpeg(full, min(int(w), 400))
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "max-age=86400")
                self.end_headers()
                self.wfile.write(data)
                return
            # 原图：分块流式（服务器内存 O(64KB)，解码在浏览器端）
            size = os.path.getsize(full)
            self.send_response(200)
            self.send_header("Content-Type", Handler._MIME.get(
                os.path.splitext(rel)[1], "application/octet-stream"))
            self.send_header("Content-Length", str(size))
            self.send_header("Cache-Control", "max-age=86400")
            self.end_headers()
            try:
                with open(full, "rb") as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass    # 客户端提前断开（切页）是常态

    srv = ThreadingHTTPServer((args.bind, args.port), Handler)
    print(f"[preview] http://{args.bind}:{args.port} ← 清单 {manifest}（实时）"
          f"\n[preview] 笔记本访问：ssh -N -L {args.port}:localhost:{args.port} "
          f"ubuntu@<本机> 后开 http://localhost:{args.port}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[preview] 退出")


# ---------------------------------------------------------------------------
# export 模式：离线自包含单文件（旧形态，无网络场景备用）
# ---------------------------------------------------------------------------

def run_export(args) -> None:
    rows = []
    with open(os.path.join(args.dataset, "meta", args.manifest),
              encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    rows = rows[:args.limit]
    by_inst: dict = {}
    for r in rows:
        for name in r.get("instances") or [""]:
            by_inst.setdefault(name, []).append(r)
    annotated = sum(1 for r in rows if r.get("quality") is not None)

    def b64_thumb(row):
        try:
            data = thumb_jpeg(os.path.join(args.dataset,
                                           row.get("path") or ""), 240)
            return "data:image/jpeg;base64," + base64.b64encode(data).decode()
        except Exception:  # noqa: BLE001 - 坏图跳卡片图
            return None

    sections = []
    for name, rs in by_inst.items():
        cards = []
        for r in rs:
            src_uri = b64_thumb(r)
            if not src_uri:
                continue
            cards.append(card(r, src_uri))
        sections.append(f"<h2>{esc(name)}<span>{len(cards)} 张</span></h2>"
                        f'<div class="grid">{"".join(cards)}</div>')
    stats = (f"{len(rows)} 行 · {len({r['sha256'] for r in rows})} 唯一图 · "
             f"{len(by_inst)} 实例 · 已标注 {annotated}（离线快照）")
    doc = PAGE.format(instance="", source="", min_q="", limit=args.limit,
                      stats=stats, sections="\n".join(sections))
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(doc)
    print(f"[preview] {stats}\n[preview] 写出 {args.out}"
          f"（{os.path.getsize(args.out) // 1024} KB）")



def main() -> None:
    ap = argparse.ArgumentParser(description="采集结果预览（serve=活服务 / export=离线）")
    sub = ap.add_subparsers(dest="mode")
    sp = sub.add_parser("serve")
    sp.add_argument("--dataset", default=DEFAULT_DATASET)
    sp.add_argument("--manifest", default="metadata.jsonl")
    sp.add_argument("--port", type=int, default=8901)
    sp.add_argument("--bind", default="127.0.0.1",
                    help="默认仅本机（配 SSH 隧道）；公网直连需安全组放行")
    ep = sub.add_parser("export")
    ep.add_argument("--dataset", default=DEFAULT_DATASET)
    ep.add_argument("--manifest", default="metadata.jsonl")
    ep.add_argument("--limit", type=int, default=200)
    ep.add_argument("--out", default="preview.html")
    args = ap.parse_args()
    if args.mode == "export":
        run_export(args)
    else:
        run_serve(args)


if __name__ == "__main__":
    main()
