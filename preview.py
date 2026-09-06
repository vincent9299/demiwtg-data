"""采集湖预览：概念列表 → 概念详情（图墙/原图/知识文档）→ 文档正文。

serve 模式（默认）：本机 HTTP 服务，DuckDB 实时过滤，免拷数据。
export 模式：离线自包含单文件（无网络场景备用）。

用法：
    python3 preview.py serve [--dataset DIR] [--manifest 'image*.jsonl']
                             [--port 8901] [--bind 127.0.0.1]
    python3 preview.py export [--out preview.html] [--limit 200]

笔记本访问（免开防火墙）：
    ssh -N -L 8901:localhost:8901 ubuntu@<机器>
    浏览器打开 http://localhost:8901
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
from urllib.parse import parse_qs, quote, urlparse

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATASET = "/lhcos-data/demiwtg-data/datasets/demiwtg"
# 配额表来源（在场则列表页展示 目标张数/进度条）
BATCH_PATH = "/lhcos-data/demiwtg-data/concepts_batch_200.json"

_CSS = """body{font:13px/1.5 -apple-system,sans-serif;margin:16px;background:#fafafa}
form{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0 12px;align-items:center}
input{font:inherit;padding:3px 6px;border:1px solid #ccc;border-radius:5px}
button{padding:4px 12px;cursor:pointer}
#stats{color:#666;margin:4px 0 12px;font-size:12px}
table{border-collapse:collapse;background:#fff}
td,th{border:1px solid #eee;padding:5px 10px;text-align:left;font-size:12px}
.bar{background:#eee;border-radius:4px;width:120px;height:10px;overflow:hidden;
display:inline-block;vertical-align:middle;margin-right:6px}
.bar i{display:block;height:100%;background:#5b8def}
.ok i{background:#4caf50}
h2{font-size:15px;margin:20px 0 8px}
h2 span{color:#888;font-weight:normal;margin-left:8px;font-size:12px}
.grid{display:flex;flex-wrap:wrap;gap:10px}
.card{background:#fff;border:1px solid #e5e5e5;border-radius:8px;overflow:hidden;width:250px}
.card img{width:250px;height:187px;object-fit:cover;display:block;background:#eee}
.meta{padding:6px 8px;font-size:11px;color:#555}
a{color:#06c;text-decoration:none}
.badge{font-size:10px;padding:1px 7px;border-radius:8px;color:#fff;
margin-right:6px;vertical-align:2px}
.b-wiki{background:#5b8def}.b-serp{background:#8a8a8a}
.b-curated{background:#4caf50}
.doc-card{background:#fff;border:1px solid #e5e5e5;border-radius:8px;
padding:10px 12px;margin:8px 0;max-width:880px}
.passage{border-top:1px dashed #eee;margin-top:8px;padding-top:8px}
.passage p{margin:0 0 6px;font-size:12px;color:#333;white-space:pre-wrap}
.pimgs{display:flex;gap:6px;flex-wrap:wrap}
.pimgs img{width:200px;height:140px;object-fit:cover;border-radius:6px}
pre.doc{background:#fff;border:1px solid #e5e5e5;border-radius:8px;padding:12px;
white-space:pre-wrap;max-width:860px;font-size:12px}
"""


def esc(x) -> str:
    return html.escape(str(x if x is not None else "—"))


def page_html(body: str, subtitle: str = "") -> str:
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f'<title>采集湖预览</title><style>{_CSS}</style></head><body>'
            f'<h1>采集湖预览 <span style="font-size:12px;color:#888">'
            f'{esc(subtitle)}</span></h1>{body}</body></html>')


def card(row: dict, img_url: str) -> str:
    q = row.get("quality")
    ann = f"q={q:g} kb={row.get('kb_match')}" if q is not None else "未标注"
    name = (row.get("concepts") or [row.get("name")])[0]
    query = (row.get("queries") or {}).get(name, "")
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


def thumb_jpeg(blob_path: str, max_edge: int = 300):
    from PIL import Image
    with Image.open(blob_path) as im:
        im = im.convert("RGB")
        im.thumbnail((max_edge, max_edge))
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=65)
        return buf.getvalue()


def load_quota() -> dict:
    """批任务在场则取 {概念: 目标张数}（gate 兜底口径）。"""
    try:
        doc = json.load(open(BATCH_PATH, encoding="utf-8"))
    except OSError:
        return {}
    qmap = {"strict": 40, "category": 20, "relevance": 10}
    out = {}
    for c in doc.get("concepts") or []:
        gate = c.get("gate") or "category"
        out[c["name"]] = int((c.get("collect") or {}).get(
            "min_images", qmap.get(gate, 20)) or 20)
    return out


def run_serve(args) -> None:
    import glob as _glob
    import duckdb

    meta_dir = os.path.join(args.dataset, "meta")
    manifest_glob = os.path.join(meta_dir, args.manifest)
    if not _glob.glob(manifest_glob):
        raise SystemExit(f"清单不存在：{manifest_glob}")
    docs_glob = os.path.join(meta_dir, args.docs_manifest)
    has_docs = bool(_glob.glob(docs_glob))
    quota = load_quota()
    root = args.blob_root or args.dataset   # blob/pages 解析根（共享存储）

    lock = threading.Lock()
    con = duckdb.connect()

    def q(sql: str, params: tuple = ()):
        with lock:
            return con.execute(sql, params).fetchall()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            u = urlparse(self.path)
            p = parse_qs(u.query)
            try:
                if u.path in ("/", "/index.html"):
                    self._list(p)
                elif u.path == "/concept":
                    self._concept(p)
                elif u.path == "/blob":
                    self._blob(p)
                elif u.path == "/page":
                    self._page(p)
                else:
                    self.send_error(404)
            except Exception as exc:  # noqa: BLE001
                self.send_error(500, str(exc))

        def _send(self, body: bytes, ctype: str):
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _list(self, p):
            kw = (p.get("q", [""])[0] or "").strip()
            cond, params = "", ()
            if kw:
                cond, params = "WHERE list_contains(concepts, ?)", (kw,)
            rows = q(f"""SELECT concepts[1] AS c, count(*) AS n,
                        count(DISTINCT source) AS s,
                        sum(CASE WHEN quality IS NOT NULL THEN 1 ELSE 0 END)
                        FROM read_json_auto('{manifest_glob}') {cond}
                        GROUP BY c""", params)
            docs_cnt = {}
            if has_docs:
                for c, n in q(f"""SELECT concepts[1] AS c, count(*) FROM
                              read_json_auto('{docs_glob}') {cond}
                              GROUP BY c""", params):
                    docs_cnt[c] = n
                for c in list(docs_cnt):
                    if not any(r[0] == c for r in rows):
                        rows.append((c, 0, 0, 0))      # text-only 概念补位
            rows.sort(key=lambda r: (-docs_cnt.get(r[0], 0) - r[1], r[0]))
            body = [('<form>概念 <input name="q" value="' + esc(kw)
                     + '" size="12"><button>检索</button> <a href="/">全部</a>'
                     '　<small>点击概念进入图墙</small></form>')]
            total = sum(r[1] for r in rows)
            met = sum(1 for r in rows if r[0] in quota and r[1] >= quota[r[0]])
            body.append(f'<div id="stats">概念 {len(rows)} 个 · {total} 行'
                        + (f' · 配额达标 {met}' if quota else "")
                        + "（实时，清单 " + esc(args.manifest) + "）</div>")
            trs = []
            for c, n, srcs, ann in rows:
                tgt = quota.get(c)
                if tgt:
                    pct = min(100, n * 100 // tgt)
                    col = " ok" if pct >= 100 else ""
                    shown = (f'<span class="bar{col}"><i style="width:{pct}%">'
                             f"</i></span>{n}/{tgt}")
                else:
                    shown = str(n)
                trs.append(f"<tr><td><a href='/concept?name={quote(c)}'>"
                           f"{esc(c)}</a></td><td>{shown}</td>"
                           f"<td>{docs_cnt.get(c, 0)}</td>"
                           f"<td>{srcs}</td><td>{ann or 0}</td></tr>")
            body.append("<table><tr><th>概念</th><th>图 已采/目标</th>"
                        "<th>docs</th><th>图源数</th><th>已标注</th></tr>"
                        + "".join(trs) + "</table>")
            self._send(page_html("".join(body), "· 概念列表").encode(),
                       "text/html; charset=utf-8")

        def _concept(self, p):
            name = p.get("name", [""])[0]
            src_f = (p.get("source", [""])[0] or "").strip()
            conds, params = ["list_contains(concepts, ?)"], [name]
            if src_f:
                conds.append("source = ?")
                params.append(src_f)
            where = " AND ".join(conds)
            rows = q(f"""SELECT * FROM read_json_auto('{manifest_glob}')
                         WHERE {where} ORDER BY quality DESC NULLS LAST""",
                     tuple(params))
            cols = [d[0] for d in q(
                f"DESCRIBE SELECT * FROM read_json_auto('{manifest_glob}')")]
            recs = [dict(zip(cols, r)) for r in rows]
            try:
                src_stats = q(f"""SELECT source, count(*) FROM
                             read_json_auto('{manifest_glob}') WHERE {where}
                             GROUP BY source ORDER BY 2 DESC""", tuple(params))
            except Exception:  # noqa: BLE001 - 无图像概念
                src_stats = []
            tgt = quota.get(name)
            docs = []
            if has_docs:
                docs = q(f"""SELECT url, path, authority, title, n_images
                             FROM read_json_auto('{docs_glob}')
                             WHERE list_contains(concepts, ?)
                             ORDER BY (authority='wiki') DESC, url""",
                         (name,))
            cards = "\n".join(
                card(r, f"/blob?path={esc(r.get('path') or '')}&w=280")
                for r in recs)
            src_links = " · ".join(
                f"<a href='/concept?name={quote(name)}&source={esc(s)}'>"
                f"{esc(s)}({n})</a>" for s, n in src_stats) or "—"
            ann_n = sum(1 for r in recs if r.get("quality") is not None)
            docs_html = ""
            if docs:
                from operators.page import extract_passages
                items = []
                for url, path, authority, title, n_imgs in docs:
                    full_md = os.path.join(root, path or "")
                    passages = []
                    if path and os.path.exists(full_md):
                        passages = extract_passages(
                            open(full_md, encoding="utf-8",
                                 errors="replace").read())
                    badge = (f'<span class="badge b-{esc(authority)}">'
                             f"{esc(authority)}</span>")
                    segs = []
                    for pa in passages:
                        imgs = "".join(
                            f'<a href="/blob?path={esc(im.get("blob_path"))}"'
                            f' target="_blank"><img loading="lazy" src='
                            f'"/blob?path={esc(im.get("blob_path"))}&w=200">'
                            f"</a>"
                            for im in pa.get("images") or []
                            if im.get("blob_path"))
                        segs.append(
                            f'<div class="passage"><p>{esc(pa["text"])}</p>'
                            + (f'<div class="pimgs">{imgs}</div>' if imgs
                               else "") + "</div>")
                    items.append(
                        f'<div class="doc-card">{badge} '
                        f'<b>{esc(title or url)}</b> '
                        f'<a href="{esc(url)}">原文</a> · '
                        f'{len(passages)} 段 / {n_imgs} 图'
                        + ("".join(segs) if segs else
                           '<p style="color:#999">（正文读取失败）</p>')
                        + "</div>")
                docs_html = (f"<h2>知识文档<span>{len(docs)} 篇"
                             f"</span></h2>" + "".join(items))
            head = (f'<a href="/">← 概念列表</a><h2>{esc(name)}<span>'
                    f'图 {len(recs)} 张' + (f" · 目标 {tgt}" if tgt else "")
                    + f' · 已标注 {ann_n}</span></h2>'
                    f'<p style="font-size:12px;color:#666">来源：{src_links}</p>')
            self._send(page_html(head + docs_html
                                 + f'<div class="grid">{cards}</div>',
                                 f"· {name}").encode(),
                       "text/html; charset=utf-8")

        def _page(self, p):
            rel = (p.get("path", [""])[0] or "").lstrip("/")
            full = os.path.join(root, rel)
            if (not rel.startswith("pages/") or ".." in rel
                    or not os.path.exists(full)):
                self.send_error(404)
                return
            text = open(full, encoding="utf-8", errors="replace").read()
            self._send(page_html(
                f'<pre class="doc">{esc(text)}</pre>', "· 文档正文").encode(),
                "text/html; charset=utf-8")

        _MIME = {".png": "image/png", ".jpg": "image/jpeg",
                 ".jpeg": "image/jpeg", ".gif": "image/gif",
                 ".webp": "image/webp", ".bmp": "image/bmp"}
        _thumb_sem = threading.Semaphore(4)

        def _blob(self, p):
            rel = (p.get("path", [""])[0] or "").lstrip("/")
            full = os.path.join(root, rel)
            if (not rel.startswith("blobs/") or ".." in rel
                    or not os.path.exists(full)):
                self.send_error(404)
                return
            w = p.get("w", [None])[0]
            if w:
                with Handler._thumb_sem:
                    data = thumb_jpeg(full, min(int(w), 400))
                self._send(data, "image/jpeg")
                return
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
                pass

    srv = ThreadingHTTPServer((args.bind, args.port), Handler)
    print(f"[preview] http://{args.bind}:{args.port} ← {manifest_glob}"
          f"（实时：概念列表→图墙→原图）\n[preview] 笔记本："
          f"ssh -N -L {args.port}:localhost:{args.port} ubuntu@<本机>"
          f" 后开 http://localhost:{args.port}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[preview] 退出")


def run_export(args) -> None:
    rows = []
    import glob as _glob
    for path in sorted(_glob.glob(os.path.join(
            args.dataset, "meta", args.manifest))):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    rows = rows[:args.limit]
    by_concept: dict = {}
    for r in rows:
        for name in r.get("concepts") or [r.get("name")]:
            by_concept.setdefault(name, []).append(r)
    sections = []
    for name, rs in by_concept.items():
        cards = []
        for r in rs:
            try:
                data = thumb_jpeg(os.path.join(args.dataset,
                                               r.get("path") or ""), 240)
                uri = ("data:image/jpeg;base64,"
                       + base64.b64encode(data).decode())
                cards.append(card(r, uri))
            except Exception:  # noqa: BLE001
                continue
        sections.append(f"<h2>{esc(name)}<span>{len(cards)} 张</span></h2>"
                        f'<div class="grid">{"".join(cards)}</div>')
    stats = f"{len(rows)} 行 · {len(by_concept)} 概念（离线快照）"
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(page_html("".join(sections), stats))
    print(f"[preview] {stats}\n[preview] 写出 {args.out}"
          f"（{os.path.getsize(args.out) // 1024} KB）")


def main() -> None:
    ap = argparse.ArgumentParser(description="采集湖预览")
    sub = ap.add_subparsers(dest="mode")
    sp = sub.add_parser("serve")
    sp.add_argument("--dataset", default=DEFAULT_DATASET)
    sp.add_argument("--manifest", default="image*.jsonl",
                    help="图像清单 glob（默认分片+合并件全量）")
    sp.add_argument("--docs-manifest", default="docs*.jsonl",
                    help="docs 清单 glob（默认 docs*.jsonl）")
    sp.add_argument("--blob-root", default="",
                    help="blob/页面根（共享存储；缺省=--dataset）")
    sp.add_argument("--port", type=int, default=8901)
    sp.add_argument("--bind", default="127.0.0.1")
    ep = sub.add_parser("export")
    ep.add_argument("--dataset", default=DEFAULT_DATASET)
    ep.add_argument("--manifest", default="image*.jsonl")
    ep.add_argument("--blob-root", default="")
    ep.add_argument("--limit", type=int, default=200)
    ep.add_argument("--out", default="preview.html")
    args = ap.parse_args()
    if args.mode == "export":
        run_export(args)
    else:
        run_serve(args)


if __name__ == "__main__":
    main()
