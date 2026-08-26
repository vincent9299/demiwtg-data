#!/usr/bin/env python3
"""生成 t2i/results_review.ipynb（t2i 赛道打分/评估结果审阅，纯只读展示）。

用法：python3 benchmark/t2i/gen_results_review.py   # 覆写同目录 results_review.ipynb
"""
import json
from pathlib import Path

OUT_NB = Path(__file__).resolve().parent / "results_review.ipynb"

MD_INTRO = """# t2i 赛道 · 打分/评估结果审阅

- 题库：`t2i/data/synth_gen/questions.jsonl`（`eval_synthesize.py` 产物；换批次/快照改下方 `QFILE`）
- 模型产出：`bagel/results/wkbench_v0/`（`responses_shard*.jsonl` + `imgs/<qid>.png`；换目录改 `RESP_DIR`）
- 只展示 task=t2i 的题；**默认只展示 3 个冒烟 case**，全量跑完后把 `SHOW_ALL` 改 `True`
- 纯只读展示，零数据处理逻辑；多分片记录按 qid 合并（后读到的文件覆盖，即最新一次重跑结果优先）
- 逐题分数与聚合报表由 `eval_score.py` 产出（`data/scores_t2i.jsonl(.report.json)`），分析见 question_dev.ipynb

> 运行：菜单 Run All，或逐 Cell 运行。"""

CELL_LOAD = """import json, base64
from pathlib import Path

QFILE    = Path('data/synth_gen/questions.jsonl')   # ← 题库（出题产物或快照）
RESP_DIR = Path('../../bagel/results/wkbench_v0')   # ← 模型产出目录
SMOKE_QIDS = ['0001-t2i-1', '0002-t2i-1', '0003-t2i-1']
SHOW_ALL = False     # 全量跑完后改 True 审阅全部已完成的题
TASK = 't2i'

qs = {}
if QFILE.exists():
    for l in QFILE.open(encoding='utf-8'):
        q = json.loads(l)
        if q.get('task', TASK) == TASK:
            qs[q['qid']] = q
else:
    print(f'!! 题库不存在：{QFILE}（先跑 eval_synthesize.py 出题，或改 QFILE 指向已有题库快照）')

# 合并所有分片：按 qid 去重，后读到的文件覆盖（最新重跑结果优先）
recs = {}
if RESP_DIR.exists():
    for f in sorted(RESP_DIR.glob('responses_shard*.jsonl')):
        for l in f.open(encoding='utf-8'):
            if l.strip():
                r = json.loads(l)
                if r['qid'] in qs:
                    recs[r['qid']] = r
else:
    print(f'!! 产出目录不存在：{RESP_DIR}（模型还没跑，或改 RESP_DIR）')

n_ok = sum(1 for r in recs.values() if r['ok'])
print(f'{TASK} 题库 {len(qs)} 题 | 已有记录 {len(recs)}（ok {n_ok}）| 展示 {"全部" if SHOW_ALL else "冒烟 3 题"}')"""

CELL_HELPERS = """from IPython.display import display, HTML

IMG_W = 380

def _img(path, title=''):
    p = Path(path)
    if not p.exists():
        return f'<figure style="margin:4px"><figcaption style="color:#999">（{title} 图未产出）</figcaption></figure>'
    uri = base64.b64encode(p.read_bytes()).decode()
    return (f'<figure style="margin:4px;text-align:center">'
            f'<img src="data:image/png;base64,{uri}" style="max-width:{IMG_W}px;max-height:{IMG_W}px">'
            f'<figcaption style="font-size:12px;color:#666">{title}</figcaption></figure>')

def _li(items):
    out = []
    for p in items or []:
        if isinstance(p, dict):
            w = p.get('weight')
            txt = p.get('point') or p.get('check') or json.dumps(p, ensure_ascii=False)
            out.append(f'<li>{"[" + str(w) + "] " if w is not None else ""}{txt}</li>')
        else:
            out.append(f'<li>{p}</li>')
    return '<ul style="margin:4px 0">' + ''.join(out) + '</ul>' if out else ''

def _head(q, r):
    dims = '/'.join(q.get('probe_dims') or [])
    if r is None:
        badge = '⏳ 未跑到'
    elif r['ok']:
        badge = f'✅（{r.get("seconds", "?")}s）'
    else:
        badge = '❌ ' + str(r.get('error', ''))[:100]
    return (f'<h3 style="margin:16px 0 4px">{q["qid"]} · {q["task"]} · {q.get("difficulty", "")} '
            f'· {q.get("knowledge_dim", "")} · {dims}　{badge}</h3>')"""

CELL_SHOW = """EVAL_ROOT = Path('data')   # 样本图相对本子模块数据区（题库 _sample_image 字段）

def show_t2i(q, r):
    gen = RESP_DIR / r.get('image', '') if r and r['ok'] else RESP_DIR / 'imgs' / f'{q["qid"]}.png'
    html = [f'<b>生成提示</b>：{q["gen_prompt"]}',
            '<div style="display:flex;gap:12px">', _img(gen, '模型生成结果'), '</div>',
            f'<details><summary>隐式校验点（判分用）</summary>{_li(q.get("implicit_checks"))}</details>']
    return ''.join(html)

SHOW = sorted(recs) if SHOW_ALL else SMOKE_QIDS
for qid in SHOW:
    q = qs.get(qid)
    if q is None:
        print(f'{qid} 不在题库快照中，跳过')
        continue
    r = recs.get(qid)
    display(HTML(_head(q, r) + show_t2i(q, r)))
    display(HTML('<hr>'))"""


def cell(src, ctype="code"):
    return {
        "cell_type": ctype,
        "metadata": {},
        "source": src.splitlines(keepends=True),
        **({"outputs": [], "execution_count": None} if ctype == "code" else {}),
    }


nb = {
    "cells": [cell(MD_INTRO, "markdown"), cell(CELL_LOAD), cell(CELL_HELPERS), cell(CELL_SHOW)],
    "metadata": {
        "kernelspec": {"display_name": "demiwtg (env)", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

OUT_NB.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
print(f"written: {OUT_NB}")
