#!/usr/bin/env python3
"""生成 vlm/results_review.ipynb（vlm 赛道打分/评估结果审阅，纯只读展示）。

用法：python3 benchmark/vlm/gen_results_review.py   # 覆写同目录 results_review.ipynb
"""
import json
from pathlib import Path

OUT_NB = Path(__file__).resolve().parent / "results_review.ipynb"

MD_INTRO = """# vlm 赛道 · 打分/评估结果审阅

- 题库：默认读 `bagel/results/wkbench_v0/questions.jsonl`（runner 快照，混有三赛道题，只筛 task=vlm）；vlm 赛道重新出题后改 `QFILE` 指到本子模块题库
- 模型产出：`bagel/results/wkbench_v0/`（`responses_shard*.jsonl`；换目录改 `RESP_DIR`）
- **默认只展示 3 个冒烟 case**，全量跑完后把 `SHOW_ALL` 改 `True`
- 纯只读展示，零数据处理逻辑；多分片记录按 qid 合并（后读到的文件覆盖，即最新一次重跑结果优先）
- vlm 赛道判分代码尚未拆入本子模块（协议待定）；逐题打分结果出来后在此追加分析格

> 运行：菜单 Run All，或逐 Cell 运行。"""

CELL_LOAD = """import json, base64
from pathlib import Path

QFILE    = Path('../../bagel/results/wkbench_v0/questions.jsonl')   # ← 题库（快照或出题产物）
RESP_DIR = Path('../../bagel/results/wkbench_v0')                   # ← 模型产出目录
SMOKE_QIDS = ['0001-vlm-1', '0002-vlm-1', '0003-vlm-1']
SHOW_ALL = False     # 全量跑完后改 True 审阅全部已完成的题
TASK = 'vlm'

qs = {}
if QFILE.exists():
    for l in QFILE.open(encoding='utf-8'):
        q = json.loads(l)
        if q.get('task') == TASK:
            qs[q['qid']] = q
else:
    print(f'!! 题库不存在：{QFILE}（改 QFILE 指向已有题库快照）')

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

def _find_sample(rel):
    # 旧混编题库的 _sample_image 相对历史数据区，可能不存在；缺失时只提示
    p = EVAL_ROOT / rel
    return p

def show_vlm(q, r):
    sample = _find_sample(q['_sample_image'])
    html = ['<div style="display:flex;gap:12px;align-items:flex-start">']
    html.append(_img(sample, '样图'))
    html.append(f'<div style="flex:1"><b>题干</b><br>{q["stem"]}</div>')
    html.append('</div>')
    if r and r['ok']:
        resp = r.get('response', '').replace('<', '&lt;')
        html.append(f'<div style="background:#f6f8fa;padding:8px;border-radius:6px;margin:6px 0">'
                    f'<b>模型回答</b>（think+understanding，贪心）<br>'
                    f'<span style="white-space:pre-wrap">{resp}</span></div>')
    html.append(f'<details><summary>rubric 判分点 + 参考答案</summary>{_li(q.get("rubric"))}'
                f'<b>参考答案</b><br>{q.get("reference_answer", "")}</details>')
    return ''.join(html)

SHOW = sorted(recs) if SHOW_ALL else SMOKE_QIDS
for qid in SHOW:
    q = qs.get(qid)
    if q is None:
        print(f'{qid} 不在题库快照中，跳过')
        continue
    r = recs.get(qid)
    display(HTML(_head(q, r) + show_vlm(q, r)))
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
