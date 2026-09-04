"""edit（编辑）赛道评测样本抽样：从本仓数据集分层抽评测用图。

抽样源是 data_pipeline 权威清单 metadata.jsonl（质量字段 quality/identity/focus
在它身上，不在 images.jsonl）；实例 → 树分支经 taxonomy/mount_map.py 现算
（挂载关系不落盘的解耦契约，AGENTS.md 1.5）。

流水线：
1. 过滤：--filter 一条 duckdb SQL WHERE，直接作用于清单 read_json_auto
   （默认质量门 + 编辑适配门：quality>=8 且 identity 且 focus>=7 且
   短边>=512；可用 --no-edit-gate 关掉编辑适配门只留质量门）；
2. 分层：按树 L1/L2 分支配额（sqrt 平滑防头部刷屏），每实例至多
   --per-instance 张；
3. 排除集：三个赛道子模块（vlm/t2i/edit）各自 data/samples.jsonl 的
   sha256 全量剔除（跨赛道防同一张图重复出题 → 判分模型背答案），
   可用 --exclude 覆盖。

每次运行全量重抽：直接抽 --n 张，覆盖写 --out 清单（数据分布可能变化，
不维护增量）；--img-dir 里本脚本产出的旧样本拷贝（^\\d{4}_ 命名）落盘前
清除，编号从 0001 起。所有路径均为参数，默认取仓库标准布局。

产物（评测结果数据，在 edit/ 下且不入 git，默认路径）：
    benchmark/edit/data/samples.jsonl     # 本次抽样的权威清单（覆盖写）
    benchmark/edit/data/images/<nnnn>_<实例>_<sha8>.<ext>   # 图片拷贝

用法：
    python3 benchmark/edit/eval_sample.py [--n 1000] [--seed 20260823] [--dry-run]
    python3 benchmark/edit/eval_sample.py --filter "quality >= 8 AND identity = true"
"""

from __future__ import annotations

import argparse
import json
import math
import random
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path

SUB_DIR = Path(__file__).resolve().parent                    # edit/
BENCH_ROOT = SUB_DIR.parent                                  # benchmark/
REPO_ROOT = BENCH_ROOT.parent                                # 仓库根
sys.path.insert(0, str(REPO_ROOT / "data"))    # taxonomy 包已迁至 data/taxonomy/

def load_mount_map(taxonomy_path):                                   # noqa: E402
    """挂载聚合最小副本（原 taxonomy.mount_map；curation 已迁出本仓）。"""
    import json
    from collections import defaultdict
    with open(taxonomy_path, encoding="utf-8") as f:
        tree = json.load(f).get("tree") or {}
    mounts = defaultdict(list)

    def walk(n):
        path = n.get("path", "")
        for nm in n.get("instances") or []:
            if isinstance(nm, dict):
                nm = nm.get("name")
            nm = str(nm).strip() if nm is not None else ""
            if nm and path and path not in mounts[nm]:
                mounts[nm].append(path)
        for ch in n.get("children") or []:
            walk(ch)

    walk(tree)
    return dict(mounts)

META_DIR = REPO_ROOT / "datasets" / "demiwtg" / "meta"
OUT_DIR = SUB_DIR / "data"

# 默认排除集：三赛道样本清单（跨赛道防重复出题；不存在的清单自动跳过）
DEFAULT_EXCLUDES = [
    BENCH_ROOT / sub / "data" / "samples.jsonl"
    for sub in ("vlm", "t2i", "edit")
]

OWN_IMG_RE = re.compile(r"^\d{4}_")    # 本脚本产出的样本拷贝命名前缀


def clean_name(s: str, n: int = 40) -> str:
    """实例名 → 安全文件名片段（实例名可能含 / 等非法字符）。"""
    s = re.sub(r'[\\/:*?"\'<>|\s]+', '_', str(s)).strip('_')
    return s[:n] or 'noname'


# 默认过滤 = 质量门 + 编辑适配门（主体显著 + 分辨率够编辑）
MIN_EDGE_EDIT = 512    # 编辑适配：短边下限
FOCUS_EDIT = 7         # 编辑适配：主体显著度下限
DEFAULT_FILTER = ("quality >= 8 AND identity = true "
                  f"AND focus >= {FOCUS_EDIT} "
                  f"AND least(width, height) >= {MIN_EDGE_EDIT}")


def load_exclude_shas(manifests: list) -> set:
    shas = set()
    for p in manifests:
        if not p.exists():
            continue
        with p.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    shas.add(json.loads(line)["sha256"])
                except (json.JSONDecodeError, KeyError):
                    continue
    return shas


def branch_of(mounts: dict, instance: str) -> tuple:
    """实例 → (L1, L2) 分支；未挂载归 ("未挂载", "未挂载")。"""
    paths = mounts.get(instance) or []
    for p in paths:
        segs = [s.strip() for s in p.split(" / ")]
        # path 从根『demiwtg』起算（前缀精简后域直挂）：[根, 域, L2, ...]
        if len(segs) >= 3:
            return segs[1], segs[2]
    return ("未挂载", "未挂载")


def load_pool(manifest: Path, taxonomy: Path, filter_sql: str) -> list:
    """过滤条件后的候选行（每行带分支标注）。"""
    import duckdb
    mounts = load_mount_map(str(taxonomy))
    con = duckdb.connect()
    res = con.execute(
        f"SELECT * FROM read_json_auto('{manifest}', "
        f"maximum_object_size=1073741824) WHERE {filter_sql}")
    cols = [d[0] for d in res.description]
    pool = []
    for row in res.fetchall():
        rec = dict(zip(cols, row))
        insts = rec.get("instances") or []
        if not insts:
            continue
        l1, l2 = branch_of(mounts, insts[0])
        rec["_branch"] = (l1, l2)
        rec["_mount_paths"] = mounts.get(insts[0]) or []
        pool.append(rec)
    return pool


def stratified_pick(pool: list, n: int, per_instance: int,
                    seed: int) -> list:
    """按 (L1, L2) 分支配额抽样：配额 ∝ sqrt(分支候选数)，实例限张数。"""
    by_branch = defaultdict(list)
    for rec in pool:
        by_branch[rec["_branch"]].append(rec)

    weights = {b: math.sqrt(len(v)) for b, v in by_branch.items()}
    total_w = sum(weights.values())
    # 最大余数法：配额总和恰为 n（分支数 > n 时小分支自然分到 0）
    raw = {b: n * w / total_w for b, w in weights.items()}
    quota = {b: int(raw[b]) for b in raw}
    rem = n - sum(quota.values())
    for b in sorted(quota, key=lambda x: -(raw[x] - int(raw[x]))):
        if rem <= 0:
            break
        quota[b] += 1
        rem -= 1

    rng = random.Random(seed)
    picked, inst_cnt = [], defaultdict(int)
    for b in sorted(by_branch):                      # 分支序确定性
        if quota[b] <= 0:
            continue
        cands = by_branch[b].copy()
        rng.shuffle(cands)
        for rec in cands:
            if len([p for p in picked if p["_branch"] == b]) >= quota[b]:
                break
            inst = rec["instances"][0]
            if inst_cnt[inst] >= per_instance:
                continue
            inst_cnt[inst] += 1
            picked.append(rec)
    return picked


def next_index(img_dir: Path) -> int:
    if not img_dir.exists():
        return 1
    mx = 0
    for p in img_dir.iterdir():
        head = p.name[:4]
        if head.isdigit():
            mx = max(mx, int(head))
    return mx + 1


def clean_img_dir(img_dir: Path) -> int:
    """清除旧样本拷贝（只删本脚本 ^\\d{4}_ 命名产物，不碰其他文件）。"""
    if not img_dir.exists():
        return 0
    n = 0
    for p in img_dir.iterdir():
        if p.is_file() and OWN_IMG_RE.match(p.name):
            p.unlink()
            n += 1
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=1000,
                    help="抽取张数（默认 1000；每次全量重抽，覆盖写清单）")
    ap.add_argument("--filter", default=DEFAULT_FILTER,
                    help="候选过滤 SQL（duckdb WHERE 片段，作用于清单 read_json_auto；"
                         "默认质量门+编辑适配门，可用 --no-edit-gate 只留质量门）")
    ap.add_argument("--no-edit-gate", action="store_true",
                    help="关闭编辑适配门，只留质量门 "
                         "'quality >= 8 AND identity = true'")
    ap.add_argument("--per-instance", type=int, default=2)
    ap.add_argument("--seed", type=int, default=20260823)
    ap.add_argument("--manifest", type=Path, default=META_DIR / "metadata.jsonl",
                    help="抽样源清单（默认 demiwtg meta/metadata.jsonl）")
    ap.add_argument("--taxonomy", type=Path, default=META_DIR / "taxonomy.json",
                    help="标签树（分支分层用，默认 meta/taxonomy.json）")
    ap.add_argument("--blobs", type=Path,
                    default=REPO_ROOT / "datasets" / "demiwtg" / "blobs",
                    help="图片字节区（拷贝源）")
    ap.add_argument("--out", type=Path, default=OUT_DIR / "samples.jsonl",
                    help="样本清单输出路径（覆盖写）")
    ap.add_argument("--img-dir", type=Path, default=OUT_DIR / "images",
                    help="图片拷贝目录（落盘前清除旧样本拷贝）")
    ap.add_argument("--exclude", type=Path, nargs="*", default=None,
                    help="排除清单（可多个；默认三赛道 data/samples.jsonl；"
                         "空列表 = 不排除）")
    ap.add_argument("--dry-run", action="store_true", help="只打印配额不拷贝")
    args = ap.parse_args()

    filter_sql = ("quality >= 8 AND identity = true"
                  if args.no_edit_gate else args.filter)

    excludes = DEFAULT_EXCLUDES if args.exclude is None else args.exclude
    exclude = load_exclude_shas(excludes)
    print(f"排除集：{len(excludes)} 份清单共 {len(exclude)} 个历史样本 sha",
          flush=True)

    pool = load_pool(args.manifest, args.taxonomy, filter_sql)
    print(f"过滤后候选（WHERE {filter_sql}）：{len(pool)} 行", flush=True)
    pool = [r for r in pool if r["sha256"] not in exclude]
    print(f"剔除排除集后：{len(pool)} 行", flush=True)

    picked = stratified_pick(pool, args.n, args.per_instance, args.seed)
    branches = defaultdict(int)
    for r in picked:
        branches[r["_branch"]] += 1
    print(f"实抽 {len(picked)} 张（--n {args.n}），覆盖 {len(branches)} 个分支：")
    for b in sorted(branches, key=lambda x: -branches[x]):
        print(f"  {b[0]} / {b[1]}: {branches[b]}")

    if args.dry_run:
        print("[dry-run] 不落盘")
        return

    args.img_dir.mkdir(parents=True, exist_ok=True)
    n_clean = clean_img_dir(args.img_dir)
    if n_clean:
        print(f"已清除旧样本拷贝：{n_clean} 个", flush=True)
    idx = next_index(args.img_dir)
    n_copied = 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fout:   # 覆盖写（全量重抽）
        for rec in picked:
            sha = rec["sha256"]
            ext = rec.get("ext") or "jpg"
            inst = rec["instances"][0]
            name = f"{idx:04d}_{clean_name(inst)}_{sha[:8]}.{ext}"
            src = args.blobs / sha[:2] / f"{sha}.{ext}"
            if not src.exists():
                print(f"  [warn] blob 缺失跳过: {sha[:12]}", file=sys.stderr)
                continue
            shutil.copy2(src, args.img_dir / name)
            w, h = rec.get("width") or 0, rec.get("height") or 0
            out = {
                "sample_id": f"{idx:04d}",
                "image": f"{args.img_dir.name}/{name}",
                "sha256": sha,
                "instance": inst,
                "instances": rec["instances"],
                "l1": rec["_branch"][0],
                "l2": rec["_branch"][1],
                "mount_paths": rec["_mount_paths"],
                "quality": rec.get("quality"),
                "focus": rec.get("focus"),
                "kb_match": rec.get("kb_match"),
                "width": w,
                "height": h,
                "caption": rec.get("caption", ""),
                "queries": rec.get("queries", {}),
            }
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            idx += 1
            n_copied += 1
    print(f"\n完成：{n_copied} 张 -> {args.out}")


if __name__ == "__main__":
    main()
