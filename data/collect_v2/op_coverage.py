"""collect_v2 覆盖算子：链上第一个算子，启动期从真相清单现算实例覆盖度，
产出「该跳过的已覆盖实例」判定，位于 op_seed 之前。

契约（.qoder/handoff_collect_v2.md §6.1 + 2026-08-21 用户拍板）：
- 只读 datasets/demiwtg/meta/metadata.jsonl 的 instances 字段现场聚合
  （消费者现场聚合模式，同 viewer/build_viewer 与 taxonomy/mount_map），
  **纯内存、不落盘**——不新增任何状态文件；
- 输出 {实例名: 图数}；图数 ≥ 阈值的实例由链在启动期整体跳过
  （连检索与下载都不发生，比 sink 撞车去重更早一步）；
- 改名残留的旧实例名算未覆盖 → 会重采，安全：sha 撞车由 sink 兜底不双写，
  改名实体按新知识重判本就是期望行为；
- 只拦「实例整体已覆盖」这一种浪费；**跨实例的 sha 撞车**（A 角色的图被
  B 角色的检索词召回）实例级视角看不见，仍由 sink 撞车去重兜底；
- 本算子只产判定，不改写实例本身（只选择不加工）；不做阈值把关语义的
  质量判断，阈值是链的策略参数（--skip-covered），默认 0=不启用；
- 覆盖口径可带质量门（2026-08-23 用户拍板）：只数合格行（quality>=阈值且/
  或 identity=true），「有图但全不合格」的实例按 0 图对待继续采；缺
  quality 字段的行（存量迁移残留）一律按不合格计。
"""

from __future__ import annotations

import json
import os


def load_coverage(dataset_dir: str,
                  manifest_name: str = "metadata.jsonl",
                  min_quality: float = 0,
                  require_identity: bool = False) -> dict:
    """扫主清单现算 {实例名: 合格图数}（坏行容忍，与读端口径一致）。

    合格 = quality >= min_quality（缺 quality 字段按不合格）且（若启用）
    identity 为 True；两门全关时退化为数全部行（旧口径）。
    31 万行量级启动扫描秒级可接受；清单不存在返回空（全新数据湖）。
    """
    manifest = os.path.join(dataset_dir, "meta", manifest_name)
    counts: dict[str, int] = {}
    if not os.path.exists(manifest):
        return counts
    with open(manifest, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if min_quality > 0:
                q = rec.get("quality")
                if not isinstance(q, (int, float)) or q < min_quality:
                    continue
            if require_identity and rec.get("identity") is not True:
                continue
            for name in rec.get("instances") or []:
                counts[name] = counts.get(name, 0) + 1
    return counts


def filter_uncovered(insts: list, counts: dict,
                     min_images: int) -> tuple:
    """按覆盖度过滤实例列表，返回 (未覆盖实例, 跳过数)。

    保原表序，只选择不改写；min_images=0 时不过滤（跳过数恒 0）。
    """
    if min_images <= 0:
        return insts, 0
    kept = [i for i in insts
            if counts.get(i.get("name") or "", 0) < min_images]
    return kept, len(insts) - len(kept)
