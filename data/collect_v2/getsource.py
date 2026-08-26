"""collect_v2 域路由算子（getsource）：seed → (seed, 源) 配对，位于 op_seed 与 op_search 之间。

契约（.qoder/handoff_collect_v2.md §4.4 + 2026-08-20 用户拍板）：
- 域路由从「种子流入口侧」实体化为独立算子（用户拍板：单独加算子 getsource）；
- **配置表驱动**：路由规则是数据（ROUTE_TABLE）不是散落逻辑，加源只改表；
- 2026-08-20 拍板更新：虚拟角色向新源对**所有 seed**（zh/latin）全量投递，
  无召回即认缺不做语言特判；新源：anilist/mal/pixiv/bing_images/
  yandex_images/deviantart；fandom 全局端点被 Cloudflare 拦，挂起待拍板；
- inaturalist（需生物类实例标识）本期无路由依据，挂起；
- 2026-08-20 国内爬虫三源（huaban_api/toutiao/so360，旧系统迁移）只打 zh 种子
  （中文站内检索，拉丁词无召回价值），与 baidu 同列；
- 防错配不杀候选：路由只决定「这个 seed 打哪些源」，不对候选做任何筛选。
"""

from __future__ import annotations

from collect_v2.op_search import Seed

# 新源（虚拟角色向）：对所有语言的 seed 全量投递（用户拍板）；
# anilist/mal 不在此列：专场已爬过（mal 3218 行/anilist 1436 行），
# 当前全量专场非角色实例占多数，两角色库基本 404 认缺，纯浪费投递（2026-08-22 拍板）。
_CHAR_SOURCES = ["bing_images", "yandex_images"]

# 代理慢源只打 latin 种子（2026-08-22 拍板）：虚拟角色已全覆盖、本场全是真实实体，
# zh 种子打这两源≈纯认缺（连角色专场时命中率都极低），恢复语言对位；
# latin 行保留西文粉丝图通道。日后虚拟角色专场重启时把两源加回 _CHAR_SOURCES。
# deviantart 2026-08-22 再摘（用户拍板）：近 1 小时仅 59 张、多次 10 分钟零产出，
# 半死源还占投递对数与代理流量；日后复活再加回本名单。
_LATIN_ONLY_SOURCES = ["pixiv"]

# 国内爬虫档中文源：只打 zh 种子（中文站，同 baidu）
_CN_CRAWLER_SOURCES = ["huaban_api", "toutiao", "so360"]

# 域路由表：lang → 源列表（顺序即投递顺序，无权重语义）
#
# 2026-08-22 代理 192.168.10.109:10808 修复复通（五端点实测全通），
# 还原 2026-08-21 临时摘除的代理源；同期按拍板剔除 anilist/mal（专场已爬，
# 全量专场无命中价值）；pixiv/deviantart 移入 _LATIN_ONLY_SOURCES（zh 行摘除，
# 恢复语言对位，见上方注释）。日后虚拟角色专场重启时再把角色源加回。
ROUTE_TABLE: dict = {
    "zh": ["baidu", "wikimedia_zh"] + _CN_CRAWLER_SOURCES + _CHAR_SOURCES,
    "latin": ["wikimedia"] + _CHAR_SOURCES + _LATIN_ONLY_SOURCES,
}


def route(seed: Seed) -> list:
    """单 seed 路由：返回 [(seed, 源), ...]，源序按表内顺序。

    未登记的 lang 返回空列表（认缺，不回落不放宽）。
    """
    return [(seed, source) for source in ROUTE_TABLE.get(seed.lang, [])]
