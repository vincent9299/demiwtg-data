"""data_pipeline 文本引擎 + 文本检索算子（docs 线，2026-09-06 落地）。

行契约：
- 种子行（读，与图像线同源）：{name, query, lang, …}
- 页面候选行（TextSearchStage 产）：{name, page_url, title, authority,
  query}——authority: wiki | serp（合成材料权重与溯源用）
"""

from __future__ import annotations

import asyncio

import httpx

from demiflow.collect import net
from demiflow.collect.search import register_engine
from demiflow.data.plan import StreamStage

# ---------------------------------------------------------------------------
# 文本引擎（SearchEngine 协议实现；与图像引擎同注册表不同路由表）
# ---------------------------------------------------------------------------

# docs 线终态（2026-09-07，与图像线同拍板）：关键词检索统一走 searxng
# 聚合网关——WikiEntityEngine（wikipedia REST 直连）退役，wiki 召回改由
# searxng general 池的 wikipedia/wikidata 引擎承接；SERP 引擎白名单
# （原 engines="google, bing"）放开为不限制（按用户拍板，死源/噪声源
# 由 relevance 门与后续 agent 化遥测剔除）。
# wiki 抓取分叉不受影响：PageFetchStage 按 URL 模式走 _wiki_extract，
# 与 authority 标签解耦（wikipedia.org 链接自动走 REST 直取路径）。



class SearxngGeneralEngine:
    """SearXNG 通用 SERP（webgate categories=general，引擎不设限）。

    自声明：wiki_upstream 元组（上游引擎名命中 → authority=wiki，与
    直连时代 wiki_entity 的权威口径连续；其余 → serp）。
    """

    name = "searxng_general"
    k_cap = 6
    wiki_upstream = ("wikipedia", "wikidata", "wikisearch")

    limits = net.SourceLimits(rate=6.0, concurrency=8)
    dl_limits = net.SourceLimits(rate=6.0, concurrency=8)

    _API = "http://127.0.0.1:8080/search"

    async def search(self, query: str, k: int, *, lang: str = "en",
                     client=None) -> list:
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._API, client=client,
            params={"q": query, "categories": "general", "format": "json",
                    "language": "zh-CN" if lang == "zh" else "en",
                    "safesearch": 1})
        out, seen = [], set()
        for r in (resp.json().get("results") or [])[:k]:
            url = r.get("url")
            if not url or not str(url).startswith(("http://", "https://")):
                continue
            if url in seen:
                continue
            seen.add(url)
            upstream = str(r.get("engine") or "")
            out.append({"page_url": str(url), "title": r.get("title"),
                        "snippet": (r.get("content") or "")[:300],
                        "authority": "wiki"
                        if any(w in upstream for w in self.wiki_upstream)
                        else "serp",
                        # 引擎溯源（源健康度口径）：general 池上游名归一
                        # 落 source，与图像线同款——死透判定不再冤杀 docs 源
                        "source": upstream.strip().lower().replace(" ", "_")
                        or "searxng_general"})
        return out


register_engine(SearxngGeneralEngine())
net.register_limits({"searxng_general": SearxngGeneralEngine.limits})

# 文本路由：docs 线单引擎（searxng general 池内含 wikipedia/wikidata
# 承接原 wiki_entity 的权威召回；语言参数对位 zh-CN/en 透传上游）
TEXT_ROUTE_TABLE = {
    "zh": ["searxng_general"],
    "latin": ["searxng_general"],
}


import re  # noqa: E402 （WikiEntityEngine 的 snippet 清洗用）


_DISAMBIG_MARKS = ("可以指", "可以是指", "消歧义", "disambiguation")
_TRUSTED_URL = ("wikipedia.org", "baike.baidu.com", "zhihu.com",
                "britannica.com")
# 电商/产品页（知识性低：商品列表/定价页混入 docs 的治理）
_COMMERCE_URL = ("amazon.", "ebay.", "taobao.", "jd.com", "tmall.",
                 "alibaba.", "walmart.", "aliexpress.", "bolt.eu")


def relevance_score(cand: dict, name: str, aliases: list) -> int:
    """候选页相关性打分（2026-09-06：SERP 词面混入治理）。

    - 标题精确=概念名 100 / 概念名为标题子串 70 / 别名子串 55；
    - 西文词重叠（≥3 字母词）每词 +6；
    - 权威站 +8；消歧义页（snippet/标题含消歧标记）-40（非目标知识，
      有更优候选时按排序自然沉底）；
    - 阈值 <18 丢弃（实测：词典/摄影类词面页 6 分、材质母类页 20 分留）。
    """
    title = (cand.get("title") or "").strip()
    score = 0
    if title == name:
        score = 100
    elif name in title:
        score = 70
    else:
        for a in aliases or []:
            a = (a or "").strip()
            if not a:
                continue
            # 短西文别名（<5 字母）子串匹配太松（"Bolt"→"Ride with
            # Bolt" 出租车页混入螺栓概念）。收紧：词边界命中且标题主部
            # 词数 <=2 才给分（别名是标题主体）；埋在长标题里的词面
            # 命中不给分。大小写不敏感。
            tl = title.lower()
            al = a.lower()
            if (re.fullmatch(r"[A-Za-z0-9 .\-]+", a)
                    and len(a.replace(" ", "")) < 5):
                head = title.split("|")[0]
                n_words = len(head.split())
                if re.search(rf"\b{re.escape(a)}\b", title, re.IGNORECASE) \
                        and n_words <= 2:
                    score = max(score, 45)
                    break
                continue
            if al in tl:
                score = max(score, 55)
                break
    toks = set(re.findall(r"[a-z]{3,}", title.lower()))
    want = set(re.findall(r"[a-z]{3,}",
                          (name + " " + " ".join(aliases or [])).lower()))
    score += 6 * len(toks & want)
    if any(d in cand.get("page_url", "") for d in _TRUSTED_URL):
        score += 8
    if any(d in cand.get("page_url", "") for d in _COMMERCE_URL):
        score -= 40                  # 商业页强降权（实测 55 分别名命中也压出局）
    if any(m in (cand.get("snippet") or "") for m in _DISAMBIG_MARKS) \
            or "消歧义" in title:
        score -= 40
    return score


class TextSearchStage(StreamStage):
    """文本检索算子：种子行 → 页面候选行集（相关性过滤 + 打分排序）。

    每 (种子,引擎) 取 top_n 页后做概念相关性过滤（词面混入治理：
    词典/无关行业页丢弃），按分数降序输出；消歧义页降权沉底。
    页级预算由下游 PageFetchStage 按概念计数控制。
    """

    label = "text_search"
    concurrency = 8
    queue_depth = 48
    catch = (net.InfraError, httpx.HTTPError)

    def __init__(self, per_query: int = 2, *, aliases_by_name: dict = None,
                 min_score: int = 18):
        self.per_query = per_query
        self._aliases = aliases_by_name or {}
        self.min_score = min_score

    async def __call__(self, seed: dict):
        from demiflow.collect.search import engine_search
        name = seed["name"]
        aliases = self._aliases.get(name, [])
        results = await asyncio.gather(*(
            engine_search(s, seed.get("query") or seed["name"], self.per_query,
                          lang=seed.get("lang", "zh"))
            for s in TEXT_ROUTE_TABLE.get(seed.get("lang", "zh"), [])),
            return_exceptions=True)
        out, seen = [], set()
        for rows in results:
            if isinstance(rows, BaseException):
                if isinstance(rows, self.catch):
                    continue
                raise rows
            for r in rows:
                u = r["page_url"]
                if u in seen:
                    continue
                seen.add(u)
                s = relevance_score(r, name, aliases)
                if s < self.min_score:
                    continue
                out.append({**r, "name": name, "score": s,
                            "query": seed.get("query") or seed["name"]})
        out.sort(key=lambda r: -r["score"])
        return out or None
