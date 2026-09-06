"""data_pipeline 文本引擎 + 文本检索算子（docs 线，2026-09-06 落地）。

行契约：
- 种子行（读，与图像线同源）：{name, query, lang, …}
- 页面候选行（TextSearchStage 产）：{name, page_url, title, authority,
  query}——authority: wiki | serp（合成材料权重与溯源用）
"""

from __future__ import annotations

import asyncio
import html as _html

import httpx

from demiflow.collect import net
from demiflow.collect.search import register_engine
from demiflow.data.plan import StreamStage

# ---------------------------------------------------------------------------
# 文本引擎（SearchEngine 协议实现；与图像引擎同注册表不同路由表）
# ---------------------------------------------------------------------------


class WikiEntityEngine:
    """wikipedia 实体检索（REST v1，免爬免 key）：search/page 取候选页。

    结构化、权威度高（合成材料 authority=wiki）；zh/en 按种子语言对位。
    """

    name = "wiki_entity"
    k_cap = 4

    limits = net.SourceLimits(rate=1.5, concurrency=2)
    dl_limits = net.SourceLimits(rate=1.0, concurrency=2)   # 占位（文本引擎无下载闸）

    _SEARCH = "/w/rest.php/v1/search/page"

    async def search(self, query: str, k: int, *, lang: str = "en",
                     client=None) -> list:
        k = min(k, self.k_cap)
        base = (f"https://zh.wikipedia.org" if lang == "zh"
                else "https://en.wikipedia.org")
        from operators.search import API_UA
        resp = await net.request(
            self.name, "GET", base + self._SEARCH, client=client,
            params={"q": query, "limit": str(k)},
            headers={"User-Agent": API_UA})
        out = []
        for p in (resp.json().get("pages") or [])[:k]:
            key = p.get("key")
            if not key:
                continue
            out.append({
                "page_url": f"{base}/wiki/{key}",
                "title": p.get("title"),
                "snippet": _html.unescape(re.sub(
                    r"<[^>]+>", "", p.get("excerpt") or ""))[:300],
                "authority": "wiki",
            })
        return out


class SearxngGeneralEngine:
    """SearXNG 通用 SERP（webgate categories=general）：广度补充。"""

    name = "searxng_general"
    k_cap = 6

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
                    "safesearch": 1, "engines": "google, bing"})
        out = []
        for r in (resp.json().get("results") or [])[:k]:
            url = r.get("url")
            if not url or not str(url).startswith(("http://", "https://")):
                continue
            out.append({"page_url": str(url), "title": r.get("title"),
                        "snippet": (r.get("content") or "")[:300],
                        "authority": "serp"})
        return out


register_engine(WikiEntityEngine())
register_engine(SearxngGeneralEngine())
net.register_limits({e.name: e.limits for e in
                    (WikiEntityEngine(), SearxngGeneralEngine())})

# 文本路由：两语言都双引擎（wiki 权威打底，SERP 补广）
TEXT_ROUTE_TABLE = {
    "zh": ["wiki_entity", "searxng_general"],
    "latin": ["wiki_entity", "searxng_general"],
}


import re  # noqa: E402 （WikiEntityEngine 的 snippet 清洗用）


_DISAMBIG_MARKS = ("可以指", "可以是指", "消歧义", "disambiguation")
_TRUSTED_URL = ("wikipedia.org", "baike.baidu.com", "zhihu.com",
                "britannica.com")


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
