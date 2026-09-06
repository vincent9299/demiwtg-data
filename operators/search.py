"""collect_v2 检索算子：输入 (种子, 源) → 输出有界有序候选列表。

契约（.qoder/handoff_collect_v2.md §3.1 / §4.1）：
- 只收域路由之后的 (种子, 源) 对，本文件不做域路由；
- 输出按源原生相关度排序的候选列表，adapter 不重排、不筛选、不凑数；
- K 封顶不分页深翻：语义/爬虫源 ≤5，结构化源 10-20；
- 列表不足或为空原样返回，认缺是链层的事；
- adapter 只产结构化候选，不碰主清单；所有请求走 net.request。

数据流（2026-09-04·十 dict 行化）：demiflow 原生 dict 行流转，
各算子在行上追加自己的产出键（键契约见各算子 docstring），
不设独立的 Candidate/DownloadResult 类型。

候选 URL 契约（2026-08-21 用户拍板，数据用途定案为训练数据）：
- adapter 产 content_urls：**同一张图**的候选链接有序列表，按档位**大到小**
  （原图在前、压缩档殿后）；多数源天然单档即单元素列表，pixiv 等
  有多档的源产多元素；一图只落一档，绝不多档并存（浪费算力与存储）；
- 下载级按序依次试，首个成功即停，获胜链接记回 content_url（清单只写它）；
- 源知识（档位推导）留在 adapter，下载算子只认通用有序列表。

本期代表源：wikimedia_zh（官方 API 档）、baidu（爬虫档）。
wikimedia（英文/拉丁 seed 打同一 commons 端点，2026-08-20 拍板补注册）。
2026-08-20 新增六源（用户拍板，虚拟角色向）：anilist（GraphQL 只搜 Character）、
mal（角色搜索 HTML）、pixiv（ajax，regular 直取、R18 出口剔除）、bing_images、
yandex_images（SSR initialState 解析）、deviantart（RSS）；
fandom 全局搜索端点被 Cloudflare 拦，挂起待拍板。
2026-08-20 新增国内爬虫三源（旧系统迁移）：huaban_api（api.huaban.com JSON）、
toutiao（so.toutiao.com 全文本图链抽取）、so360（image.so.com j JSON）。
2026-09-04·八 引擎抽象上移：SearchEngine 协议/注册表/分派归 demiflow.collect.search，
本模块=检索算子全部：13 个引擎实现（自声明限速/下载闸）+ 域路由策略 +
SearchStage（路由+扇出+dict 行映射）。
2026-09-04 新增 searxng：自托管元搜索（data/webgate 模块，127.0.0.1:8080 JSON API），
聚合 google/bing/ddg 图片检索；检索本机直连、下载走代理的双闸见 net。
类型沿革：Seed/Item 已拆至 types.py、UA 常量已归位 net（2026-09-04 瑕疵修复）。
"""

from __future__ import annotations

import html as _html
import json
import re
from typing import Optional

import httpx

from demiflow.collect import net
import asyncio

from demiflow.collect import net
from demiflow.collect.search import (engine_search, is_connect_failure,
                                     register_engine)
from demiflow.data.plan import StreamStage

# 身份 UA（Wikimedia robot policy 要求可识别调用方与真实联系方式，
# 占位邮箱会被拦、真实仓库 URL 实测放行——用户拍板用仓库首页）
API_UA = ("collect-v2/0.1 (research image collection; "
          "https://github.com/vincent9299/demiwtg-data) httpx/0.28")

K_SEMANTIC = 5        # 语义检索源（wikimedia/搜索爬虫）K 封顶
K_STRUCTURED = 15     # 结构化源（inaturalist 等）K 封顶，后续源启用时生效


def _int_or_none(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None




# ---------------------------------------------------------------------------
# adapters
# ---------------------------------------------------------------------------

class WikimediaZhEngine:
    """维基共享资源（中文检索词）：打 commons.wikimedia.org 媒体库本体（旧系统验证过的端点），
    generator=search 只搜文件命名空间。"""

    name = "wikimedia_zh"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=2.0, concurrency=2, proxy=True)
    dl_limits = net.SourceLimits(rate=6.0, concurrency=8, proxy=True)
    _API = "https://commons.wikimedia.org/w/api.php"

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        params = {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": query,
            "gsrnamespace": "6",        # 文件命名空间
            "gsrlimit": str(k),
            "prop": "imageinfo|pageprops",
            "iiprop": "url|size|mime|extmetadata",
            "ppprop": "canonicalurl",
        }
        resp = await net.request(
            self.name, "GET", self._API, client=client,
            params=params, headers={"User-Agent": API_UA},
        )
        pages = (resp.json().get("query") or {}).get("pages") or {}
        # API 返回 dict，index 字段即相关度序；排序后取前 k
        ordered = sorted(pages.values(), key=lambda p: int(p.get("index", 0)))
        out: list[dict] = []
        for rank, page in enumerate(ordered[:k]):
            info = (page.get("imageinfo") or [{}])[0]
            ext = info.get("extmetadata") or {}

            def _ext(key: str) -> Optional[str]:
                v = ext.get(key)
                return v.get("value") if isinstance(v, dict) else None

            props = page.get("pageprops") or {}
            out.append({
                # commons API 直出即原图，单档
                "tiers": [info["url"]] if info.get("url") else [],
                "landing": props.get("canonicalurl") or info.get("descriptionurl"),
                "width": info.get("width"),
                "height": info.get("height"),
                "mime": info.get("mime"),
                "license": _ext("LicenseShortName"),
                "author": _ext("Artist"),
                "native": {
                    "page_title": page.get("title"),
                    "page_id": page.get("pageid"),
                    "mediatype": info.get("mediatype"),
                },
            })
        return out


class WikimediaEngine(WikimediaZhEngine):
    """维基共享资源（拉丁检索词）：与 zh 版同端点同参数——commons 搜索不限语言，
    独立 source 名只为 latin 路由/限速池/统计口径分立（2026-08-20 拍板补注册）。"""

    name = "wikimedia"


class BaiduEngine:
    """百度图片 acjson 接口（爬虫档）。

    纯业务经验来自旧系统（_reference/old_repo/collect/sources/baidu.py）：
    - 无会话 cookie 直接调 acjson 会被 antiFlag 拦截，需先预热拿 BAIDUID；
    - objURL 为混淆编码且解码不稳定，**不用**；优先 middleURL（明文 https、较大），
      回退 thumbURL/hoverURL（middleURL 已是可用最大档，候选单元素）；
    - acjson 的 width/height 是原图尺寸，与 middleURL 实际服务尺寸常不符，
      声明尺寸改从 URL 查询串 ?w=&h= 提取；
    - 非 JSON 应答按瞬态失败走 net 重试。
    """

    name = "baidu"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=10.0, concurrency=16, proxy=False)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=False)
    _API = "https://image.baidu.com/search/acjson"
    _HOME = "https://www.baidu.com/"
    _warmed = False

    async def _warmup(self, client: Optional[httpx.AsyncClient]) -> None:
        """预热拿会话 cookie（BAIDUID），失败不阻断，留给正式请求自行暴露。"""
        if BaiduEngine._warmed:
            return
        http = client or net.get_client()
        try:
            await http.get(self._HOME, headers={
                "User-Agent": net.BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9",
            })
        except httpx.HTTPError:
            return
        BaiduEngine._warmed = True

    @staticmethod
    def _pick_url(it: dict) -> Optional[str]:
        """middleURL 优先（明文且较大），回退 thumbURL/hoverURL；不用 objURL（加密）。"""
        for key in ("middleURL", "thumbURL", "hoverURL"):
            u = (it.get(key) or "").strip()
            if u and u.lower().startswith("http"):
                return u
        return None

    @staticmethod
    def _dims_from_url(url: str) -> tuple[Optional[int], Optional[int]]:
        """百度 CDN URL 查询串带真实服务尺寸（?w=500&h=889），比 acjson 原图尺寸可信。"""
        mw = re.search(r"[?&]w=(\d+)", url)
        mh = re.search(r"[?&]h=(\d+)", url)
        if mw and mh:
            return int(mw.group(1)), int(mh.group(1))
        return None, None

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        await self._warmup(client)
        params = {
            "tn": "resultjson_com",
            "ipn": "rj",
            "ct": "201326592",
            "fp": "result",
            "word": query,
            "queryWord": query,
            "rn": str(k),
            "pn": "0",
            "ie": "utf-8",
        }
        resp = await net.request(
            self.name, "GET", self._API, client=client,
            params=params,
            headers={
                "User-Agent": net.BROWSER_UA,
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Referer": "https://image.baidu.com/",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            # 反爬页/空壳应答：按瞬态失败上抛，由 net 分类重试语义兜住
            raise net.TransientExhaustedError(
                f"baidu 检索应答非 JSON（疑似反爬页）: {query}"
            ) from exc
        if data.get("antiFlag"):
            # 源明确拦截（如 "Forbid spider access"）：重试无意义，确定性失败认缺
            raise net.DeterministicError(
                f"baidu 反爬拦截: {data.get('message')!r} query={query}"
            )
        items = data.get("data") or []
        out: list[dict] = []
        seen: set[str] = set()
        rank = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            content_url = self._pick_url(it)
            if not content_url or content_url in seen:
                continue  # 无图址/重复的空壳条目，不筛选内容
            seen.add(content_url)
            w, h = self._dims_from_url(content_url)
            out.append({
                "tiers": [content_url],
                "landing": it.get("fromURL") or it.get("hoverURL"),
                "width": w,
                "height": h,
                "mime": None,   # 百度不返回 MIME，下载后由解码实测补齐
                "license": None,
                "author": None,
                "native": {
                    "from_page_title": it.get("fromPageTitleEnc"),
                    "from_url": it.get("fromURL"),
                    "orig_width": _int_or_none(it.get("width")),
                    "orig_height": _int_or_none(it.get("height")),
                    "size_bytes": _int_or_none(it.get("di")),
                },
            })
            rank += 1
            if rank >= k:
                break
        return out


# ---------------------------------------------------------------------------
# adapters（2026-08-20 新增六源，接口细节均实网探测实证）
# ---------------------------------------------------------------------------

class AniListEngine:
    """AniList GraphQL（官方、免鉴权）：只搜 Character（用户拍板，虚拟角色本体）。

    单次查询只取最优一条（GraphQL search 语义），多召回靠链层多种子/多源覆盖。
    """

    name = "anilist"
    k_cap = K_STRUCTURED
    limits = net.SourceLimits(rate=2.0, concurrency=2, proxy=True)
    dl_limits = net.SourceLimits(rate=6.0, concurrency=8, proxy=True)
    _API = "https://graphql.anilist.co"
    _QUERY = ("query($q:String){Character(search:$q){"
              "id name{full} image{large} siteUrl}}")

    async def search(self, query, k, *, lang="zh", client=None):
        resp = await net.request(
            self.name, "POST", self._API, client=client,
            json={"query": self._QUERY, "variables": {"q": query}},
            headers={"User-Agent": API_UA, "Content-Type": "application/json"},
        )
        char = (resp.json().get("data") or {}).get("Character")
        if not char or not (char.get("image") or {}).get("large"):
            return []   # 无命中 = 认缺
        return [{
            # AniList image.large 已是 API 提供的最大档，单档
            "tiers": [char["image"]["large"]],
            "landing": char.get("siteUrl"),
            "width": None,
            "height": None,
            "mime": None,
            "license": None,
            "author": None,
            "native": {"character_id": char.get("id"),
                       "character_name": (char.get("name") or {}).get("full")},
        }]


class MalEngine:
    """MyAnimeList 角色搜索 HTML 抓取（官方 API 需 client_id，不用）。

    character.php?q= 列表页结构（实网实测）：每行是绝对 URL 角色链接
    <a href="https://myanimelist.net/character/ID/Name">，链接**后**紧跟
    lazyload img（data-src 为 /r/42x62/ 规格缩略图，去规格前缀即 CDN 原图）。
    """

    name = "mal"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=10.0, concurrency=16, proxy=False)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=False)
    _SEARCH = "https://myanimelist.net/character.php"
    _RESIZED_RE = re.compile(r"/r/\d+x\d+/")
    _ROW_RE = re.compile(
        r'href="https://myanimelist\.net/character/(\d+)/([^"]+)".*?'
        r'data-src="([^"]+)"', re.S)

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._SEARCH, client=client,
            params={"q": query},
            headers={"User-Agent": net.BROWSER_UA,
                     "Accept-Language": "en"},
        )
        out: list[dict] = []
        seen: set[str] = set()
        for m in self._ROW_RE.finditer(resp.text):
            cid, cname, img = m.group(1), m.group(2), m.group(3)
            content_url = self._RESIZED_RE.sub("/", img, count=1)
            if content_url in seen:
                continue
            seen.add(content_url)
            out.append({
                # 去 /r/规格前缀后已是 CDN 原图，单档
                "tiers": [content_url],
                "landing": f"https://myanimelist.net/character/{cid}/{cname}",
                "width": None,
                "height": None,
                "mime": None,
                "license": None,
                "author": None,
                "native": {"character_id": int(cid),
                        "character_name": cname.replace("_", " ")},
            })
            if len(out) >= k:
                break
        return out


class PixivEngine:
    """Pixiv 搜索 ajax 接口（无需登录，必须带站内 Referer）。

    候选档位（2026-08-21 拍板，数据用途定案训练数据，原图优先）：
    [original.jpg, original.png, master1200] —— 原图扩展名无法从搜索接口
    得知，jpg/png 依次试错，全败回退 master1200（长边 1200 压缩档）；
    原图与压缩档同路径时间桶，作品被删时两档同死，回退只救扩展名猜错；
    ugoira（illustType=2）无静态原图（原件是 zip），只给 master1200 首帧。
    xRestrict>0 的 R18 作品在检索出口剔除（内容政策，非语义过滤）。
    """

    name = "pixiv"
    k_cap = K_STRUCTURED
    limits = net.SourceLimits(rate=10.0, concurrency=16, proxy=True)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=True)
    _API = "https://www.pixiv.net/ajax/search/artworks/"

    @staticmethod
    def _regular_url(thumb: str) -> str:
        """搜索接口 250 方图 → regular 尺寸 master（同路径规则推导，两种命名实测 200）：
        /c/250x250_80_a2/img-master/.../{id}_square1200.jpg        （单页作，无 _p0）
        /c/250x250_80_a2/img-master/.../{id}_p0_square1200.jpg     （多页作）
        → 去裁剪前缀 + _square1200 换 _master1200，_p0 有无原样保留（多页作取首页）。"""
        url = thumb.replace("/c/250x250_80_a2/", "/")
        return re.sub(r"_square1200(\.\w+)$", r"_master1200\1", url)

    @staticmethod
    def _candidate_urls(thumb: str, illust_type) -> list[str]:
        """产档位大到小的候选：[原图jpg, 原图png, master1200]。

        原图路径规则（实网实测）：img-master/→img-original/，去 _master1200 后缀，
        _p0 有无原样保留；扩展名服务端不告知，jpg 优先 png 殿后。
        custom-thumb/ 路径（AI 生成作，实网实测）无 img-original 对应档，
        推导规则不适用，custom1200 即该路径最大档，单元素候选。
        ugoira（illustType=2）原件是 zip 无静态原图，只给 master1200 首帧。"""
        master = PixivEngine._regular_url(thumb)
        if _int_or_none(illust_type) == 2 or "/custom-thumb/" in master:
            return [master]
        orig = re.sub(r"/img-master/", "/img-original/", master)
        orig = re.sub(r"_master1200\.\w+$", "", orig)
        return [orig + ".jpg", orig + ".png", master]

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._API + query, client=client,
            params={"lang": "en"},
            headers={"User-Agent": net.BROWSER_UA,
                     "Referer": "https://www.pixiv.net/",
                     "Accept": "application/json"},
        )
        data = resp.json()
        if data.get("error"):
            raise net.TransientExhaustedError(
                f"pixiv 检索应答 error=true: {data.get('message')!r}")
        arts = ((data.get("body") or {}).get("illustManga") or {}).get("data") or []
        out: list[dict] = []
        for a in arts:
            if a.get("xRestrict", 0) > 0:   # R18 剔除（用户拍板）
                continue
            url = a.get("url")
            if not url:
                continue
            out.append({
                "tiers": self._candidate_urls(url, a.get("illustType")),
                "landing": f"https://www.pixiv.net/artworks/{a.get('id')}",
                "width": _int_or_none(a.get("width")),
                "height": _int_or_none(a.get("height")),
                "mime": None,
                "license": None,
                "author": a.get("userName"),
                "native": {"artwork_id": a.get("id"),
                        "title": a.get("title"),
                        "illust_type": a.get("illustType"),
                        "user_id": a.get("userId")},
            })
            if len(out) >= k:
                break
        return out


class BingImagesEngine:
    """Bing 图片 async 接口 HTML：每个结果块的 m 属性是 JSON
    （murl=原图直链/mw/mh 尺寸/purl=来源页），turl 缩略图不用。"""

    name = "bing_images"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=10.0, concurrency=16, proxy=False)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=False)
    _API = "https://www.bing.com/images/async"
    _M_RE = re.compile(r'm="({.*?})"\s', re.S)

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._API, client=client,
            params={"q": query, "first": "0", "count": "35", "mmasync": "1"},
            headers={"User-Agent": net.BROWSER_UA,
                     "Accept-Language": "en"},
        )
        out: list[dict] = []
        seen: set[str] = set()
        for m in self._M_RE.finditer(resp.text):
            try:
                meta = json.loads(_html.unescape(m.group(1)))
            except (json.JSONDecodeError, ValueError):
                continue
            url = (meta.get("murl") or "").strip()
            if not url.lower().startswith("http") or url in seen:
                continue
            seen.add(url)
            out.append({
                # murl 即源站原图直链，单档
                "tiers": [url],
                "landing": meta.get("purl"),
                "width": _int_or_none(meta.get("mw")),
                "height": _int_or_none(meta.get("mh")),
                "mime": None,
                "license": None,
                "author": None,
                "native": {"title": meta.get("t"), "desc": meta.get("desc")},
            })
            if len(out) >= k:
                break
        return out


class YandexImagesEngine:
    """Yandex 图片：SSR 页面内嵌 HTML 实体转义的 initialState JSON，
    反转义后提取结构化条目 {url,w,h,fileSizeInBytes}（实网 185 条实证）。
    条目无来源页，landing_url 认缺留 None。"""

    name = "yandex_images"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=10.0, concurrency=16, proxy=False)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=False)
    _SEARCH = "https://yandex.com/images/search"
    _ENTRY_RE = re.compile(
        r'\{"url":"(https://[^"]+)","fileSizeInBytes":(\d+),'
        r'"w":(\d+),"h":(\d+)\}')

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._SEARCH, client=client,
            params={"text": query},
            headers={"User-Agent": net.BROWSER_UA, "Accept-Language": "en"},
        )
        unescaped = _html.unescape(resp.text)
        out: list[dict] = []
        seen: set[str] = set()
        for m in self._ENTRY_RE.finditer(unescaped):
            url = m.group(1)
            if url in seen:
                continue
            seen.add(url)
            out.append({
                # SSR 内嵌 url 即源站原图，单档
                "tiers": [url],
                "landing": None,
                "width": int(m.group(3)),
                "height": int(m.group(4)),
                "mime": None,
                "license": None,
                "author": None,
                "native": {"file_size": int(m.group(2))},
            })
            if len(out) >= k:
                break
        return out


class DeviantArtEngine:
    """DeviantArt RSS（backend.deviantart.com，官方公开通道免 OAuth）：
    media:content 为 wixmp CDN 图直链，media:credit 作者名。"""

    name = "deviantart"
    k_cap = K_STRUCTURED
    limits = net.SourceLimits(rate=2.0, concurrency=2, proxy=True)
    dl_limits = net.SourceLimits(rate=6.0, concurrency=8, proxy=True)
    _RSS = "https://backend.deviantart.com/rss.xml"

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._RSS, client=client,
            params={"type": "deviation", "q": f"boost:popular {query}"},
            headers={"User-Agent": API_UA},
        )
        out: list[dict] = []
        seen: set[str] = set()
        for block in re.finditer(r"<item>(.*?)</item>", resp.text, re.S):
            item_xml = block.group(1)

            def _tag(tag: str) -> Optional[str]:
                m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", item_xml, re.S)
                return _html.unescape(m.group(1)).strip() if m else None

            # media:content 是自闭合标签，图址/尺寸在属性里（实测结构）
            mc = re.search(r'<media:content\s+url="([^"]+)"[^>]*'
                           r'height="(\d+)"\s+width="(\d+)"', item_xml)
            url = mc.group(1) if mc else None
            if not url or url in seen:
                continue
            seen.add(url)
            out.append({
                # RSS media:content 给的就是 wixmp 全尺寸档，单档
                "tiers": [url],
                "landing": _tag("link"),
                "width": int(mc.group(3)) if mc else None,
                "height": int(mc.group(2)) if mc else None,
                "mime": None,
                "license": None,
                "author": _tag("media:credit"),
                "native": {"title": _tag("title")},
            })
            if len(out) >= k:
                break
        return out


# ---------------------------------------------------------------------------
# adapters（国内爬虫档三源：纯业务经验自旧系统 collect/sources/ 迁移）
# ---------------------------------------------------------------------------

_EXT_RE = re.compile(r"\.(?:jpg|jpeg|png|webp)", re.I)
# UI 噪声关键词：命中即视为图标/装饰而非内容图（抽取型源的解析边界，非语义筛选）
_UI_SKIP = (
    "icon", "logo", "avatar", "emoji", "banner", "ad", "btn", "sprite",
    "bg", "arrow", "nav", "footer", "header", "qrcode", "wechat", "badge",
    "loading", "placeholder", "default", "thumb_s", "-gray",
    "play", "ico", "pixel", "counter", "stat",
)


def _filter_img_urls(urls: list[str], host_hint: tuple[str, ...], *,
                     exclude: tuple[str, ...] = ()) -> list[str]:
    """图片 URL 清洗：扩展名把关、http→https 升级、exclude/UI 噪声剔除、去重，
    host_hint 命中的内容图排前（保序不截断，K 封顶在调用方）。"""
    out: list[str] = []
    seen: set[str] = set()
    ordered = sorted(urls, key=lambda u: 0 if any(h in u.lower() for h in host_hint) else 1) \
        if host_hint else urls
    for u in ordered:
        if not _EXT_RE.search(u):
            continue
        if u.lower().startswith("http://"):
            u = "https://" + u[u.find("://") + 3:]
        low = u.lower()
        if exclude and any(k in low for k in exclude):
            continue
        if any(k in low for k in _UI_SKIP):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


class HuabanApiEngine:
    """花瓣 api.huaban.com/search JSON 接口（旧系统实证通道；HTML 页 JS 渲染拦截，不迁）。
    pins[].file.key 拼 hbimg.huaban.com 直链，file 内宽高即原图尺寸。"""

    name = "huaban_api"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=10.0, concurrency=16, proxy=False)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=False)
    _API = "https://api.huaban.com/search"

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._API, client=client,
            params={"q": query, "limit": "20"},
            headers={"User-Agent": net.BROWSER_UA,
                     "Accept": "application/json",
                     "Referer": "https://huaban.com/",
                     "Accept-Language": "zh-CN,zh;q=0.9"},
        )
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise net.TransientExhaustedError(
                f"huaban_api 检索应答非 JSON（疑似反爬页）: {query}"
            ) from exc
        pins = data.get("pins") or data.get("data") or []
        out: list[dict] = []
        seen: set[str] = set()
        for p in pins:
            f = p.get("file") or {}
            key = f.get("key")
            if not key:
                continue
            content_url = "https://hbimg.huaban.com/" + key
            if content_url in seen:
                continue
            seen.add(content_url)
            out.append({
                # hbimg 直链即原图（file 内宽高即原图尺寸），单档
                "tiers": [content_url],
                "landing": None,
                "width": _int_or_none(f.get("width")),
                "height": _int_or_none(f.get("height")),
                "mime": None,
                "license": None,
                "author": None,
                "native": {"pin_id": p.get("pin_id"),
                        "board_title": (p.get("board") or {}).get("title")},
            })
            if len(out) >= k:
                break
        return out


class ToutiaoEngine:
    """今日头条搜索（so.toutiao.com）全文本图链抽取（含内联 JSON，比仅扫 <img> 更全）。
    toutiaoimg.com 为签名图床普遍 403 防盗链，出口剔除；仅保留 byteimg/douyinpic CDN。"""

    name = "toutiao"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=10.0, concurrency=16, proxy=False)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=False)
    _SEARCH = "https://so.toutiao.com/search"
    _URL_RE = re.compile(r"https?://[^\s\"'<>]+\.(?:jpg|jpeg|png|webp)", re.I)
    _HOST_HINT = ("byteimg.com", "douyinpic.com")

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._SEARCH, client=client,
            params={"keyword": query, "source": "input",
                    "traffic_source": "web_search_tab"},
            headers={"User-Agent": net.BROWSER_UA,
                     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                     "Accept-Language": "zh-CN,zh;q=0.9"},
        )
        urls = self._URL_RE.findall(resp.text)
        out: list[dict] = []
        for u in _filter_img_urls(urls, self._HOST_HINT,
                                  exclude=("toutiaoimg.com",)):
            out.append({
                "tiers": [u],
                "landing": None,
                "width": None,
                "height": None,
                "mime": None,
                "license": None,
                "author": None,
                "native": {},
            })
            if len(out) >= k:
                break
        return out


class So360Engine:
    """360 图片 image.so.com/j JSON 接口：多档候选 imgurl（原图）> middle > thumb
    （档位大到小，下载端按序首个成功即停）；url 字段是图所在网页（landing_url）。"""

    name = "so360"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=2.0, concurrency=8)    # 2026-09-06 巡检降速：A机错误率52%持续两轮（反爬）
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=False)
    _API = "https://image.so.com/j"

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        resp = await net.request(
            self.name, "GET", self._API, client=client,
            params={"q": query, "sn": "0", "pn": "30", "src": "tab_www"},
            headers={"User-Agent": net.BROWSER_UA,
                     "Accept": "application/json",
                     "Referer": "https://image.so.com/",
                     "Accept-Language": "zh-CN,zh;q=0.9"},
        )
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise net.TransientExhaustedError(
                f"so360 检索应答非 JSON（疑似反爬页）: {query}"
            ) from exc
        lst = data.get("list") or [] if isinstance(data, dict) else []
        out: list[dict] = []
        seen: set[str] = set()
        for it in lst:
            if not isinstance(it, dict):
                continue
            # 档位大到小收集全档位候选（去重），下载端按序首个成功即停
            urls = []
            for key in ("imgurl", "middle", "thumb"):
                u = (it.get(key) or "").strip()
                if u.lower().startswith("http") and u not in seen:
                    seen.add(u)
                    urls.append(u)
            if not urls:
                continue
            out.append({
                "tiers": urls,
                "landing": it.get("url") or None,
                "width": _int_or_none(it.get("width")),
                "height": _int_or_none(it.get("height")),
                "mime": None,
                "license": None,
                "author": None,
                "native": {"title": it.get("title")},
            })
            if len(out) >= k:
                break
        return out


class SearxngEngine:
    """SearXNG 元搜索（2026-09-04 接入）：自托管本机实例聚合 google/bing/ddg 图片检索。

    - 端点：data/webgate 模块的 /search?format=json（settings.yml 显式开 JSON，
      默认关闭；127.0.0.1:8080 仅本机监听）；
    - 档位契约：content_urls = [img_src（原图直链）, thumbnail_src（缩图兜底）]，
      落进下载档位轮转——元搜索死链/防盗链率高，缩图是实测可得的兜底档；
    - native.engine 保留来源引擎（google/bing_images/duckduckgo 等）：
      产出质量按引擎观测，劣质引擎在 SearXNG settings 侧直接关；
    - language 对位：zh 种子传 zh-CN、latin 种子传 en（SearXNG 转发给上游引擎）。
    """

    name = "searxng"
    k_cap = K_SEMANTIC
    limits = net.SourceLimits(rate=10.0, concurrency=16)
    dl_limits = net.SourceLimits(rate=15.0, concurrency=32, proxy=True)
    _API = "http://127.0.0.1:8080/search"

    async def search(self, query, k, *, lang="zh", client=None):
        k = min(k, self.k_cap)
        params = {
            "q": query,
            "categories": "images",
            "format": "json",
            "language": "zh-CN" if lang == "zh" else "en",
            "safesearch": 1,
        }
        try:
            resp = await net.request(self.name, "GET", self._API,
                                     params=params, client=client)
        except (net.DeterministicError, net.TransientExhaustedError) as exc:
            # 网关没起是配置错误不是源故障：fail-fast 终止并给出口，
            # 不进认缺（否则全部 searxng 召回无声消失）
            if is_connect_failure(exc):
                raise RuntimeError(
                    "SearXNG 网关不可达（127.0.0.1:8080）："
                    "先启动 bash data/webgate/start.sh") from exc
            raise
        out: list[dict] = []
        seen: set[str] = set()
        for res in resp.json().get("results", []):
            img = res.get("img_src")
            if not img or not str(img).startswith(("http://", "https://")):
                continue   # 相对/协议相对链（flickr 风等）无 host 不可下载
            key = str(img)
            if key in seen:
                continue
            seen.add(key)
            tiers = [u for u in (img, res.get("thumbnail_src")) if u]
            resolution = str(res.get("resolution") or "")
            w, _, h = resolution.partition("x")
            out.append({
                "tiers": tiers,
                "landing": res.get("url") or None,
                "width": _int_or_none(w) if resolution else None,
                "height": _int_or_none(h) if resolution else None,
                "mime": None,
                "license": None,
                "author": None,
                "native": {"engine": res.get("engine"),
                        "title": res.get("title")},
            })
            if len(out) >= k:
                break
        return out


# ---------------------------------------------------------------------------
# 引擎与限速注册（自声明式：import 期完成，检索闸+下载闸随引擎走）
# ---------------------------------------------------------------------------

_ENGINES = (
    WikimediaZhEngine(), WikimediaEngine(), BaiduEngine(), AniListEngine(),
    MalEngine(), PixivEngine(), BingImagesEngine(), YandexImagesEngine(),
    DeviantArtEngine(), HuabanApiEngine(), ToutiaoEngine(), So360Engine(),
    SearxngEngine(),
)
for _e in _ENGINES:
    register_engine(_e)
    net.register_limits({_e.name: _e.limits, f"dl:{_e.name}": _e.dl_limits})


# ---------------------------------------------------------------------------
# 域路由表（原 getsource.py 迁入：路由是检索算子的内部策略，非独立概念）
# lang → 源列表（顺序即投递顺序，无权重语义）；未登记 lang 认缺不回落。
#
# 沿革：2026-08-20 虚拟角色向新源对所有 seed 全量投递拍板；2026-08-22
# 代理复通还原代理源、anilist/mal 剔除（专场已爬）、pixiv/deviantart 移入
# latin-only 恢复语言对位；国内爬虫三源只打 zh；2026-09-04 searxng 双行
# 挂载（language 参数对位 zh-CN/en）。
_CHAR_SOURCES = ["bing_images", "yandex_images"]
_LATIN_ONLY_SOURCES = ["pixiv"]
_CN_CRAWLER_SOURCES = ["huaban_api", "toutiao", "so360"]
_META_SOURCES = ["searxng"]

ROUTE_TABLE: dict = {
    "zh": ["baidu", "wikimedia_zh"] + _CN_CRAWLER_SOURCES + _CHAR_SOURCES
          + _META_SOURCES,
    "latin": ["wikimedia"] + _CHAR_SOURCES + _LATIN_ONLY_SOURCES + _META_SOURCES,
}


def _sources_for(seed: dict) -> list:
    return ROUTE_TABLE.get(seed.get("lang", "zh"), [])


class SearchStage(StreamStage):
    """检索算子（demiflow 规范）：种子行 → 候选行集。

    内部策略：域路由（ROUTE_TABLE）+ 多引擎并发扇出（各引擎自带限速闸
    全局节流）+ dict 行映射。top_n 为每源固定切片（无补位）；单源白名单
    异常认缺不断链，非白名单异常（如网关 fail-fast）终止整链。

    行契约：
    - 读键：name、query、lang
    - 产行：{**种子键, source, tiers[档位大到小], landing, width, height,
            mime, license, author, native}
    """
    label = "search"
    concurrency = 16
    queue_depth = 96
    catch = (net.InfraError, httpx.HTTPError)   # 检索级认缺白名单

    def __init__(self, top_n: int = 2, k: int = K_SEMANTIC):
        self.top_n, self.k = top_n, k

    async def __call__(self, seed: dict):
        sources = _sources_for(seed)
        if not sources:
            return None
        # 每行切片数：行可带 top_n_hint 覆盖类默认（配额驱动切片用，
        # 通用机制——不带提示的行走类声明值）
        top_n = seed.get("top_n_hint") or self.top_n
        query = seed.get("query") or seed["name"]
        results = await asyncio.gather(*(
            engine_search(s, query, self.k, lang=seed.get("lang", "zh"))
            for s in sources), return_exceptions=True)
        out: list[dict] = []
        for source, rows in zip(sources, results):
            if isinstance(rows, BaseException):
                if isinstance(rows, self.catch):
                    continue            # 单源认缺
                raise rows              # 真异常（含网关 fail-fast）
            for r in rows[:top_n]:      # 每源固定切片无补位
                out.append({**seed,
                            "source": source,
                            "tiers": r.get("tiers") or [],
                            "landing": r.get("landing"),
                            "width": r.get("width"),
                            "height": r.get("height"),
                            "mime": r.get("mime"),
                            "license": r.get("license"),
                            "author": r.get("author"),
                            "native": r.get("native") or {}})
        return out or None


def scale_engine_limits(divisor: int) -> None:
    """分片并行时按分片数等分限速/并发预算（2026-09-04·D2）。

    全局限速语义保持：N 个分片进程各持 rate/N、concurrency/N，合计≈
    原预算（防封禁口径不超发；静态划分无中心协调，分片数须固定）。
    下限保护：rate>=0.1、concurrency>=1。须在引擎注册后、首次请求前
    调用（flow --shard 启动期）。
    """
    if divisor <= 1:
        return
    from dataclasses import replace
    scaled = {key: replace(lim, rate=max(0.1, lim.rate / divisor),
                           concurrency=max(1, lim.concurrency // divisor))
              for key, lim in net.SOURCE_LIMITS.items()}
    net.register_limits(scaled)
