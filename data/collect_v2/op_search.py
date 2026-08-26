"""collect_v2 检索算子：输入 (种子, 源) → 输出有界有序候选列表。

契约（.qoder/handoff_collect_v2.md §3.1 / §4.1）：
- 只收域路由之后的 (种子, 源) 对，本文件不做域路由；
- 输出按源原生相关度排序的候选列表，adapter 不重排、不筛选、不凑数；
- K 封顶不分页深翻：语义/爬虫源 ≤5，结构化源 10-20；
- 列表不足或为空原样返回，认缺是链层的事；
- adapter 只产结构化候选，不碰主清单；所有请求走 infra.request。

数据流（用户拍板）：算子链是数据算子流，全链路流转统一的 Item 记录
（类似 Ray Dataset 的行），各算子在 Item 上追加自己的产出字段，
不设独立的 Candidate/DownloadResult 类型。

候选 URL 契约（2026-08-21 用户拍板，数据用途定案为训练数据）：
- adapter 产 content_urls：**同一张图**的候选链接有序列表，按档位**大到小**
  （原图在前、压缩档殿后）；多数源天然单档即单元素列表，pixiv 等
  有多档的源产多元素；一图只落一档，绝不多档并存（浪费算力与存储）；
- op_download 按序依次试，首个成功即停，获胜链接记回 content_url（清单只写它）；
- 源知识（档位推导）留在 adapter，下载算子只认通用有序列表。

本期代表源：wikimedia_zh（官方 API 档）、baidu（爬虫档）。
wikimedia（英文/拉丁 seed 打同一 commons 端点，2026-08-20 拍板补注册）。
2026-08-20 新增六源（用户拍板，虚拟角色向）：anilist（GraphQL 只搜 Character）、
mal（角色搜索 HTML）、pixiv（ajax，regular 直取、R18 出口剔除）、bing_images、
yandex_images（SSR initialState 解析）、deviantart（RSS）；
fandom 全局搜索端点被 Cloudflare 拦，挂起待拍板。
2026-08-20 新增国内爬虫三源（旧系统迁移）：huaban_api（api.huaban.com JSON）、
toutiao（so.toutiao.com 全文本图链抽取）、so360（image.so.com/j JSON）。
"""

from __future__ import annotations

import html as _html
import json
import re
from dataclasses import dataclass, field
from typing import Optional

import httpx

from collect_v2 import infra

K_SEMANTIC = 5        # 语义检索源（wikimedia/搜索爬虫）K 封顶
K_STRUCTURED = 15     # 结构化源（inaturalist 等）K 封顶，后续源启用时生效


def _int_or_none(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


# UA 策略（实网实测结论）：
# - 官方 API 档：Wikimedia robot policy 要求可识别调用方与可联系方式，否则 403；
#   占位邮箱（example.com）会在下载层被拦，真实仓库 URL 实测放行（用户拍板用仓库首页）；
# - 爬虫档：自报机器人身份会被拦（百度 antiFlag "Forbid spider access"），用常规浏览器 UA。
API_UA = ("collect-v2/0.1 (research image collection; "
          "https://github.com/vincent9299/demiwtg) httpx/0.28")
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


# ---------------------------------------------------------------------------
# 数据结构（算子链统一流转的 Item）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Seed:
    """检索种子：域路由之后的 (实例名, 源) 里的种子部分。

    由 op_seed 产出：中文本体 seed（lang="zh"，query 即实例名）与
    西文投影 seed（lang="latin"，query 为 LLM 判定的同实体西文名）。
    """
    name: str                        # 实例名
    query: Optional[str] = None      # 真实检索词，缺省即实例名（透传给 sink）
    lang: str = "zh"                 # 种子语言形态：zh / latin（op_seed 判定）


@dataclass
class Item:
    """算子链统一流转的数据记录（数据算子流，类似 Ray Dataset 的行）。

    各算子只追加自己的产出字段，不改写上游字段；字段缺失即 None：
    - op_seed 产：种子（instance/query/lang，见 Seed）；
    - op_search 产：instance/query/lang/source/rank/content_urls/landing_url/
      declared_width/declared_height/mime/license/author/native；
    - op_download 追加：content_url（获胜链接）/data/sha256/ext/
      actual_width/actual_height/size_bytes；
    - op_annotate 追加：kb_match/richness/caption/identity/focus/quality
      （失败则全部为 None）；
    - op_sink 追加：local_path/fetched_at（落盘成功才有值）。
    """
    # 种子（域路由后的实例与真实检索词，query 禁止回落造假）
    instance: str
    query: str
    lang: str = "zh"    # 种子语言形态 zh/latin，sink 写 query_langs 用
    # 检索产出（声明尺寸常失真，实际尺寸以下载解码为准）
    source: str = ""
    rank: int = 0
    # 同一张图的候选链接，档位大到小（原图在前）；op_download 按序首个成功即停
    content_urls: list[str] = field(default_factory=list)
    landing_url: Optional[str] = None
    declared_width: Optional[int] = None
    declared_height: Optional[int] = None
    mime: Optional[str] = None
    license: Optional[str] = None
    author: Optional[str] = None
    native: dict = field(default_factory=dict)   # 源原生元数据原样保留
    # 下载产出
    content_url: Optional[str] = None   # 获胜候选（op_download 记回，清单写它）
    data: Optional[bytes] = None
    sha256: Optional[str] = None
    ext: Optional[str] = None
    actual_width: Optional[int] = None
    actual_height: Optional[int] = None
    size_bytes: Optional[int] = None
    # 标注产出
    kb_match: Optional[int] = None
    richness: Optional[int] = None
    caption: Optional[str] = None
    identity: Optional[bool] = None
    focus: Optional[int] = None        # 主体显著度（2026-08-20 拍板转正）
    quality: Optional[float] = None    # 综合分（op_annotate 派生，非 VLM 产出）
    # 落盘产出
    local_path: Optional[str] = None   # blobs/<aa>/<sha>.<ext>，相对 datasets/demiwtg/
    fetched_at: Optional[float] = None


# ---------------------------------------------------------------------------
# adapters
# ---------------------------------------------------------------------------

class SearchAdapter:
    """检索源适配器基类：一源一类，只产 Item 不碰主清单。"""

    source: str = ""
    k_cap: int = K_SEMANTIC

    async def search(
        self,
        seed: Seed,
        k: int,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> list[Item]:
        raise NotImplementedError


class WikimediaZhAdapter(SearchAdapter):
    """维基共享资源（中文检索词）：打 commons.wikimedia.org 媒体库本体（旧系统验证过的端点），
    generator=search 只搜文件命名空间。"""

    source = "wikimedia_zh"
    k_cap = K_SEMANTIC
    _API = "https://commons.wikimedia.org/w/api.php"

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
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
        resp = await infra.request(
            self.source, "GET", self._API, client=client,
            params=params, headers={"User-Agent": API_UA},
        )
        pages = (resp.json().get("query") or {}).get("pages") or {}
        # API 返回 dict，index 字段即相关度序；排序后取前 k
        ordered = sorted(pages.values(), key=lambda p: int(p.get("index", 0)))
        out: list[Item] = []
        for rank, page in enumerate(ordered[:k]):
            info = (page.get("imageinfo") or [{}])[0]
            ext = info.get("extmetadata") or {}

            def _ext(key: str) -> Optional[str]:
                v = ext.get(key)
                return v.get("value") if isinstance(v, dict) else None

            props = page.get("pageprops") or {}
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=rank,
                # commons API 直出即原图，单档
                content_urls=[info["url"]] if info.get("url") else [],
                landing_url=props.get("canonicalurl") or info.get("descriptionurl"),
                declared_width=info.get("width"),
                declared_height=info.get("height"),
                mime=info.get("mime"),
                license=_ext("LicenseShortName"),
                author=_ext("Artist"),
                native={
                    "page_title": page.get("title"),
                    "page_id": page.get("pageid"),
                    "mediatype": info.get("mediatype"),
                },
            ))
        return out


class WikimediaAdapter(WikimediaZhAdapter):
    """维基共享资源（拉丁检索词）：与 zh 版同端点同参数——commons 搜索不限语言，
    独立 source 名只为 latin 路由/限速池/统计口径分立（2026-08-20 拍板补注册）。"""

    source = "wikimedia"


class BaiduAdapter(SearchAdapter):
    """百度图片 acjson 接口（爬虫档）。

    纯业务经验来自旧系统（_reference/old_repo/collect/sources/baidu.py）：
    - 无会话 cookie 直接调 acjson 会被 antiFlag 拦截，需先预热拿 BAIDUID；
    - objURL 为混淆编码且解码不稳定，**不用**；优先 middleURL（明文 https、较大），
      回退 thumbURL/hoverURL（middleURL 已是可用最大档，候选单元素）；
    - acjson 的 width/height 是原图尺寸，与 middleURL 实际服务尺寸常不符，
      声明尺寸改从 URL 查询串 ?w=&h= 提取；
    - 非 JSON 应答按瞬态失败走 infra 重试。
    """

    source = "baidu"
    k_cap = K_SEMANTIC
    _API = "https://image.baidu.com/search/acjson"
    _HOME = "https://www.baidu.com/"
    _warmed = False

    async def _warmup(self, client: Optional[httpx.AsyncClient]) -> None:
        """预热拿会话 cookie（BAIDUID），失败不阻断，留给正式请求自行暴露。"""
        if BaiduAdapter._warmed:
            return
        http = client or infra.get_client()
        try:
            await http.get(self._HOME, headers={
                "User-Agent": BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9",
            })
        except httpx.HTTPError:
            return
        BaiduAdapter._warmed = True

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

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
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
        resp = await infra.request(
            self.source, "GET", self._API, client=client,
            params=params,
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Referer": "https://image.baidu.com/",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            # 反爬页/空壳应答：按瞬态失败上抛，由 infra 分类重试语义兜住
            raise infra.TransientExhaustedError(
                f"baidu 检索应答非 JSON（疑似反爬页）: {query}"
            ) from exc
        if data.get("antiFlag"):
            # 源明确拦截（如 "Forbid spider access"）：重试无意义，确定性失败认缺
            raise infra.DeterministicError(
                f"baidu 反爬拦截: {data.get('message')!r} query={query}"
            )
        items = data.get("data") or []
        out: list[Item] = []
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
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=rank,
                content_urls=[content_url],
                landing_url=it.get("fromURL") or it.get("hoverURL"),
                declared_width=w,
                declared_height=h,
                mime=None,   # 百度不返回 MIME，下载后由解码实测补齐
                license=None,
                author=None,
                native={
                    "from_page_title": it.get("fromPageTitleEnc"),
                    "from_url": it.get("fromURL"),
                    "orig_width": _int_or_none(it.get("width")),
                    "orig_height": _int_or_none(it.get("height")),
                    "size_bytes": _int_or_none(it.get("di")),
                },
            ))
            rank += 1
            if rank >= k:
                break
        return out


# ---------------------------------------------------------------------------
# adapters（2026-08-20 新增六源，接口细节均实网探测实证）
# ---------------------------------------------------------------------------

class AniListAdapter(SearchAdapter):
    """AniList GraphQL（官方、免鉴权）：只搜 Character（用户拍板，虚拟角色本体）。

    单次查询只取最优一条（GraphQL search 语义），多召回靠链层多种子/多源覆盖。
    """

    source = "anilist"
    k_cap = K_STRUCTURED
    _API = "https://graphql.anilist.co"
    _QUERY = ("query($q:String){Character(search:$q){"
              "id name{full} image{large} siteUrl}}")

    async def search(self, seed, k, *, client=None):
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "POST", self._API, client=client,
            json={"query": self._QUERY, "variables": {"q": query}},
            headers={"User-Agent": API_UA, "Content-Type": "application/json"},
        )
        char = (resp.json().get("data") or {}).get("Character")
        if not char or not (char.get("image") or {}).get("large"):
            return []   # 无命中 = 认缺
        return [Item(
            instance=seed.name,
            query=query,
            lang=getattr(seed, "lang", "zh"),
            source=self.source,
            rank=0,
            # AniList image.large 已是 API 提供的最大档，单档
            content_urls=[char["image"]["large"]],
            landing_url=char.get("siteUrl"),
            declared_width=None,
            declared_height=None,
            mime=None,
            license=None,
            author=None,
            native={"character_id": char.get("id"),
                    "character_name": (char.get("name") or {}).get("full")},
        )]


class MalAdapter(SearchAdapter):
    """MyAnimeList 角色搜索 HTML 抓取（官方 API 需 client_id，不用）。

    character.php?q= 列表页结构（实网实测）：每行是绝对 URL 角色链接
    <a href="https://myanimelist.net/character/ID/Name">，链接**后**紧跟
    lazyload img（data-src 为 /r/42x62/ 规格缩略图，去规格前缀即 CDN 原图）。
    """

    source = "mal"
    k_cap = K_SEMANTIC
    _SEARCH = "https://myanimelist.net/character.php"
    _RESIZED_RE = re.compile(r"/r/\d+x\d+/")
    _ROW_RE = re.compile(
        r'href="https://myanimelist\.net/character/(\d+)/([^"]+)".*?'
        r'data-src="([^"]+)"', re.S)

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._SEARCH, client=client,
            params={"q": query},
            headers={"User-Agent": BROWSER_UA,
                     "Accept-Language": "en"},
        )
        out: list[Item] = []
        seen: set[str] = set()
        for m in self._ROW_RE.finditer(resp.text):
            cid, cname, img = m.group(1), m.group(2), m.group(3)
            content_url = self._RESIZED_RE.sub("/", img, count=1)
            if content_url in seen:
                continue
            seen.add(content_url)
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                # 去 /r/规格前缀后已是 CDN 原图，单档
                content_urls=[content_url],
                landing_url=f"https://myanimelist.net/character/{cid}/{cname}",
                declared_width=None,
                declared_height=None,
                mime=None,
                license=None,
                author=None,
                native={"character_id": int(cid),
                        "character_name": cname.replace("_", " ")},
            ))
            if len(out) >= k:
                break
        return out


class PixivAdapter(SearchAdapter):
    """Pixiv 搜索 ajax 接口（无需登录，必须带站内 Referer）。

    候选档位（2026-08-21 拍板，数据用途定案训练数据，原图优先）：
    [original.jpg, original.png, master1200] —— 原图扩展名无法从搜索接口
    得知，jpg/png 依次试错，全败回退 master1200（长边 1200 压缩档）；
    原图与压缩档同路径时间桶，作品被删时两档同死，回退只救扩展名猜错；
    ugoira（illustType=2）无静态原图（原件是 zip），只给 master1200 首帧。
    xRestrict>0 的 R18 作品在检索出口剔除（内容政策，非语义过滤）。
    """

    source = "pixiv"
    k_cap = K_STRUCTURED
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
        master = PixivAdapter._regular_url(thumb)
        if _int_or_none(illust_type) == 2 or "/custom-thumb/" in master:
            return [master]
        orig = re.sub(r"/img-master/", "/img-original/", master)
        orig = re.sub(r"_master1200\.\w+$", "", orig)
        return [orig + ".jpg", orig + ".png", master]

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._API + query, client=client,
            params={"lang": "en"},
            headers={"User-Agent": BROWSER_UA,
                     "Referer": "https://www.pixiv.net/",
                     "Accept": "application/json"},
        )
        data = resp.json()
        if data.get("error"):
            raise infra.TransientExhaustedError(
                f"pixiv 检索应答 error=true: {data.get('message')!r}")
        arts = ((data.get("body") or {}).get("illustManga") or {}).get("data") or []
        out: list[Item] = []
        for a in arts:
            if a.get("xRestrict", 0) > 0:   # R18 剔除（用户拍板）
                continue
            url = a.get("url")
            if not url:
                continue
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                content_urls=self._candidate_urls(url, a.get("illustType")),
                landing_url=f"https://www.pixiv.net/artworks/{a.get('id')}",
                declared_width=_int_or_none(a.get("width")),
                declared_height=_int_or_none(a.get("height")),
                mime=None,
                license=None,
                author=a.get("userName"),
                native={"artwork_id": a.get("id"),
                        "title": a.get("title"),
                        "illust_type": a.get("illustType"),
                        "user_id": a.get("userId")},
            ))
            if len(out) >= k:
                break
        return out


class BingImagesAdapter(SearchAdapter):
    """Bing 图片 async 接口 HTML：每个结果块的 m 属性是 JSON
    （murl=原图直链/mw/mh 尺寸/purl=来源页），turl 缩略图不用。"""

    source = "bing_images"
    k_cap = K_SEMANTIC
    _API = "https://www.bing.com/images/async"
    _M_RE = re.compile(r'm="({.*?})"\s', re.S)

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._API, client=client,
            params={"q": query, "first": "0", "count": "35", "mmasync": "1"},
            headers={"User-Agent": BROWSER_UA,
                     "Accept-Language": "en"},
        )
        out: list[Item] = []
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
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                # murl 即源站原图直链，单档
                content_urls=[url],
                landing_url=meta.get("purl"),
                declared_width=_int_or_none(meta.get("mw")),
                declared_height=_int_or_none(meta.get("mh")),
                mime=None,
                license=None,
                author=None,
                native={"title": meta.get("t"), "desc": meta.get("desc")},
            ))
            if len(out) >= k:
                break
        return out


class YandexImagesAdapter(SearchAdapter):
    """Yandex 图片：SSR 页面内嵌 HTML 实体转义的 initialState JSON，
    反转义后提取结构化条目 {url,w,h,fileSizeInBytes}（实网 185 条实证）。
    条目无来源页，landing_url 认缺留 None。"""

    source = "yandex_images"
    k_cap = K_SEMANTIC
    _SEARCH = "https://yandex.com/images/search"
    _ENTRY_RE = re.compile(
        r'\{"url":"(https://[^"]+)","fileSizeInBytes":(\d+),'
        r'"w":(\d+),"h":(\d+)\}')

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._SEARCH, client=client,
            params={"text": query},
            headers={"User-Agent": BROWSER_UA, "Accept-Language": "en"},
        )
        unescaped = _html.unescape(resp.text)
        out: list[Item] = []
        seen: set[str] = set()
        for m in self._ENTRY_RE.finditer(unescaped):
            url = m.group(1)
            if url in seen:
                continue
            seen.add(url)
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                # SSR 内嵌 url 即源站原图，单档
                content_urls=[url],
                landing_url=None,
                declared_width=int(m.group(3)),
                declared_height=int(m.group(4)),
                mime=None,
                license=None,
                author=None,
                native={"file_size": int(m.group(2))},
            ))
            if len(out) >= k:
                break
        return out


class DeviantArtAdapter(SearchAdapter):
    """DeviantArt RSS（backend.deviantart.com，官方公开通道免 OAuth）：
    media:content 为 wixmp CDN 图直链，media:credit 作者名。"""

    source = "deviantart"
    k_cap = K_STRUCTURED
    _RSS = "https://backend.deviantart.com/rss.xml"

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._RSS, client=client,
            params={"type": "deviation", "q": f"boost:popular {query}"},
            headers={"User-Agent": API_UA},
        )
        out: list[Item] = []
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
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                # RSS media:content 给的就是 wixmp 全尺寸档，单档
                content_urls=[url],
                landing_url=_tag("link"),
                declared_width=int(mc.group(3)) if mc else None,
                declared_height=int(mc.group(2)) if mc else None,
                mime=None,
                license=None,
                author=_tag("media:credit"),
                native={"title": _tag("title")},
            ))
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


class HuabanApiAdapter(SearchAdapter):
    """花瓣 api.huaban.com/search JSON 接口（旧系统实证通道；HTML 页 JS 渲染拦截，不迁）。
    pins[].file.key 拼 hbimg.huaban.com 直链，file 内宽高即原图尺寸。"""

    source = "huaban_api"
    k_cap = K_SEMANTIC
    _API = "https://api.huaban.com/search"

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._API, client=client,
            params={"q": query, "limit": "20"},
            headers={"User-Agent": BROWSER_UA,
                     "Accept": "application/json",
                     "Referer": "https://huaban.com/",
                     "Accept-Language": "zh-CN,zh;q=0.9"},
        )
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise infra.TransientExhaustedError(
                f"huaban_api 检索应答非 JSON（疑似反爬页）: {query}"
            ) from exc
        pins = data.get("pins") or data.get("data") or []
        out: list[Item] = []
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
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                # hbimg 直链即原图（file 内宽高即原图尺寸），单档
                content_urls=[content_url],
                landing_url=None,
                declared_width=_int_or_none(f.get("width")),
                declared_height=_int_or_none(f.get("height")),
                mime=None,
                license=None,
                author=None,
                native={"pin_id": p.get("pin_id"),
                        "board_title": (p.get("board") or {}).get("title")},
            ))
            if len(out) >= k:
                break
        return out


class ToutiaoAdapter(SearchAdapter):
    """今日头条搜索（so.toutiao.com）全文本图链抽取（含内联 JSON，比仅扫 <img> 更全）。
    toutiaoimg.com 为签名图床普遍 403 防盗链，出口剔除；仅保留 byteimg/douyinpic CDN。"""

    source = "toutiao"
    k_cap = K_SEMANTIC
    _SEARCH = "https://so.toutiao.com/search"
    _URL_RE = re.compile(r"https?://[^\s\"'<>]+\.(?:jpg|jpeg|png|webp)", re.I)
    _HOST_HINT = ("byteimg.com", "douyinpic.com")

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._SEARCH, client=client,
            params={"keyword": query, "source": "input",
                    "traffic_source": "web_search_tab"},
            headers={"User-Agent": BROWSER_UA,
                     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                     "Accept-Language": "zh-CN,zh;q=0.9"},
        )
        urls = self._URL_RE.findall(resp.text)
        out: list[Item] = []
        for u in _filter_img_urls(urls, self._HOST_HINT,
                                  exclude=("toutiaoimg.com",)):
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                content_urls=[u],
                landing_url=None,
                declared_width=None,
                declared_height=None,
                mime=None,
                license=None,
                author=None,
                native={},
            ))
            if len(out) >= k:
                break
        return out


class So360Adapter(SearchAdapter):
    """360 图片 image.so.com/j JSON 接口：多档候选 imgurl（原图）> middle > thumb
    （档位大到小，下载端按序首个成功即停）；url 字段是图所在网页（landing_url）。"""

    source = "so360"
    k_cap = K_SEMANTIC
    _API = "https://image.so.com/j"

    async def search(self, seed, k, *, client=None):
        k = min(k, self.k_cap)
        query = seed.query or seed.name
        resp = await infra.request(
            self.source, "GET", self._API, client=client,
            params={"q": query, "sn": "0", "pn": "30", "src": "tab_www"},
            headers={"User-Agent": BROWSER_UA,
                     "Accept": "application/json",
                     "Referer": "https://image.so.com/",
                     "Accept-Language": "zh-CN,zh;q=0.9"},
        )
        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise infra.TransientExhaustedError(
                f"so360 检索应答非 JSON（疑似反爬页）: {query}"
            ) from exc
        lst = data.get("list") or [] if isinstance(data, dict) else []
        out: list[Item] = []
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
            out.append(Item(
                instance=seed.name,
                query=query,
                lang=getattr(seed, "lang", "zh"),
                source=self.source,
                rank=len(out),
                content_urls=urls,
                landing_url=it.get("url") or None,
                declared_width=_int_or_none(it.get("width")),
                declared_height=_int_or_none(it.get("height")),
                mime=None,
                license=None,
                author=None,
                native={"title": it.get("title")},
            ))
            if len(out) >= k:
                break
        return out


# ---------------------------------------------------------------------------
# 注册表与分派
# ---------------------------------------------------------------------------

_ADAPTERS: dict[str, SearchAdapter] = {}


def register(adapter: SearchAdapter) -> None:
    _ADAPTERS[adapter.source] = adapter


register(WikimediaZhAdapter())
register(WikimediaAdapter())
register(BaiduAdapter())
register(AniListAdapter())
register(MalAdapter())
register(PixivAdapter())
register(BingImagesAdapter())
register(YandexImagesAdapter())
register(DeviantArtAdapter())
register(HuabanApiAdapter())
register(ToutiaoAdapter())
register(So360Adapter())


async def search(
    seed: Seed,
    source: str,
    k: int = K_SEMANTIC,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> list[Item]:
    """对 source 检索 seed，返回有界有序 Item 列表（可能为空 = 认缺）。"""
    adapter = _ADAPTERS.get(source)
    if adapter is None:
        raise ValueError(f"源 {source!r} 未注册 adapter")
    return await adapter.search(seed, k, client=client)
