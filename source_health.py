"""collect_v2 源策略模块：源健康账本 + 自适应闸门（L0 机制层，零 LLM）。

2026-08-29 拍板：仿视觉知识卡（search_kb）的源健康账本，先在 EN 链落地
（--source-agent 旗子门控），中文链不带旗零变化，跑通后再adopt。

机制（三层记录 → 窗口统计 → 策略评估）：
- 记录端：HTTP 风控状态码（infra.request/stream 回调，dl: 前缀已剥）+
  检索命中/落空（chain search_worker 业务结局）+ 下载成败（download_worker）；
- 策略评估（chain 内 60s 周期任务调用 evaluate）：
  风控应对：窗口内 429/403 合计 ≥RATELIMIT_BURST → 该源速率乘子减半
  （下限 RATE_FLOOR），干净窗口 CLEAN_WINDOW 后逐档回升；
  剔除无效：窗口内检索调用 ≥DISABLE_MIN_CALLS 且命中率 <DISABLE_HIT_RATE
  → 停用 DISABLE_SECONDS 后自动探活复挂（search_kb 同款规则）；
- 账本落盘 state/collect/source_health_v2_<lang>.json（运行时状态按模块
  归位 state/collect/，meta/ 白名单不碰）；只存策略态与累计计数，
  滚动窗口事件内存态不落盘（重启即重新采样，可接受）。
"""

from __future__ import annotations

import json
import os
import time
from collections import deque

WINDOW = 1800.0          # 滚动窗口（秒）
RATELIMIT_BURST = 5      # 窗口内 429/403 合计达到该数 → 节流
RATE_FLOOR = 0.25        # 速率乘子下限
CLEAN_WINDOW = 600.0     # 无新风控信号该时长 → 乘子回升一档
DISABLE_MIN_CALLS = 25   # 窗口内检索调用达到该数才够格判「无效源」
DISABLE_HIT_RATE = 0.04  # 命中率低于此 → 停用
DISABLE_SECONDS = 3600.0  # 停用时长（探活复挂）


class HealthLedger:
    """源健康账本：滚动窗口事件 + 策略态（乘子/停用期）。单事件循环内用，无锁。"""

    def __init__(self, path: str, window: float = WINDOW):
        self.path = path
        self.window = window
        self._ev: dict[str, deque] = {}            # source -> deque[(ts, kind)]
        self._mult: dict[str, float] = {}          # source -> 速率乘子
        self._disabled_until: dict[str, float] = {}
        self._last_bad: dict[str, float] = {}      # 最近一次风控信号时刻
        self._bad_mark: dict[str, int] = {}        # 已动作过节流的风控计数
        self.totals: dict[str, dict] = {}          # 累计计数（可观性，不进策略）

    # ------------------------------------------------------------------
    # 记录端（infra / chain worker 调用）
    # ------------------------------------------------------------------

    def _push(self, source: str, kind: str, now: float) -> None:
        self._ev.setdefault(source, deque()).append((now, kind))
        t = self.totals.setdefault(source, {})
        t[kind] = t.get(kind, 0) + 1

    def note_http(self, source: str, status: int, now: float = 0.0) -> None:
        """风控相关状态码（其余不计）：429/403/5xx。"""
        if status == 429:
            self._push(source, "429", now or time.time())
        elif status == 403:
            self._push(source, "403", now or time.time())
        elif status >= 500:
            self._push(source, "5xx", now or time.time())

    def note_search(self, source: str, hit: bool, now: float = 0.0) -> None:
        now = now or time.time()
        self._push(source, "search_call", now)
        if hit:
            self._push(source, "search_hit", now)

    def note_download(self, source: str, ok: bool, now: float = 0.0) -> None:
        now = now or time.time()
        self._push(source, "dl_call", now)
        if ok:
            self._push(source, "dl_ok", now)

    # ------------------------------------------------------------------
    # 策略态查询（chain worker / 闸门调节用）
    # ------------------------------------------------------------------

    def disabled(self, source: str, now: float = 0.0) -> bool:
        return (now or time.time()) < self._disabled_until.get(source, 0.0)

    def rate_mult(self, source: str) -> float:
        return self._mult.get(source, 1.0)

    def sources(self) -> list:
        return list(self._ev)

    # ------------------------------------------------------------------
    # 策略评估（周期调用；返回动作描述供日志）
    # ------------------------------------------------------------------

    def evaluate(self, now: float = 0.0) -> list:
        now = now or time.time()
        actions: list[str] = []
        for source in list(self._ev):
            d = self._ev[source]
            while d and d[0][0] <= now - self.window:
                d.popleft()
            counts: dict[str, int] = {}
            for _, k in d:
                counts[k] = counts.get(k, 0) + 1

            # 停用等待复挂：期满清窗探活（乘子复位）
            until = self._disabled_until.get(source, 0.0)
            if until:
                if now < until:
                    continue
                self._disabled_until[source] = 0.0
                d.clear()
                self._mult[source] = 1.0
                actions.append(f"{source} 停用期满，探活复挂")
                continue

            # 风控应对：突发 429/403 → 减半（同一批窗口内事件只动作一次，
            # 新增信号才再降档）；信号老化出窗 + 干净窗口 → 回升一档
            bad = counts.get("429", 0) + counts.get("403", 0)
            mult = self._mult.get(source, 1.0)
            mark = self._bad_mark.get(source, 0)
            if (bad >= RATELIMIT_BURST and bad > mark
                    and mult > RATE_FLOOR + 1e-9):
                self._mult[source] = max(RATE_FLOOR, mult / 2)
                actions.append(
                    f"{source} 风控信号×{bad} → 速率×{self._mult[source]:.2f}")
            if bad:
                self._bad_mark[source] = bad
                self._last_bad[source] = now
            elif (now - self._last_bad.get(source, 0.0) > CLEAN_WINDOW
                    and mult < 1.0 - 1e-9):
                self._mult[source] = min(1.0, mult * 2)
                self._bad_mark[source] = 0
                actions.append(
                    f"{source} 干净窗口 → 速率回升×{self._mult[source]:.2f}")

            # 剔除无效（检索侧口径；下载失败另有网络异常认缺兜底）
            calls = counts.get("search_call", 0)
            if calls >= DISABLE_MIN_CALLS:
                hit_rate = counts.get("search_hit", 0) / calls
                if hit_rate < DISABLE_HIT_RATE:
                    self._disabled_until[source] = now + DISABLE_SECONDS
                    self._mult[source] = 1.0
                    actions.append(
                        f"{source} 窗口{calls}调命中率{hit_rate:.0%}"
                        f"<{DISABLE_HIT_RATE:.0%} → 停用"
                        f"{int(DISABLE_SECONDS / 60)}分钟")
        return actions

    def summary(self) -> str:
        """单行分源摘要（周期日志用）。"""
        now = time.time()
        parts = []
        for source in sorted(self._ev):
            counts: dict[str, int] = {}
            for _, k in self._ev[source]:
                counts[k] = counts.get(k, 0) + 1
            if not counts:
                continue
            calls = counts.get("search_call", 0)
            hits = counts.get("search_hit", 0)
            dl = counts.get("dl_call", 0)
            dlok = counts.get("dl_ok", 0)
            if self.disabled(source, now):
                st = "停用"
            elif self.rate_mult(source) < 1.0:
                st = f"节流×{self.rate_mult(source):.2f}"
            else:
                st = "正常"
            parts.append(
                f"{source}[{st} 检索{calls}/中{hits}"
                f"({hits / calls:.0%}若调) 下载{dl}/成{dlok} "
                f"风控{counts.get('429', 0) + counts.get('403', 0)}]")
        return " ".join(parts)

    # ------------------------------------------------------------------
    # 落盘 / 恢复（只保策略态与累计计数）
    # ------------------------------------------------------------------

    def save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"mult": self._mult,
                       "disabled_until": self._disabled_until,
                       "totals": self.totals}, f, ensure_ascii=False)
        os.replace(tmp, self.path)

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            doc = json.loads(open(self.path, encoding="utf-8").read())
        except (json.JSONDecodeError, OSError):
            return
        self._mult = {k: float(v) for k, v in (doc.get("mult") or {}).items()}
        self._disabled_until = {k: float(v) for k, v in
                                (doc.get("disabled_until") or {}).items()}
        self.totals = doc.get("totals") or {}
