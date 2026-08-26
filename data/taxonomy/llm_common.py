#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""llm_common.py — LLM 调用公共设施（gen_taxonomy_kb / gen_instance_kb 共用）。

- OpenAI 兼容 Chat Completions 接口：LLM_BASE_URL 指向任意兼容端点
  （OpenAI / 通义(Qwen) / DeepSeek / 本地 Ollama 等），不需要专用 SDK。
- 联网检索：LLM_WEB_SEARCH=1 且端点为 OpenAI 官方时走 Responses API 的
  web_search_preview，模型对不确定的实体自动联网核实；其余端点忽略并警告。
- JSON 提取容错（剥代码块/前后杂文）；生成失败指数退避重试 4 次。
- JsonlCache：断点续跑缓存，一行 {key, ok, rec}，重启自动跳过已完成项。
"""
from __future__ import annotations

import json
import os
import threading
import time

API_KEY = os.environ.get("LLM_API_KEY", "")
BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
WEB_SEARCH = os.environ.get("LLM_WEB_SEARCH") == "1"
TIMEOUT = float(os.environ.get("LLM_TIMEOUT", "60"))
TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.3"))


def make_client():
    """懒加载 openai 包：dry-run 不需要。"""
    from openai import OpenAI
    return OpenAI(api_key=API_KEY, base_url=BASE_URL, timeout=TIMEOUT)


def extract_json(text):
    text = (text or "").strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    s = text.find("{")
    e = text.rfind("}")
    if s != -1 and e != -1 and e > s:
        try:
            return json.loads(text[s:e + 1])
        except Exception:
            pass
    return None


def _chat(client, system, user):
    resp = client.chat.completions.create(
        model=MODEL,
        temperature=TEMPERATURE,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        response_format={"type": "json_object"},
    )
    return extract_json(resp.choices[0].message.content or "")


def _chat_nojson(client, system, user):
    resp = client.chat.completions.create(
        model=MODEL,
        temperature=TEMPERATURE,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
    )
    return extract_json(resp.choices[0].message.content or "")


def _responses(client, system, user):
    resp = client.responses.create(
        model=MODEL,
        temperature=TEMPERATURE,
        tools=[{"type": "web_search_preview"}],
        input=[{"role": "system", "content": system},
               {"role": "user", "content": user}],
    )
    return extract_json(getattr(resp, "output_text", "") or "")


def generate(client, system, user, use_responses=False):
    """一次生成：联网端点失败回退 chat；response_format 不兼容回退纯 chat；4 次重试。"""
    last = None
    for attempt in range(4):
        try:
            if use_responses:
                try:
                    return _responses(client, system, user)
                except Exception:
                    return _chat(client, system, user)
            try:
                return _chat(client, system, user)
            except Exception as e:
                if "response_format" in str(e) or "json_object" in str(e):
                    return _chat_nojson(client, system, user)
                raise
        except Exception as e:
            last = e
            time.sleep(2 ** attempt)
    raise last


def want_responses():
    if not WEB_SEARCH:
        return False
    ok = BASE_URL in ("https://api.openai.com/v1", "https://api.openai.com")
    if not ok:
        print("[warn] LLM_WEB_SEARCH=1 仅 OpenAI 官方端点支持联网检索；已忽略。",
              flush=True)
    return ok


def require_api_key():
    if not API_KEY:
        print("错误：未设置 LLM_API_KEY 环境变量。"
              "真实生成前请先 export LLM_API_KEY=... LLM_BASE_URL=... LLM_MODEL=...")
        raise SystemExit(2)


class JsonlCache:
    """断点续跑缓存：一行 {key, ok, rec}。"""

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)

    def done_keys(self):
        keys = set()
        if not os.path.exists(self.path):
            return keys
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("ok"):
                    keys.add(r["key"])
        return keys

    def append(self, key, rec, ok):
        line = json.dumps({"key": key, "ok": ok, "rec": rec},
                          ensure_ascii=False)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def records(self):
        recs = {}
        if not os.path.exists(self.path):
            return recs
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("ok") and r.get("rec"):
                    recs[r["key"]] = r["rec"]
        return recs


def add_common_args(ap):
    ap.add_argument("--branch", default="",
                    help="只处理 path 含该子串的对象（如 '内容作品 IP'）")
    ap.add_argument("--limit", type=int, default=0, help="最多处理 N 条（试点用）")
    ap.add_argument("--workers", type=int, default=4, help="并发线程数")
    ap.add_argument("--delay", type=float, default=0.0, help="每条提交后的间隔秒（限流）")
    ap.add_argument("--overwrite", action="store_true", help="重生成已缓存的对象")
    ap.add_argument("--dry-run", action="store_true", help="不调 API，仅打印 prompt")
    ap.add_argument("--write", action="store_true", help="把缓存合并回数据文件")
    return ap
