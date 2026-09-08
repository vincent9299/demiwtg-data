"""kb 线冒烟：构造 mini multistream dump（覆盖关键页面形态）→ flow_kb 全链。

覆盖形态：普通条目（多级章节/分类/内链/文件链/QID 线索）、redirect、
消歧义页、非主名字空间（应被过滤）、繁体前缀（分類:/檔案:）。
断言：字段值正确、ns 过滤生效、幂等重跑零新增。
运行：.venv/bin/python smokes/kb_dump.py
"""

from __future__ import annotations

import bz2
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile

NS = "http://www.mediawiki.org/xml/export-0.11/"

PAGES = [
    # (title, ns, is_redirect, redirect_target, text)
    ("长城", 0, False, None, (
        "'''长城'''是古代防御工程。\n"
        "[[Category:建筑]]\n[[分類:世界遗产]]\n"
        "[[d:Q35676]]\n"
        "==历史==\n建于[[春秋时期]]，参见[[战国]]与[[中国历史]]。\n"
        "===[[秦朝]]段落===\n连接细节。\n"
        "== 图片 ==\n[[File:Great Wall.jpg|thumb|长城]]\n"
        "[[檔案:Zhungguo map.png|地图]]\n"
        "==保护==\n[[UNESCO]]列入名录。链接重复去重：[[unesco]]。"
    )),
    ("万里长城", 0, True, "长城", ""),
    ("长城（消歧义）", 0, False, None,
     "{{disambig}}\n* [[长城]]：防御工程\n* [[长城 (游戏)]]：游戏\n"),
    ("Talk:长城", 1, False, None, "讨论页不进正文源"),
    ("故宫", 0, False, None,
     "北京故宫。{{Wikidata|Q9358}}\n[[Category:宫殿]]\n==概况==\n[[明清]]皇宫。"),
]


def page_xml(title: str, ns: int, redirect: str | None, text: str,
             page_id: int, rev_id: int) -> str:
    red = f"\n    <redirect title=\"{redirect}\" />" if redirect else ""
    return f"""  <page>
    <title>{title}</title>
    <ns>{ns}</ns>
    <id>{page_id}</id>{red}
    <revision>
      <id>{rev_id}</id>
      <parentid>0</parentid>
      <timestamp>2026-01-01T00:00:00Z</timestamp>
      <contributor><username>smoke</username><id>1</id></contributor>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text xml:space="preserve">{text}</text>
    </revision>
  </page>"""


def build_dump(path: str) -> None:
    body = "\n".join(
        page_xml(title, ns, redirect, text, 1000 + i, 9000 + i)
        for i, (title, ns, _is_red, redirect, text) in enumerate(PAGES))
    xml = (f'<mediawiki xmlns="{NS}" xml:lang="zh" version="0.11">'
           f'<siteinfo><sitename>Wikipedia</sitename>'
           f'<dbname>zhwiki</dbname></siteinfo>\n{body}\n</mediawiki>')
    # 多流拼接：两个独立 bz2 流，验证 BZ2File 跨流透明读
    half = len(xml) // 2
    with bz2.open(path, "wb") as f:
        f.write(xml[:half].encode())
    with bz2.open(path, "ab") as f:
        f.write(xml[half:].encode())


def main() -> None:
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    tmp = tempfile.mkdtemp(prefix="kb_smoke_")
    try:
        dump = os.path.join(tmp, "zhwiki-mini.xml.bz2")
        build_dump(dump)
        dataset = os.path.join(tmp, "dataset")
        env = dict(os.environ, PYTHONPATH=repo)
        cmd = [sys.executable, "flow_kb.py", "--dump", dump,
               "--lang", "zh", "--dataset", dataset, "--log-every", "1"]
        r1 = subprocess.run(cmd, cwd=repo, env=env, capture_output=True,
                            text=True)
        print(r1.stdout, r1.stderr)
        assert r1.returncode == 0, "首跑失败"
        manifest = os.path.join(dataset, "kb", "pages-zh.jsonl")
        recs = [json.loads(l) for l in open(manifest, encoding="utf-8")
                if l.strip()]
        assert len(recs) == 4, \
            f"应落 4 页（3 条目 + 1 redirect，Talk 过滤），实际 {len(recs)}"

        by_title = {r["title"]: r for r in recs}
        gw = by_title["长城"]
        assert gw["page_id"] == 1000 and gw["revision_id"] == 9000
        assert gw["categories"] == ["建筑", "世界遗产"], gw["categories"]
        assert gw["images"] == ["Great Wall.jpg", "Zhungguo map.png"]
        assert "春秋时期" in gw["links"] and "战国" in gw["links"]
        assert gw["qid"] == "Q35676", gw["qid"]
        assert [s["title"] for s in gw["sections"]] == \
            ["", "历史", "秦朝段落", "图片", "保护"]
        assert gw["sections"][3]["level"] == 2      # 「== 图片 ==」二级
        assert gw["is_disambig"] is False

        red = by_title["万里长城"]
        assert red["is_redirect"] and red["redirect_target"] == "长城"
        assert red["sections"] == []

        dis = by_title["长城（消歧义）"]
        assert dis["is_disambig"] is True
        assert by_title["故宫"]["qid"] == "Q9358"

        # 幂等重跑：零新增行
        r2 = subprocess.run(cmd, cwd=repo, env=env, capture_output=True,
                            text=True)
        assert r2.returncode == 0, "重跑失败"
        recs2 = [l for l in open(manifest, encoding="utf-8") if l.strip()]
        assert len(recs2) == 4, f"重跑后应仍 4 行（幂等），实际 {len(recs2)}"
        assert "新落盘 0 页" in r2.stdout, r2.stdout
        print("[kb_smoke] 断言全过：解析字段/ns 过滤/幂等重跑 OK")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
