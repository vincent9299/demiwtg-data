# 交接文档：三机采集护航（2026-09-06 16:30 交接）

给接手护航的会话：本文档自包含，配合 `telemetry/JOURNAL.md`（事件史）
与 `DEPLOY.md`（部署/运行手册）食用。

---

## 一、使命

三机分片采集（283 概念批任务）持续运行中。护航职责：

1. **每 45 分钟巡检一轮**：健康（反爬/停摆/崩溃）+ docs 抽样质量 review；
2. 按发现**优化管线代码**（修完必须过冒烟再推，A/B 机 git pull 生效）；
3. 一切发现/处置**记档** `telemetry/JOURNAL.md`（按轮次编号）；
4. 目标：**高质量 docs**（壳页率 <20%、错页混入≈0、图文绑定保真）。

## 二、当前运行态（交接时刻）

| 进程 | 位置 | 管理方式 | 停止/重启 |
|---|---|---|---|
| supervise（分片 0/3） | pipeline-a 远端 nohup | 自愈：flow 崩溃 5s 重拉 | `ssh pipeline-a 'pkill -f "python -m supervise"'`（**别用**，除非真要停） |
| supervise（分片 1/3） | pipeline-b 远端 nohup | 同上 | 同上 |
| supervise（分片 2/3） | 本机 persistent bgp `bgp_0770f36cc001YnomBo2I9xshMW` | 同上 | 挂了照下方命令重建 |
| ops_watch.py（5min 采样） | 本机 persistent bgp `bgp_0766115c3001lN2cSz0Bfoy6Ta` | 采样+异常记档 | 同上 |
| **patrol.py（45min 巡检）** | 本机 persistent bgp `bgp_0778803be001wa8QXQ53HXVFpS` | 健康+docs 抽样画像 | 同上 |
| preview.py:8901 | 本机 persistent bgp `bgp_0771aef72001XFzh1XLMn05iJX` | 概念列表→图墙→docs 段落 | 同上 |

本机 supervise 重建命令（bgp 挂掉时）：

```
cd /home/ubuntu/demi/demiwtg-data && /home/ubuntu/demi/.venv/bin/python -m supervise --stall-minutes 20 -- --concepts /lhcos-data/demiwtg-data/concepts_batch_200.json --dataset /home/ubuntu/lake --alias-cache /home/ubuntu/lake/alias.json --blob-root /lhcos-data/demiwtg-data/datasets/demiwtg --shard 2/3 --quota-passes 2 --docs-pages 20 --vlm-concurrency 4 --search-concurrency 6 --download-concurrency 8 --instance-concurrency 4 --log-every 20
```

远端 A/B supervise 重建（如整进程死了）：

```
ssh pipeline-a 'cd ~/pipeline/demiwtg-data && nohup ../venv/bin/python -m supervise --stall-minutes 20 -- --concepts /lhcos-data/demiwtg-data/concepts_batch_200.json --dataset ~/lake --alias-cache ~/lake/alias.json --blob-root /lhcos-data/demiwtg-data/datasets/demiwtg --shard 0/3 --quota-passes 2 --docs-pages 20 --vlm-concurrency 4 --search-concurrency 6 --download-concurrency 8 --instance-concurrency 4 --log-every 20 > ~/lake_supervise.log 2>&1 < /dev/null & echo ok'
```

（B 机把 `--shard 0/3` 换 `1/3`）

## 三、巡检操作手册（每 45 分钟）

### 3.1 读最新轮

```bash
python3 -c "
import json
rows=[json.loads(l) for l in open('/lhcos-data/demiwtg-data/telemetry/patrol.jsonl')]
r=rows[-1]
print('轮次', r['round'], '| 告警:', r['health']['alerts'])
print('docs 抽样:', r['docs']['shell_rate'], '壳页率 |', r['docs'].get('alert',''))
for p in r['docs']['picks'][:8]:
    print(f\"  {p['concept']} [{p['authority']}] {p['title'][:30]} {p['n_passages']}段/{p['n_images']}图\")"
```

### 3.2 判读与处置表

| 信号 | 判定 | 处置 |
|---|---|---|
| `引擎错误率>30%` 告警 | 反爬嫌疑 | 连续 **2 轮** >50% 才动手：`operators/search.py` 该引擎 `limits` 降速（rate 减半），或摘出 `ROUTE_TABLE`；改完过 `smokes.search` 推送 |
| `图像零增长` 告警 | 分片停滞 | `ssh <host> 'tail -3 ~/pipeline/demiwtg-data/logs/supervised_flow.log'` 看栈；supervise 20min 自愈兜底，先观察一轮 |
| 重启计数 +N 频繁 | 崩溃循环 | 看日志栈定位；常见两类：rc=2=代码与参数不匹配（查 A/B 是否 git pull 到最新）；rc=1=代码 bug（读栈修） |
| docs 壳页率 >40% | 质量门/抽取失效 | 抽 2-3 条 `picks` 的 url，`cat` 对应 pages/*.md 看原文；若原文是壳（登录墙/JS 渲染失败）→ 正常认缺；若原文有货但被滤 → 查 `extract_passages`（链密度阈值 0.35 / `_MIN_PASSAGE` 120） |
| docs 混入错页 | 相关性过滤漏洞 | 用 `relevance_score`（`operators/text_engines.py`）复算该 title，调阈值/规则；**改完必须造反例验证**（看 JOURNAL Round 0 的 Bolt 案例） |
| 图像质量异常（同图大量重复） | 引擎召回退化 | toutiao 已知嫌疑（9 行 2 唯一图）；持续则摘源 |

### 3.3 人工 review docs（每轮 2-3 条深读）

从 picks 挑可疑的，读原文与段落绑定：

```bash
# 概念详情页（preview 段落级渲染+绑定图）
curl -s "http://127.0.0.1:8901/concept?name=<URL编码概念名>" | grep -A2 passage | head -40
# 或直读页面正文
cat /lhcos-data/demiwtg-data/datasets/demiwtg/pages/<aa>/<sha>.md | head -50
```

关注：段落是否真知识（非导航/菜单）、绑定图是否正文相关、wiki 直取页
是否纯文本（应无 "Jump to content" 等导航字样——有则说明 wiki REST
回退到了浏览器路径且 fit_markdown 失效）。

### 3.4 记档

每轮发现与处置追加 `telemetry/JOURNAL.md`：

```
### Round N（时间，简由）
- 发现…（数据）
- 处置…（commit hash）
- 观察…（下轮跟进项）
```

### 3.5 代码变更纪律（血泪规约）

1. **改前读文件**，改后 `python -m py_compile` + 对应 smoke 全绿；
2. **commit 前必 `git show --stat` 核对文件清单**（今天有提交空壳事故：
   命令超时被杀，提交名有实无——三机 rc=2 循环 2.5h 的根因）；
3. push 后 **A/B 机 git pull**（supervise 崩溃自愈 5s 内吃到新代码；
   没崩的进程不会热更新——要立即生效就 `pkill -f 'venv/bin/python -m flow'`
   （**只杀 flow，别碰 supervise**——今天误杀两次）；
4. A/B 代码 = `~/pipeline/{demiwtg-data,demiflow}` 双仓，都要 pull。

## 四、关键路径速查

| 路径 | 内容 |
|---|---|
| `/home/ubuntu/demi/demiwtg-data` | 主仓（本机源） |
| `/home/ubuntu/demi/demiflow` | 引擎仓（本机源） |
| `/home/ubuntu/demi/.venv` | 本机 Python 环境 |
| `/lhcos-data/demiwtg-data/datasets/demiwtg/{meta,blobs,pages}` | 共享数据湖（COS 挂载） |
| `/lhcos-data/demiwtg-data/telemetry/` | samples/incidents/patrol.jsonl + JOURNAL.md |
| `~/lake/meta/` | 本机分片清单（image-shard-2-of-3 / docs*.jsonl / engine_telemetry.json） |
| `logs/supervised_flow.log`（仓内） | 本机 flow 子进程日志（崩溃栈在这） |
| `~/pipeline/demiwtg-data/logs/supervised_flow.log`（远端） | A/B flow 日志 |
| `~/lake_supervise.log` | supervise 自身日志（重启计数在这） |

SSH：`~/.ssh/config` 已配 `pipeline-a`(10.3.4.14) / `pipeline-b`(10.3.4.16)，
密钥 `~/.ssh/lighthouse_key`。本机内网 10.3.0.14 / 公网 43.160.250.196。

Preview：`ssh -N -L 8901:localhost:8901 ubuntu@43.160.250.196` →
`http://localhost:8901`（概念列表→图墙/docs→原图）。

## 五、架构一句话（详见各文件 docstring）

- **demiflow**（独立 GitHub 仓）：平台=引擎（streaming 流式/lazy 惰性）
  +规范（StreamStage/SearchEngine）+资源（LLM 端点注册表/HTTP 双池限速）
  +调度（run_stages）；`collect/`：net 限速分类重试、fetch_tiers 档位
  轮转、crawl 页面抓取、store 内容寻址幂等清单、resume 断点现算、
  search 引擎注册表+遥测（反爬数据源）、llm 端点。
- **demiwtg-data**：`operators/`（seed/search/download/annotate/crawl/
  concepts/text_engines/page）+ `flow.py` 编排（图像线+docs 线+配额循环）
  + supervise 看门狗 + preview/import_base/merge_shards/ops_watch/patrol。
- 概念模型：`{name, aliases[], carriers, taxonomy[]?}` 三字段+补充；
  行键 `concepts`；清单 image*.jsonl（图）/docs*.jsonl（文），全部
  (sha, concept) 幂等、分片单写者、内容寻址共享。
- docs 线：TextSearch（wiki REST 直取+searxng general，相关性打分过滤）
  → PageFetch（wiki extracts 直取绕浏览器/段落切分+内嵌图绑定/链密度
  过滤/壳页质量门）→ InlineImage（绑定图落 blob）→ DocsSink。
- 图像线：SearchStage（13 引擎扇出+配额驱动 top_n）→ DownloadStage
  （blob 即时原子落盘+行引用化）→ AnnotateSink（VLM 缺席时无标注落盘）。

## 六、在观察项（交接时刻未决）

> **Round 1 增补（16:35）**：so360 已降速 10→2 rps（52% 错误率两轮）；
> 商业域惩罚表上线（amazon/ebay/taobao 等 -40 分，玻璃刮→Amazon 实测
> 出局）；三机 flow 已重启吃进新代码。**改完代码必须重启 flow 才生效**
> （`pkill -f 'venv/bin/python -m flow'` 各机，supervise 5s 自动重拉）——
> git pull 只更新磁盘代码，不热加载。

1. **so360 错误率 52%**（Round 0 遥测）——连续 2 轮 >50% 则降速/摘除；
2. **toutiao 同图重复召回**（9 行 2 唯一）——质量嫌疑，攒数据再判；
3. **A/B docs 线未开始**（图像配额段未完）——正常排队，非故障；若图像
   配额完成后 docs 仍 0 行，查 A/B 的 demiflow 是否 pull 到遥测版本；
4. 繁简标题不匹配（金鷹獎 vs 金鹰奖）——候选改进，不急；
5. VLM 端点缺席：全部图像无标注（gate 验收后置）——等用户提供端点后
   backfill 补标。

## 七、历史坑索引（JOURNAL 有全量）

cosfs rename 失败→直写回退；cosfs 最终一致性（大文件跨机校验）；
supervise 分片监看路径；asyncio.Lock 跨 loop（每轮重建 stages）；
run_stages 收尾清注入 mock（net/llm 注入层独立）；wiki UA 403（API_UA）；
searxng 引擎死名单（general 锁 google,bing）；flickr 相对图链（绝对 URL
守卫）；fit_markdown 失效回退 raw（链密度过滤）；消歧义页（REST 识别
丢弃）；短别名词面混入（词边界+标题主部词数）；NULL concepts 脏行
（sink 拒绝+preview 兜底）。

---

交接完毕。接手会话从「3.1 读最新轮」开始即可。
