# 交接文档：collect_v2 采集系统从零重写（进行中）

日期：2026-08-21（更新）｜ 状态：**全部完成**（含 chain.py 业务编排层 + focus/quality 标注转正，均实网验证）；chain 小批实网：3 实例 1.1 分钟，下载 50/落盘 41/撞车跳过 9，metadata.jsonl 新行含 focus/quality。**当前：夜跑长驻（命令见 §6.1），明日多抽样看 case；待办清单见 §6.1**
旧代码参考仓：`_reference/old_repo`（GitHub 浅克隆，已 gitignore；只可参考纯业务逻辑如源接口细节，V2 契约优先，不得照搬旧架构）。
本文档**取代** `.qoder/handoff_rewrite_collect.md`（那份的"旧代码冻结""booru 一期四源"等口径已全部作废）。
病根证据链仍有效，参考 `.qoder/handoff_probe_retrieval.md`（注意其中 booru 相关内容仅作历史教训，booru 已彻底出局）。

## 0. 工作区现状（重要，先读这条）

- **旧代码已全删**（用户在另一窗口手动执行，git 状态为 deleted 未提交）：`collect/`、`curation/`、`AGENTS.md` 全部不存在。
- 还活着的关键数据：
  - `data/datasets/demiwtg/meta/images.jsonl` —— 31.3 万张图的唯一真相主清单；
  - `data/datasets/demiwtg/blobs/` —— 内容寻址图片字节区（只增不删不改）；
  - `data/datasets/demiwtg/meta/instances.json` —— 实例花名册（约 58k 条，采集全程只读；**其 query 字段混有类属词，新系统禁止信任**）；
  - `state/` —— 运行时状态（整体 gitignore），含 annotate_vlm 队列、DLQ 等旧物。
- `.qoder/` 下三份历史交接文档可读，但本文档优先级最高。
- 数据契约文档（原 AGENTS.md）已删，**契约本身仍然有效**，摘要见 §5。

## 1. 用户定的开发纪律（最高优先级）

1. **一步一步写，用户没讲过的不实现**，一步一确认，不许擅自加功能；
2. **不选词**：检索词直接用 instance 名（语言投影 A/B 未拍板，见 §4）；类目词零容忍、兜底词零容忍、旧 query 字段零信任；
3. **宁缺毋滥**：配额吃不满就认缺，任何环节不存在回落、放宽、扩召回路径；
4. **booru 系是开源数据集，彻底排除**，禁止在任何设计/举例中出现（danbooru/safebooru/yandere/konachan/gelbooru 全部）；
5. 开源数据集统一不走检索模式，单独维护 pipeline（本期不写数据集代码）。

## 2. 用户定案的需求（原文要点）

### 2.1 流式算子链（不严格分阶段 1/2）
六个模块，**一个算子一个文件**（2026-08-20 用户拍板 search 前加种子生成，链路变为）：

```
collect_v2/                    # 仓库顶层新目录（旧 collect/ 已删，无冲突）
├── infra.py        # 基础设施层：并行控制、限速、重试（独立于业务算子）
├── op_seed.py      # 种子算子：中文实例 → seed（语言投影，LLM 判定 + 词表缓存）
├── getsource.py    # 域路由算子：seed → (seed, 源) 配对，配置表驱动
├── op_search.py    # 检索算子：输入 seed → 输出候选元数据；adapter 在此文件内
├── op_download.py  # 下载算子：输入候选元数据 → 输出图片 + 元数据
├── op_annotate.py  # 标注算子
├── op_sink.py      # sink 算子：寻址幂等；数据处理/merge 唯一显式落点
└── chain.py        # 只做算子衔接（队列/流转）——零业务逻辑，硬约束
```

链路顺序：**op_seed → getsource → op_search → op_download → op_annotate → op_sink**。

- 检索算子可以有适配（每源一个 adapter）；
- **数据处理、merge 逻辑只在 sink/链层显性做**，adapter 只产结构化候选，不碰主清单；
- **chain.py 不允许写任何业务逻辑**（用户逐字强调）。

### 2.2 数据源范围
- **开源数据集全排除**（不进检索管道）：bulk_danbooru2023、coco、hf_dataset、**booru 系**；
- **旧版剩下的检索模式源全部支持**：wikimedia、wikimedia_zh、inaturalist、fandom、baidu、huaban_api、bing、toutiao、so360。
- **2026-08-20 虚拟角色向新源探测定案（用户发起 11 源逐一探测）**：
  - 端到端可用 9 源（adapter 已注册，实网验证）：wikimedia_zh、baidu、wikimedia（英文补注册）、
    anilist（GraphQL 只搜 Character）、mal（角色搜索 HTML）、pixiv（ajax，regular 直取、R18 剔除）、
    bing_images、yandex_images（SSR initialState）、deviantart（RSS）；
  - **排除**：ArtStation（401 需 OAuth）、Pinterest（登录墙）、fandom（全局端点被 Cloudflare 拦死，用户拍板本期排除）；
  - **挂起**：Google Images（headless 环境已就绪，代理出口 IP 被 Google 标记返 unusual traffic，待干净 IP）。

### 2.3 基础设施层（用户原话）
并行控制、限速、重试。限速已定：**按源适配，尽量快，但要避免被封**（每源一个速率配置，具体数值实现时按源特性定，如反爬源保守、官方 API 按文档限制）。

## 3. 讨论中用户认可的设计结论

### 3.1 检索算子契约（关键）
- 输出**有界有序候选列表**（top-K，按源原生相关度排序），**不是单条 top-1**；
- 链层消费（§4 拍板更新）：top-N（N 可配置）候选**全部**逐条过「结构过滤 → 真值校验」再进下载，**不是首个幸存者即停**；重复由 sink 幂等去重。列表有序性仍保证幸存者中首条即语义 top-1；
- K 封顶不分页深翻：结构化源（inaturalist）K 可到 10-20；语义检索源（wikimedia/搜索爬虫）K ≤ 5；
- 列表耗尽 = 认缺，绝不放宽条件凑数。

### 3.1.1 数据算子流（2026-08-21 用户拍板）
- 算子链是**数据算子流**（类似 Ray Dataset）：全链路流转统一的 `Item` 记录
  （定义在 op_search.py），各算子在同一 Item 上只追加自己的产出字段，
  不改写上游字段；不设独立的 Candidate/DownloadResult 类型；
- 字段分层：种子（instance/query，instance 即种子实例名，解决了旧 Candidate
  缺实例名的契约缺口）→ 检索产出（content_url/landing_url/declared_*/license/
  author/native）→ 下载产出（data/sha256/ext/actual_*/size_bytes）→
  标注产出（kb_match/richness/caption/identity，失败则全为 None）。

### 3.1.2 inaturalist 真值校验（2026-08-21 用户拍板）
**本期不做**（"必须时再加，控制复杂度"）；§3.4 的 taxon 逐条校验挂起，
inaturalist adapter 开工时再重新拍板位置（当时候选：检索出口）。

### 3.2 过滤口径（2026-08-21 用户拍板更新，覆盖原"过滤三处"定案）
- **下载算子不做任何过滤**：结构过滤（host 白名单/https/声明尺寸粗筛）与分辨率门全部移除；
  理由：优先保障候选列表的语义质量排序，过滤会把对的候选误杀；
- 解码（Pillow 完整解码）仅保留为提取实测元数据（宽高/mime/ext）的手段，
  拒收仅限"不是图"（解码失败），这是正确性验证不是质量筛选；
- **不设分辨率门**（用户拍板：先移除，不要；后续若需要另行讨论位置）；
- 域路由不变：仍在种子流入口侧（op_search 之外），防错配不杀候选；
- **禁令不变**：下载前语义过滤（模型判相关性）——旧系统已证伪。

### 3.3 压缩
**不做**。实测二次压缩收益趋零，且违反 blobs 原始字节不可变契约。任何算子都不放。

### 3.4 真值校验 → 见 §3.1.2（本期不做）

### 3.5 sink 契约（2026-08-20 拍板定稿，实现于 op_sink.py）
- sha256 内容寻址：`data/datasets/demiwtg/blobs/<aa>/<sha256>.<ext>`，临时文件同目录 + os.replace 原子替换；
- **sha 撞车直接跳过**（拍板，覆盖原「并入 instances」口径）：不写 blob、不追加行、不合并；
- **无标注照写**：标注四字段键存在值 null（区分「未标注」与「打分 0」）；
- **多 worker 并发写入保护**（拍板）：fcntl 跨进程锁（.meta.lock，§5 白名单）+ asyncio 进程内锁；
- **字段集最小兼容集**（拍板）：实测值写 width/height，声明值写 orig_width/orig_height，
  含 content_url/landing_url/fetched_at（float 时间戳）/path/instances/queries；
  V2 无信息源的旧概念字段不写（tiers/source_rank/source_score/source_kind/
  source_authorized/credit/query_langs）；存量 31 万行实测键集已逐一比对；
- **落盘方式为追加**（非旧系统全量重写）：旧系统每次 flush 全量重写 443MB 不可持续；
- 去重索引：各 worker 进程内存 sha 集（load_index 全量扫清单构建，快路径）+
  锁内吸收式增量尾扫（_absorb_tail）做跨进程权威判定；
  竞态教训：曾「miss 也推进偏移但不吸收区间内其他 sha」致共享图双写，已修（探针实证）；
- **queries[实例]=真实检索词必须透传，禁止回落成实例名**（旧系统溯源失真第一现场，缺陷 3 的直接修复点；Item 自带真实 query 字段，结构上无法回落）。

### 3.6 旧系统五大缺陷对照（新系统如何根治）
| 旧缺陷 | 新系统对策 |
|---|---|
| 短词优先排序 | 不选词，无词池无排序（§1.2） |
| 早停 | 逐词/逐候选试完，无早停 |
| query 回落造假 | sink 透传真实检索词（§3.5） |
| 域路由缺失 | 过滤三处之第一处（§3.2） |
| 真值在场不用 | inaturalist taxon 出口逐条校验（§3.4） |

## 4. 拍板记录（2026-08-20 新窗口开工前确认完毕）

1. **语言投影** → **A 方向**：aliases 只做同实体名的语言形态投影（中文→英文/拉丁），取不到就跳过该英文源。已澄清 wikimedia(en)/inaturalist 是检索源（官方 API），不是开源数据集。存量 aliases 覆盖 90.5%（西文 89.7%）但混有类目泛词，**拍板：先清洗再启用**——LLM 逐条判"是否同实体别名"，清洗结果落盘后英文源才启用；清洗前英文/拉丁源挂起，本期先跑中文源；query 字段零信任不变；
2. **标注算子消费方** → **端到端**：op_annotate 含 VLM 消费方，且**位于 sink 之前**，链路顺序为 search → download → annotate → sink；
3. **驱动方式与配额 N** → **无状态全量种子流**：输入为种子 instance/别名（可提前做域路由过滤），N 可配置（如 top3），**top-N 候选全部过管道**（不是首个幸存者即停），重复靠 sink 幂等去重；不做缺口驱动的存量检查；
4. **AGENTS.md 重写** → 未问，不阻塞 infra.py，后续再谈；
5. **重试细节** → **分类重试**：确定性失败（403/404 等）不重试、直接认缺；瞬态失败（超时/连接重置/429/5xx）有界重试；**不做指数退避**（固定次数 + 固定间隔）；
6. **验收手段** → 未问，不阻塞 infra.py，后续再谈。

### 4.1 op_search 追加拍板（2026-08-20）

- **范围**：adapter 框架 + 代表源先行（一个 API 源 + 一个爬虫源跑通，其余逐个补），不一次写九源；
- **域路由**：在 op_search 之外完成实例×源匹配，op_search 只收路由后的 (种子, 源) 对；（2026-08-20 更新：域路由实体化为 getsource 算子，见 §4.3）
- **别名清洗工具**：原登记为独立脚本不进算子链；**已作废**，2026-08-20 用户拍板实体化为 op_seed 算子（流式 LLM 判定 + 词表缓存，见 §4.3）。

### 4.2 op_sink 追加拍板（2026-08-20）

- **sha 撞车**：直接跳过（不合并 instances，不追加行）——用户四选一拍板；
- **无标注落盘**：写 null（键存在值 null），不是不写字段也不弃图；
- **并发保护**：用户明确「肯定是多 worker 并发的，需要考虑写入保护」→ fcntl 跨进程锁 + asyncio 进程内锁；
- **字段集**：最小兼容集（存量读端已识别字段 + identity；旧概念字段不写）。

### 4.3 op_seed + getsource 追加拍板（2026-08-20，用户发起：「再 search 前面再加一个算子根据中文生成 seed」）

- **算子切分**：op_seed 只管语言投影（中文实例 → seed 形态）；域路由单独成算子 getsource（用户提议）；
- **seed 形态**：每实例产中文本体 seed（必有，query=实例名）+ 西文投影 seed（最多一条）；
  **中文别名变体不产 seed**（守住不选词纪律，用户拍板推荐项）；
- **西文投影来源**：流式让 LLM（本地 qwen 端点）判存量 aliases 的西文候选是否同实体西文名（用户原话），
  不直接消费脏 aliases（旧拍板 query 零信任）；防幻觉：选中项必须是送判候选之一；
- **LLM 判定结果落盘词表 + 增量补判**（用户拍板）：data/datasets/demiwtg/meta/alias_western.json，
  判过的查表零 LLM；判定失败不落表下次重判（宁缺毋滥）；
- **getsource 配置表驱动**（用户拍板），本期最小路由表：zh → wikimedia_zh + baidu（已实现），
  latin → wikimedia（adapter 待建）；inaturalist/fandom 无路由依据挂起；
- **Seed/Item 加 lang 字段**（zh/latin），sink 补写 query_langs={实例:lang}（用户拍板，存量本有此字段）。

### 4.4 虚拟角色向新源 + metadata.jsonl + 判定增强拍板（2026-08-20，用户三点发起）

- **op_seed 判定增强**：desc 全量拼入判定上下文（不截断，用户质疑后定案；契约长度本只 150-350 字），
  判定标准补条款：排除「与之相关的另一个独立实体的正式名称」（新城劲爆颁奖礼对照测试实证）；
- **新源范围**：用户发起 11 源逐一探测，「我在看到现状之前不决策」；探测定案见 §2.2；
- **adapter 契约拍板**：Pixiv regular 直取（不二次跳详情）、R18（xRestrict>0）出口剔除；AniList 只搜 Character；
- **路由拍板**：6 新源对**所有 seed**（zh/latin）全量投递，无召回即认缺不做语言特判（实证：anilist/mal 对中文词 404 属正常认缺）；
- **wikimedia 英文 adapter**：latin 路由悬空拍板补注册（同 commons 端点不限语言，独立 source 名分立限速池）；
- **sink 目标清单**：v2 只写 **metadata.jsonl**（meta/ 白名单已用户拍板扩展），legacy images.jsonl 不碰，
  合并归消费者后续故事；Sink 清单名参数化（默认 metadata.jsonl），不做双写；
- **端到端验证**：smoke_e2e（临时湖全链路断言 + 真湖小批演示），实网 3 实例 38 图 9 源命中，VLM 真实打标。

### 4.5 focus/quality 转正 + top2 + chain 拍板（2026-08-20，源质量实验后用户拍板）

- **背景**：curation/source_quality.ipynb 源质量实验发现活动照（如华航×宝可梦剪彩合影）
  在 kb_match/identity 双高下仍被高估——缺「主体显著度」维度；
- **focus（主体显著度 0-10）转正进 op_annotate**：同一次 VLM 调用内产出（prompt 追加判据段），
  只看构图不看语义匹配；解析必选（缺失即解析失败重试）；
- **quality（综合分）为算子内派生非 VLM 产出**：0.4×kb_match + 0.4×focus + 0.2×richness，
  权重经 49 图原型实验验证（剪彩照 10/5→7.6，独占立绘→9.6+）；
  **只打分不把关口径不变**：quality 供消费层排序/分层，链上不做阈值拒收；
- **top2 策略**：每 (seed, 源) 下载源原生序前 2 条候选（实验实证 rank#1 常比 rank#0 更准），
  落在 chain 的 top_n 参数（CLI 可调，默认 2），不做重排/筛选；
- **不设声明尺寸门**：实验证伪（MIN_EDGE=768 时 baidu/bing 误杀、pixiv 虚高漏放，4 源团灭），
  声明尺寸不可信；若未来需尺寸约束应按下载后实测 actual_width/actual_height 判（本期不做）；
- **chain.py 实现定稿**：业务编排层（用户修正表述：原「零业务逻辑」→「零数据处理逻辑」，
  拓扑/并发/认缺统计是编排职责，禁的是读写/判断 Item 业务字段，只读计数除外）；
  种子流驱动、实例级信号量 + WorkPool + VLM 信号量三级并发、认缺只计数不断链、
  词表每 100 实例定期落盘 + 退出前总账；58k 词表首批 LLM 判定的跑法即 chain 全量消费
  （增量补判，重跑零成本）；
- **实网验证**：chain --limit 3 --top-n 2 写真湖成功（1.1 分钟：投递对 33/有召回 27/
  下载 50/标注 50/落盘 41/撞车跳过 9），新行含 focus/quality；smoke_annotate 同步补 focus 断言全绿。

## 5. 数据契约（原 AGENTS.md 已删，此处摘要即权威）

- blobs 内容寻址、只增不删不改；文件名哈希必须是内容 sha256；
- `data/datasets/demiwtg/meta/` 白名单：只许 `images.jsonl` + `metadata.jsonl`（collect_v2 专属清单，2026-08-20 用户拍板扩展）+ taxonomy 三件套（taxonomy.json/instances.json/alias_western.json，2026-08-21 自已撤销的 data/taxonomy/ 迁入，入 git）+ `.meta.lock`；不建派生索引、不建审计日志、不存放运行时状态；标注字段集 kb_match/richness/caption/identity/focus/quality（focus/quality 为 2026-08-20 拍板追加）；
- images.jsonl 是唯一真相，一张图一行按 sha256 去重；instances 字段只存实例名（V2 落盘口径见 §3.5：追加写 + 撞车跳过，旧「upsert 合并」已废）；
- 运行时状态进顶层 `state/`（gitignore），不进 meta/、不进 data/、不进 git；
- 仓库根由 `--meta`（默认 `data/datasets/demiwtg/meta`）向上四级推导；跨模块 import 用 `from <模块>.<文件> import`；
- 代码只进仓库根模块目录（collect_v2/ 为本次新增）；
- 现有 images.jsonl 记录字段参考（新记录必须逐字段兼容）：sha256/ext/source/source_kind/source_authorized/license/author/credit/width/height/orig_width/orig_height/size_bytes/mime/instances/queries/query_langs/asset_ids/landing_url/content_url/fetched_at/path（+ 标注字段 kb_match/richness/caption）。

## 6. 下一步（开发顺序）

1. ~~**第一步：`collect_v2/infra.py`**~~ **已完成**（asyncio + httpx；并行/限速/分类重试均按 §4 拍板实现，另新增 `stream()` 流式原语供下载用；冒烟 `python3 -m collect_v2.smoke_infra` 7 项全过）；
   - ~~**第二步：`collect_v2/op_search.py` 框架 + 代表源**~~ **已完成**（wikimedia_zh 打 commons.wikimedia.org；baidu 用旧系统经验：www.baidu.com 预热拿 cookie、middleURL 优先不用加密 objURL、尺寸取 URL 查询串；冒烟 `python3 -m collect_v2.smoke_search` 6 项全过，实网两源实测有召回）；
   - ~~**第三步：`collect_v2/op_download.py`**~~ **已完成**（无过滤、20MB 流式封顶、解码提元数据、按源下载头；冒烟 `python3 -m collect_v2.smoke_download` 7 项全过；实网复验通过：wikimedia_zh rank0 原图 2288x1712 jpeg、baidu rank0 500x667 webp，query 透传正确）；
   - ~~**第四步：`collect_v2/op_annotate.py`**~~ **已完成**（同口径 prompt + 新增 identity 字段；VLM 失败无标注放行；只打分不把关；冒烟 `python3 -m collect_v2.smoke_annotate` 6 项全过；真实 VLM 验证：慕田峪长城实图 kb_match=9/identity=True/caption 正常）；同步完成全链路 Item 化（op_search/op_download/两个冒烟）；
   - ~~**第五步：`collect_v2/op_sink.py`**~~ **已完成**（撞车跳过/写 null/fcntl+asyncio 双锁/最小兼容字段集/追加写/吸收式增量尾扫；冒烟 `python3 -m collect_v2.smoke_sink` 8 项全过含跨进程并发，30 轮压测无随机失败；真实链路端到端验证：search→download→真实 VLM→sink 全通；Item 新增 local_path/fetched_at 落盘产出字段）；
   - ~~**第六步：`collect_v2/op_seed.py` + `collect_v2/getsource.py`**~~ **已完成**（用户发起：search 前加种子生成；语言投影流式 LLM 判定 + 词表落盘增量补判；中文别名变体不产 seed；域路由配置表驱动最小路由表；Seed/Item 加 lang，sink 补写 query_langs；冒烟 `python3 -m collect_v2.smoke_seed` 7 项全过；真实 LLM 判定验证：慕田峪长城/跳绳/大熊猫选名正确、笛子认缺正确）；
2. 之后逐个算子写，**每个算子开工前先跟用户确认契约**（用户纪律：没讲过的不实现）；
3. 顺序（已按拍板调整）：infra → op_search → op_download → op_annotate → op_sink → **op_seed + getsource（2026-08-20 补在 search 前）** → chain（最后串，零业务逻辑）；
4. 每步写完跑最小冒烟再进下一步。

### 6.1 开工指引（2026-08-21 更新）

**阻塞项（等用户）**：无。

**当前步骤：chain.py 已完成并实网验证（见 §4.5），夜跑长驻中**：

```bash
# 夜跑（全量 58k 实例，断点续跑安全：sink sha 去重 + 词表增量补判）
cd /tank/demiwtg && env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u ALL_PROXY \
  PYTHONPATH=/tank/demiwtg nohup python3 -m collect_v2.chain --top-n 2 \
  --vlm-concurrency 16 --pair-concurrency 24 > logs/chain_night.log 2>&1 &
# 抽样看 case（明日）：--shuffle 固定种子打乱 + --limit 控量
python3 -m collect_v2.chain --shuffle 42 --limit 20 --log-every 1
# 进度：tail -f logs/chain_night.log（每 20 实例一行）
```

**2026-08-20 夜拍板：优先跑虚拟角色**：从 taxonomy.json 「虚构角色 IP」子树聚合出 3601 实例
（0 孤儿名），过滤视图落 state/collect/instances_fiction_chars.json（运行时产物可重建，
不进 data/datasets/demiwtg/meta/ 权威区），chain 加 `--instances state/collect/instances_fiction_chars.json`
专场消费；3601 实例按 0.1 实例/s 约 10 小时跑完，一夜覆盖全部虚拟角色。
重建命令见 git 历史/session：遍历子树 instances 名单去重 → 按 instances.json 原表序过滤。

注意：首批跑 58k 词表 LLM 判定是主要前置开销（每实例一次短文本判定，词表落盘后重跑零成本）；
吞吐瓶颈在 VLM 打标（qwen3.8-27b，TP=2 双卡）；**管道并发须 ≥ vlm 并发**（串行管道下打标在飞数
被管道数封顶，2026-08-20 实测 WorkPool=8 卡死 vlm=16 的错配后拍板参数化 --pair-concurrency，默认 24）；
要跑完全量需多夜续跑（幂等安全），或先确认 vLLM 承载后再调高 --vlm-concurrency（max-num-seqs 256 为上限，
Mamba 架构 cache 块数约束）。

**待办队列（按序）**：
1. 明日抽样看 case（用户计划）：多抽样审 quality/focus 分布，定消费层分层阈值；
2. 其余旧源 adapter：inaturalist、bing（逐个补，每源开工前对契约；inaturalist 无路由依据仍挂起）；
   （huaban_api/toutiao/so360 已迁入；wikimedia 英文 adapter 已补注册；fandom 用户拍板本期排除）；
3. 挂起项：Google Images headless（环境就绪，待干净出口 IP）；
4. §4 遗留两项：AGENTS.md 是否重写精简版、验收手段（旧探针已删）。
（原待办 3「别名清洗工具」已由 op_seed 算子实现，见 §4.3，移出队列。）

**已就绪可复验**：`python3 -m collect_v2.smoke_infra`（7 项）、`python3 -m collect_v2.smoke_search`（6 项）、`python3 -m collect_v2.smoke_download`（7 项）、`python3 -m collect_v2.smoke_annotate`（8 项含 focus/quality 派生）、`python3 -m collect_v2.smoke_sink`（9 项含跨进程并发 + metadata.jsonl 默认清单）、`python3 -m collect_v2.smoke_seed`（7 项含新源全量投递路由）、**`python3 -m collect_v2.smoke_e2e`（实网全链路：3 实例×9 源，临时湖断言 + 真湖小批）**、**`python3 -m collect_v2.chain --limit 3`（实网全链路写真湖，已验证）**；实网已验 12 源检索→下载、真实 VLM 打标（含 focus）、op_seed desc 增强判定（注：本机外网 DNS 偶发瞬时故障，遇到先 curl 对照确认再怀疑代码）。

## 7. 易误解点提醒

- **"开源数据集"的范围比直觉大**：booru 系也被用户定性为数据集，不在检索管道内，别拿它举例；
- **过滤只剩域路由**：下载算子不做任何过滤（§3.2 更新），别再把分辨率门/host 白名单加回去；
- **wikimedia 下载必须用带真实联系方式的 bot UA**：占位邮箱（example.com）会被 robot policy 在下载层 403，检索 API 层却放行，容易误判；已定案用仓库首页 `https://github.com/vincent9299/demiwtg` 作联系方式（实网 200）；
- **"不选词"不等于不做语言处理**：语言投影（A 方案）若获批，是"同实体名字的形态转换"，不是词池选择——两者界限要在实现时守住；
- **中文别名变体不是 seed**：「慕田峪/慕田峪关」这类变体不产 seed（不选词纪律），只有实例名本体 + 最多一条 LLM 判定合格的西文投影；别把 aliases 直接当检索词用（query 零信任）；
- **alias_western.json 在 data/datasets/demiwtg/meta/ 下**：是 op_seed 专属持久化产物，在 meta/ 白名单登记（2026-08-21 随 taxonomy 三件套迁入），不是「派生索引」；值为 null 表示判过无合格投影（认缺），键不存在才是未判过；
- **sha 撞车是「直接跳过」不是「合并 instances」**：§3.5 已按 2026-08-20 拍板定稿，旧口径（并入实例名）作废；后命中实例的关联就是丢，用户接受；
- **sink 的跨进程去重是「内存快路径 + 锁内吸收式尾扫」两层**：改 op_sink 时别破坏吸收式推进语义（miss 也推进偏移但不吸收区间内其他 sha 会导致撞车双写，已有探针实证）；
- **2026-08-21 夜跑崩溃教训（两条）**：① 主因是 chain 认缺缺口——infra.stream 契约是读流阶段
  网络异常原样上抛不重试（重头下载代价大，认缺），但 process_pair 只捕了 InfraError，
  裸 httpx 异常（RemoteProtocolError）击穿 gather 杀全进程；已补捕 httpx.HTTPError 双保险。
  ② 结构隐患：yield 曾写在重试 for 循环内（异常路径难推理），已重构为建流/重试循环与 yield
  彻底分离。长驻链路的认缺层必须同时覆盖基础设施异常族与传输层原始异常；
- **2026-08-21 提速演进（两段，后者推翻前者）**：① 曾把 Sink.known() 快路径前置到 chain
  （打标前跳撞车图省 VLM），实测撞车区 VLM 零消耗；② 用户拍板职责清晰优先：删除
  known()（sink 回到单一职责：只写盘 + 为自己去重而读），改由第一算子 op_coverage 承接
  实例级跳过——启动期从 metadata.jsonl 的 instances 字段现算 {实例:图数}，纯内存不落盘，
  chain 加 --skip-covered N（0=不启用；先全覆盖用 1，回补缺口用更大值），过滤在
  shuffle/offset/limit 之前；跨实例 sha 撞车实例级看不见，仍由 sink 撞车去重兜底；
  实测 skip-covered=1 对专场 3601 跳 240；同时拍板实例并发默认 4→16；
  另：「判无后 LLM 造西文名」讨论过未启用（泛词翻译风险，用户拍板先不做）；
- **2026-08-21 吞吐瓶颈定性（vLLM 日志实证）**：Running 2-7 时生成吞吐恒 150-195 tok/s，
  batch 加大不涨 → GPU 算力饱和（GPU1 有外部任务占 19.5GB），再加客户端并发无效；
  剩余提速杠杆需拍板：MAX_EDGE 1024→768（视觉 token 减半但改旧口径）/ caption 缩短 /
  外部任务释放 GPU；每源限速是反扒生命线不动；
- **2026-08-21 凌晨网络事故**：宿主直连出口全断（baidu/so360/toutiao/bili curl 全超时，
  load 23），仅代理 192.168.10.109:10808 部分可用（google 通、anilist 不通）；
  chain 认缺兜底如设计工作（实例照常推进、进程不死），网络恢复后自动正常产出；
- **chain.py 是业务编排层，硬约束是「零数据处理逻辑」**（用户修正过表述，原「零业务逻辑」有歧义）：
  拓扑/流转顺序/并发策略/认缺统计是编排职责可以写；禁的是基于 Item/Seed 业务字段做判断/筛选/改写（只读计数除外）；数据处理只能在算子文件里；
- **anilist 404 的罗马字拼写变体问题（2026-08-20 查明）**：AniList `Character(search:)` 是精确匹配，
  op_seed 的 Hepburn 投影（Tanjiro）与库内拼法（Tanjirou）不一致即 404 认缺；汉字原名可命中。
  本期按认缺处理不改 adapter（宁缺毋滥），若日后要提召回，方向是 aliases 里带库内拼法的候选或投影 prompt 补「长音变体」提示；
- **anilist/mal 对中文词返 HTTP 404 是正常认缺不是源故障**：两源索引不含中文词（AniList GraphQL 无命中返 404、MAL 返 404 页），中文 seed 的覆盖本就靠 zh 系源，不要为此改 adapter；
- **pixiv content_url 是从缩略推导的 regular master**：搜索接口 url 是 250 方图，`_regular_url` 去 `/c/250x250_80_a2/` 前缀 + `_square1200→_master1200`，`_p0` 有无原样保留（单页作无 _p0，硬塞会 404，实证过）；下载必须带站内 Referer；
- **infra 双池代理**：海外源走 192.168.10.109:10808（_PROXY_SOURCES），国内直连；直连池 httpx 默认 trust_env 会捡环境代理，跑前必须清环境里宕机的 100.89.199.67 残留（AGENTS.md §7，smoke_e2e 头部已示范）；冒烟注入 mock 时注意按源归池 set_client(proxy=...)；
- **v2 不写 images.jsonl**：sink 目标清单是 metadata.jsonl（§4.4），别把 manifest 切回旧名或双写；
- 旧交接文档 handoff_rewrite_collect.md 的"一期三源/booru 二期/旧代码冻结"表述全部作废，以本文为准；
- git 工作区有大量 deleted 未提交（旧代码删除），新窗口不要误 `git checkout` 恢复；
- `_reference/old_repo` 是旧代码 GitHub 浅克隆（已 gitignore）：**只参考纯业务逻辑**（源接口端点、反爬技巧、字段取舍），不得照搬旧架构，与 V2 契约冲突时以 V2 为准；长期保留不删。
