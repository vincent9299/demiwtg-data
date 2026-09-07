# demiwtg 护航交接（2026-09-07 17:15Z 全量重写版）

> 新窗口开场白：**"读 HANDOVER.md 接手护航"** —— 然后按 §三 巡检。
> 本版覆盖 09-06 16:39Z 至 09-07 17:15Z 的全部演化（旧版口径已失效）。

---

## 一、使命

45 分钟~1 小时巡检七机 + docs 质量 + 记档（telemetry/JOURNAL.md）+ 持续优化。
人工门原则：拓扑变更/切概念波/杀源等动作先报用户拍板。

## 二、当前运行态

**七机舰队·二维正交架构**（组间按源分组 × 组内概念分片）：

| 机 | 角色 | 分片 | 访问 | 备注 |
|---|---|---|---|---|
| 本机(43.160.250.196) | SG 组 | --shard 4/5 | 本地 | supervise 持久 bgp |
| pipeline-a(10.3.4.14) | SG 组 | --shard 0/5 | ssh lighthouse_key | |
| pipeline-b(10.3.8.9) | SG 组 | --shard 1/5 | ssh lighthouse_key | |
| pipeline-c(10.3.0.17) | SG 组 | --shard 2/5 | ssh lighthouse_key | 09-07 新接 |
| pipeline-d(10.3.8.9 同段) | SG 组 | --shard 3/5 | ssh lighthouse_key | 09-07 新接 |
| pipeline-e(111.230.130.55) | CN 组 | --shard 0/2 | ssh cn_key 公网 | 广州；跨境 SSH 偶发 banner 超时 |
| pipeline-f(159.75.21.11) | CN 组 | --shard 1/2 | ssh cn_key 公网 | 广州 |

- **SG 池**（西方引擎 ~44 个：google images/qwant/artstation/pixiv/
  wikicommons/inaturalist/…）：五台 settings 已规范化（单 engines 键，
  CN 五源 baidu/toutiao/huaban/sogou/quark 显式 disabled）
- **CN 池**（baidu/toutiao/huaban/sogou/quark/bing/yandex + docs 检索
  baidu/sogou/sogou wechat/360search/quark）：被墙源显式关闭
- supervise 满参数：--search-concurrency 12 --download-concurrency 24
  --instance-concurrency 8 --top-n 4 --vlm-concurrency 16
  （VLM 探活跳过中——端点未部署，quality 全 None 属预期）
- 重建命令在 git log 与本表；supervise 双清单停摆判定（image+docs 任一
  增长即续期）
- webgate（searxng 网关）：每机 127.0.0.1:8080，guard.py 看门狗常驻
  （探活 30s 自动重拉）
- **存储**：SG 桶 lhcos-368f6（ap-singapore）/ GZ 桶 lhcos-cee54
  （ap-guangzhou，CN 组专用）——**用户拍板不同步**，分析时分别读；
  清单在各机 ~/lake/meta（本地盘）。pan123 工具链已通
  （~/demi/pan123，凭据在仓内 creds.json，交付包已取用）

## 三、巡检操作手册

1. **七机健康**（每轮）：
   `pgrep -f "[p]ython -m flow"` + `wc -l ~/lake/meta/image-shard-*.jsonl`
   （注意本机是 4/5，E/F 是 i/2——glob 用 image-shard-*）+
   `grep -c '^Traceback' ~/pipeline/demiwtg-data/logs/supervised_flow.log`
   （阈值不涨即可；E/F 路径在 ~/pipeline）
2. **饥饿检测**：`python3 starvation_report.py`（七机清单×配额→
   饥饿概念+taxonomy 分支；>10% 饥饿 = 扩源信号→报告用户）
3. **源健康**（一等指标）：`python3 source_health.py` v3——
   **有效采集源数（24h/1h 窗口）**为战略指标（目标：业内最多，
   searxng+自定义都要扩）；候选集限定路由可达类目（images+general，
   videos/it 等不查询类目不算）；趋势留痕 telemetry/effective_sources.jsonl
4. **agent 三动作**（人工门）：扩源（改 domain_sources.json 或写
   demi_* 引擎）/杀源（settings disabled+webgate 重启）/复活
   （单机金丝雀→观察→转正，lifecycle 状态机自动排期 72h）
5. **记档**：`cat >> /lhcos-data/demiwtg-data/telemetry/JOURNAL.md`
   格式 `### Round N（时间，标题）+ 要点`，编号接续（当前 ~28）

## 四、关键路径速查

- 仓库：本机 `/home/ubuntu/demi/demiwtg-data`（+`/home/ubuntu/demi/demiflow`
  平台仓）；A/B git pull（github_key）；**C/D/E/F 无 github 通道——代码
  变更用 rsync 按文件推送**（E/F 加 `-e "ssh -i ~/.ssh/cn_key"`）
- 数据湖：`/lhcos-data/demiwtg-data/`（共享桶）；E/F 的 blob 落 GZ 桶
  （同样路径，各自挂载）
- 遥测：`telemetry/{JOURNAL.md,starvation_report.json,source_health.json,
  samples.jsonl}`；engine_telemetry.json 在各机 ~/lake/meta（drain 落盘）
- 领域注册表：`operators/domain_sources.json`（交付包
  taxonomy_source_full_v3.1.csv 转换，1333 节点；改它=路由调整，零代码）
- searxng 引擎模块：`webgate/searxng/searx/engines/demi_*.py`（随仓走）
- preview:8901（全分片共享盘视图）/ jupyter:8890（token demi-quality-2026）
  / pan123 下载工具 ~/demi/pan123/pan123.py

## 五、架构一句话

概念种子（taxonomy 透传）→ 领域路由（注册表最长前缀→searxng 多 bang
并集，活配置启用感知）→ 唯一召回网关 searxng（44+引擎，上游引擎名
归一落清单 source）→ 下载档位轮转（dl 限速表+平台默认兜底）→ 落盘
（blob 内容寻址+清单幂等）。编排=链式 Dataset API
（from_items→map_async×N→run_stream，fn|actor 二元注入）。

## 六、在观察项（接手优先看）

1. **wave2 切换待拍板**：283 概念全队饥饿 0%（吃透），
   `concepts_wave2_3000.json` 已备好——用户口令即切（全队 supervise
   --concepts 换文件重拉；E/F 需 rsync 本地文件）
2. general 引擎产量积累（docs source 溯源 09-07 17:12Z 上线）→
   2-3 轮后出可信杀源名单
3. fandom/safebooru 引擎待开发（注册表已留名：624/319 节点等着）
4. 本机 webgate 的 baidu 引擎 crash（cookie 失效，已禁用无碍；
   CN 机正常）
5. E 跨境 SSH 偶发 banner 超时（重试即通；supervise 自愈不影响采集）
6. VLM 离线标注管线（采集吞吐优先暂缓；quality 字段全 None）
7. CN 组是否加机：wave2 后看 E/F 认缺率拐点（>70% 且 SG 在产→+2 台）
8. **扩源路线（有效源数 24h 基线 15/53，2026-09-07 v3 首跑）**：
   路由喂活 artic/flickr/imgur/pixabay（探测有货、注册表没喂）→
   修 pinterest/unsplash 解析（上游改版）→ fandom/safebooru 开发 →
   T1 key 源注册（NASA/Europeana/TMDB/Rijksmuseum）→ CN 侧
   tuchong/zcool 自定义源；**新源解锁前置步：bang 可达性实测**
   （09-07 教训：+14 解锁源里 9 个上来就死 403，已杀）
9. 09-07 17:5xZ 已杀 9 源（SG 五台 settings disabled+webgate 重启）：
   adobe stock/cara/magnific/mojeek images/picjumbo/privacywall/
   tusksearch/findfiles（403）+ openverse（上游默认启用显式覆盖）；
   E/F 本就未启用；lifecycle 72h 金丝雀复活排期照常

## 七、历史坑索引（会话实证，勿再踩）

1. `pkill -f "python -m flow"` 在 ssh 命令行内含同串会自杀→用 `[p]ython`
2. settings.yml 重复 `engines:` 键：YAML 后键丢弃+searxng first-wins——
   改引擎态必须单键显式重建（生成器模式）
3. webgate venv 无 httpx（引擎模块用标准库）
4. 上游同名引擎 inactive:true 会被 use_default_settings 继承→显式覆盖
5. net 严格登记制：新上游源需 dl 键（或吃 DEFAULT_DL_LIMITS 兜底）
6. 双仓升级顺序：先 demiflow 后 demiwtg，同窗完成（错配窗口
   AttributeError 崩流，supervise 兜底但浪费）
7. searxng `engines` URL 参数无效（用多 bang 并集）；bang 捷径以活配置
   /config 为准（手写表会漂）
8. ssh 内 heredoc 会丢（printf 追加）；ssh nohup 挂会话（setsid+重定向）
9. robots 门：robotparser 默认 UA 被 403 限流→误判全禁（项目 UA 手动拉）
10. 引擎遥测 drain 才落盘（运行中不可差分）；errors 字段是按异常类型的
    dict
11. GZ/SW 双桶：E/F 挂的是自家空桶起家→concepts 用本地文件
    （~/concepts_batch_200.json）
12. cosfs 公网直拉 SG 桶 = 外网流量费（¥120/天级）——跨境走 SG 机
    区域内网读+rsync 推
13. CN 机 playwright/pip 走清华镜像；github 不通（rsync 供码）
14. 波次重分片会清空分片清单重采（blob 幂等，带宽成本可控）

## 八、近期主线 commit（倒序）

- 90e8b4e docs source 引擎溯源 + 健康归一化
- 37b1109 source_health v2 + starvation_report（agent 三动作）
- 4b677f5 领域→源路由 + settings 规范化 + 交付包转换
- 59570e8 demi_inaturalist（官方 API·license 透传）
- a06e31e 合规兜底链（snippet/Wayback/demi_360baike）
- 06f5828 翻页×3 + VLM 探活跳过（吞吐 ×17.5 的主力）
- 6f98567/fdad1a3/510d680/e2012ee 链式 API 回归系列（run_stages/
  map_stage 移除）
- c39ca5e so360 退役；9781dd5 折源终态（searxng 唯一网关）
