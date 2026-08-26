# AGENTS.md · 项目架构原则与数据约束

本文档是**定死的架构约束**。任何代码修改、脚本新增、数据整理，都必须遵守。修改本文件本身就是一次架构决策，需要显式说明理由。

## 1. 项目分区

```
demiwtg/
├── viewer/                     # 【代码】查看器闭环：tag_tree_explorer.html（+ tag_tree_explorer_en.html 英文平行页，由 build_viewer.py --lang en 从主页生成）+ build_viewer.py + build/、build_en/ 产物（gitignore）
├── benchmark/                  # 【代码】评测基准：按三大题型拆成 vlm/、t2i/、edit/ 三子模块（抽样-出题-判分流水线 + 每子模块 question_dev.ipynb 题目分析 + results_review.ipynb 结果审阅）；各子模块数据区 data/ 不入 git
├── bagel/                      # 【子项目】Bagel 模型训练/评测（独立仓库与 .gitignore，整体不入主仓；见架构决策 2026-08-23）
├── modelhub/                   # 【子项目】LLM 网关（LiteLLM）+ 静态出口代理（mihomo）：本地 vLLM/Galaxy 直连、OpenRouter 走静态住宅 IP；独立仓库，整体不入主仓（见架构决策 2026-08-25）
├── .venv/                      # 【环境】项目公共 Python 环境（conda py3.10，torch 2.6+cu124；原 bagel/env，2026-08-24 提升为公共并由 env/ 改名；不入 git）
├── datasets/                   # 【纯数据】数据集根目录（一数据集一目录；原 data/datasets/，2026-08-24 升为顶层）
│   ├── demiwtg/                #   自建数据集 demiwtg（硬约束见第 2 节）
│   │   ├── blobs/              #     图片原始字节区（内容寻址，不可变，不入 git）
│   │   └── meta/               #     真相区：images.jsonl/metadata.jsonl 与 taxonomy 三件套 + 英文平行两件套 taxonomy_en.json/instances_en.json（三件套与英文两件套入 git）
│   └── .../                    #   开源数据集落盘区（danbooru2024/coco2017 等，不入 git）
├── data/                       # 【代码】数据构建代码（原仓库根五模块中的三个，2026-08-24 收编入 data/）
│   ├── collect_v2/             #   采集（检索→下载→落盘/打标链路编排）
│   ├── taxonomy/               #   体系维护与富化（mount_map / gen_taxonomy_kb / gen_instance_kb / audit_nodes / upgrade_v31）
│   └── curation/               #   数据策展（分析 notebook：dataset_analysis.ipynb 已从 IDE 快照恢复归位，见架构决策 2026-08-24）
├── state/                      # 运行时状态，按模块归属分子目录（不入 git）
│   ├── collect/                #   datasets/（下载过程脚本，只读归档）+ v1 遗留运行时状态（死信/health/runs，只读归档）
│   ├── dataset_index/          #   COCO 标注缓存
│   ├── taxonomy/               #   taxonomy 模块 LLM 断点缓存与审计报告
│   ├── curation/               #   curation 历史分析残留（标签树 CSV、watermark 实验产物等）
│   └── .lancedb/               #   Lance 查询索引
├── logs/                       # 运行日志（不入 git）
├── AGENTS.md                   # 唯一权威约束/说明文档
└── README.md                   # 极简指针，只指向本文档
```

- `datasets/` 下**只是数据存储**：任何代码、页面、生成产物都不许放进去。
- 代码只允许放在 `data/` 下三模块（`collect_v2/`、`taxonomy/`、`curation/`）与仓库根 `viewer/`、`benchmark/`。
- 仓库顶层禁止新增散落的脚本或数据目录（`datasets/`、`data/`、`state/`、`logs/` 是明确登记过的例外；`bagel/` 为登记的独立子项目例外，内部布局自治，不受本仓模块/数据边界规则约束；`modelhub/` 为登记的独立子项目例外（LLM 网关 + 静态代理），内部布局自治，同不受约束；`.venv/` 为登记的公共环境例外，只放环境不放代码）。
- 文档只有两份：`AGENTS.md`（约束）与 `README.md`（指针）。历史过程文档（docs/、子目录 README）已删除，**不再恢复**——过程记录看 git 历史。

## 1.5 标签体系数据契约（两个独立概念，定死；两文件同居 datasets/demiwtg/meta/）

整个标签体系**只存在两个概念**，代码、数据字段、文档一律使用这两个词，禁止再引入其他分类术语（category、leaf、root 已废除）。数据模型以本节为准（原 schema/tag_taxonomy.schema.json 已删除：无校验消费者、与 instances.json 顶层结构不符，勿恢复）。

**两者彻底解耦（架构决策 2026-08-19）**：实例是资产，taxonomy 是视角。实例的生灭与富知识完全不依赖树；树只是展示/导航视图，同一套实例未来可被多套树视角引用。关联关系（谁挂在哪）**不写进任何一方文件**，需要时由消费者从树的 instances 名单现场聚合（`taxonomy/mount_map.py`）。原 build_unified.py（树推导实例表的重建器）已删除：它维护的正是被废除的耦合。

### taxonomy.json —— 树（展示视角）

```
{ "schema_version": "...", "meta": {...}, "tree": <node> }

node = {
  name: str                    # 节点显示名
  path: str                    # 完整路径，' / ' 分隔，从根『demiwtg』起算（前缀精简：域直挂根，无中间层）
  depth: int                   # 根为 0
  children?: [node]            # 子树；末端节点省略
  instances?: [str]            # 挂在本节点下的实例名列表（对 instances.json 的引用，挂载关系的唯一落点）
  knowledge_intro?/aliases?/representative_cases?/related_tags?: [KB 字段，可选；knowledge_intro 为 150-350 字维基百科词条风格]
}
```

### instances.json —— 实例（独立权威源，实体资产库）

```
{ "schema_version": "...", "meta": {...}, "instances": [instance] }

instance = {
  name: str                    # 实例名（全局唯一主键：一个实体只允许一条记录）
  source: "curated" | "llm" | "derived"   # curated=人工精写；llm=LLM 生成；derived=未富化占位（templated 为历史值，不再新写）
  desc?: str                   # 详细介绍（唯一富描述字段；150-350 字，维基百科词条风格：具体知识点，拒绝空话套话）
  aliases?: [str]              # 别名/英文名
  query?: [str]                # 检索扩展词（LLM 生成，含英文/简称）
}
```

- **实例独立于树**：未挂载任何树节点的实例是合法状态（待认领池）；增删树节点不造成实例的创建或删除，富知识（desc/query/aliases）只存在 instances.json。
- **`name` 全局唯一是硬约束**：同一实体的知识字段只维护一份；多处挂载表现为多个树节点的 instances 名单同时含该名字（原 taxonomy_paths 字段已废除：它是树的影子，不进实例表）。
- 没有 `type` 字段：有没有子树看 `children`，挂不挂实例看 `instances`。
- **英文平行两件套**（架构决策 2026-08-24）：`taxonomy_en.json` / `instances_en.json` 与中文两件套同居 meta/、schema 完全同构（英文树根也是 demiwtg 直挂 29 域）；两套数据完全独立，中英实例名空间零交集，英文实例当前无图片关联（不打标、不映射中文富知识）。
- 图片打标只存**实例名**（实体标签，不含路径）——体系演化（改路径/重生成树）不需要迁移图数据。看图入口（viewer 的 build/imgs.js）由 meta/images.jsonl 的 instances 字段现场聚合、相对路径指到 blobs 原图（相对 viewer/ 的 ../datasets/demiwtg/blobs/...），不再建软链树。
- 数据字段定义即契约，改字段 = 改本节 + 同步全部消费代码。

## 2. datasets/demiwtg/ 硬约束（定死，逐条执行）

### 2.1 blobs/ —— 原始字节区（不可变）

```
datasets/demiwtg/blobs/<aa>/<sha256>.<ext>   # aa = sha256 前两位；sha256 = 文件内容哈希
```

- 图片**只增不删、不重命名、不改动**。
- 新增图片必须：先算内容 sha256，再按 `blobs/<aa>/<sha256>.<ext>` 落盘；已存在同名文件则直接跳过（内容寻址天然去重）。
- 文件名中的哈希**必须是文件内容的 sha256**，禁止沿用下载器给的不可信文件名。
- 删除任何旧图片目录之前，必须逐文件验证其内容已存在于 blobs（sha256 比对），否则先并入 blobs 再删。

### 2.2 meta/ —— 真相区（只放真相，别的什么都不放）

**允许的文件（穷举，不允许出现清单之外的东西）：**

| 文件 | 角色 |
|---|---|
| `images.jsonl` | 唯一权威主清单：一张图一行（sha256 + 全部字段），按 sha256 增量 upsert；instances 字段只存实例名，实例名↔图关系由它单点承载 |
| `metadata.jsonl` | collect_v2 专属采集清单（2026-08-20 拍板） |
| `taxonomy.json` | 标签体系树（展示视角，权威源，入 git） |
| `instances.json` | 实例资产库（实体权威源，入 git） |
| `alias_western.json` | op_seed 西文别名词表（LLM 判定增量落盘，入 git） |
| `taxonomy_en.json` | 英文平行标签树（结构同构 taxonomy.json，v3.1 英文底稿入库，入 git） |
| `instances_en.json` | 英文平行实例资产库（结构同构 instances.json，全量 derived 占位，入 git） |
| `.meta.lock` | 跨进程写锁（运行时瞬态） |

**禁止出现在 meta/ 下的东西：**

- ❌ 审计日志（只写不读的账本一律不建；先有读取代码才允许写入）
- ❌ 备份文件（*.bak-*、*.bak-sync 之类）
- ❌ 派生索引（LanceDB、实例名→图反向索引等；需要时由消费者从 images.jsonl 现场聚合）
- ❌ 运行时状态（死信队列 sqlite、健康账本、done flags、COCO 缓存）

**判据（新增任何文件前先回答）：**

1. 有消费者吗？——**必须先有读取它的代码，才允许写入它**。
2. 是真相还是派生？——派生的东西不进 meta。
3. 删掉它会丢数据吗？——丢了数据才是真相；能重建的不进 meta。

### 2.3 运行时状态在顶层 state/（不属于数据湖，按模块归属分子目录）

`state/collect/`：下载过程脚本（datasets/download_all.sh、character_resume.sh、hf_mirror_hf.py，只读归档；HF 数据集落盘区已迁至 `datasets/`）；v1 遗留状态（死信队列 `.dlq_*.sqlite3`、`source_health.json`、`runs/<run_id>/`、`source_registry.jsonl`）只读归档不再写入；`state/dataset_index/`：COCO 缓存；`state/.lancedb/`：Lance 查询索引；`state/taxonomy/`：taxonomy 模块 LLM 断点缓存与审计报告；`state/curation/`：curation 历史分析残留（标签树 CSV、watermark 实验产物等；评测数据已迁入 benchmark/，见架构决策 2026-08-24）。代码约定：仓库根由 `--meta`（默认 `datasets/demiwtg/meta`）向上三级推导（datasets/demiwtg/meta → 仓库根）。永远不进 meta/、不进 datasets/、不进 git。

### 2.4 一致性规则

- `images.jsonl` 是唯一真相；**不建任何派生索引文件**（原 instance_images.json 已废除：双份存储存在一致性漂移风险，需要实例名→图关系时由消费者从 images.jsonl 现场聚合，如 viewer/build_viewer.py）。
- `images.jsonl` 的 instances 字段只应是当前体系的实例名；体系演化后残留的死名打标从 images.jsonl 剥离（无隔离区）。
- 一张图的 instances 变更（改名/隔离）改的是 images.jsonl，**图字节不动**。
- 新元数据字段设计时必须先问"哪个消费者读它"；答案为空就不加。

## 3. 代码模块职责

| 模块 | 职责 | 入口 |
|---|---|---|
| `data/taxonomy/` | 标签体系维护：树审计（audit_nodes 死叶子审查）、挂载聚合（mount_map，只读现算不落盘）、富化（gen_taxonomy_kb 节点 KB / gen_instance_kb 实例知识，各一次 LLM 调用） | 各脚本 `--write` |
| `data/collect_v2/` | 图片采集 v2：infra/算子/chain 三层架构，检索→下载→落盘/打标链路编排（op_seed/op_search/op_download/op_sink/op_annotate，chain.py 串联，smoke_* 分层验证）；存量迁移链（op_backfill 补标算子 + migrate.py 记录驱动入口，images.jsonl → metadata.jsonl） | `data/collect_v2/chain.py`；迁移 `data/collect_v2/migrate.py` |
| `data/curation/` | 数据策展：数据集分析 notebook（dataset_analysis.ipynb，参数写在 cell 内部，直接运行）：① danbooru2024 metadata.parquet 字段字典 + 单字段下钻（取值分布/TopN 覆盖/Gini/均衡化提示/抽样看 case）；② demiwtg 权威清单（metadata.jsonl）得分分布与按源对比、多实例与重复检查、每实例图数分布、按阈值过滤看 case；③ taxonomy 视角：mount_map 现算节点量级/CSV 导出/按节点抽样看图，只读（评测集分析 notebook 已挪入 benchmark/） | notebook 直接运行 |
| `viewer/` | 查看器闭环：页面 tag_tree_explorer.html（+ tag_tree_explorer_en.html 英文平行页，由 --lang en 从主页现场替换生成，单一来源防漂移）+ 构建脚本 build_viewer.py（--lang en 读英文两件套）+ 产物 build/、build_en/（sidecar taxonomy.js/instances.js/imgs.js 与 standalone 单文件，gitignore；英文侧 imgs.js 注入 null，英文版无图）；HTML 与 build/ 同址是 file:// 双击可用的硬要求 | `viewer/build_viewer.py` |
| `benchmark/` | 评测基准：按三大题型拆成三子模块（见架构决策 2026-08-24 三子模块拆分）。**t2i/**（生成）与 **edit/**（编辑）各带完整四件套：抽样（eval_sample.py 分层配额，--filter 一条 duckdb SQL WHERE；edit 版默认叠加编辑适配门）、出题（eval_synthesize.py，Galaxy API；t2i 版含 facet 词表审计、edit 版 9 类 edit_type 轮转 + 每第 5 题知识编辑套）、判分（eval_score.py 调本地 vLLM judge，score/dump 子命令；t2i 版 FACETS 权威源 + φ 映射聚合，edit 版 EDIT_DIMS 三维钳制）、gen_results_review.py（生成审阅 notebook）；**vlm/**（理解）暂不拆代码，只放 notebook。每子模块两个 notebook：question_dev.ipynb（抽样+分布+题库审阅，for 题目构造）、results_review.ipynb（打分/评估结果分析）。评测数据在各子模块 data/ 下（样本图/题库/判分产物，均不入 git，.gitignore 登记）；编辑评分契约 edit/edit_score_prompts.json（ImgEdit 官方原文，随代码入 git） | 各脚本 `--help`；各子模块 `question_dev.ipynb` / `results_review.ipynb` |

> **架构决策（2026-08-25）**：新开 `modelhub/` 独立子项目（用户拍板：可单独 push GitHub、其他机器 pull 直接复用；静态代理全套并入）。定位：本地 LLM 统一接入层——LiteLLM 网关（127.0.0.1:4000，OpenAI 兼容）路由三条线：本地 vLLM（qwen3.8-27b，no_proxy 直连）、Galaxy 专线（qwen3.7-plus，no_proxy 直连）、OpenRouter 通配（进程级代理注入 → mihomo 按域名分流走静态住宅 IP 出口）；静态代理模块即原 `/root/gpu-static-proxy`（mihomo v1.19.30 双层链式：10808 隧道换源 IP → 静态 IP 节点 216.132.205.99；modelhub 只维护「AI API 域名走 STATIC 双层静态出口」一层规则，其余流量 modelhub 视角直连、继承宿主策略、不感知不维护）整体迁入 `modelhub/static_proxy/`（二进制与 GeoIP 库随迁，旧目录作废可删）。定案：① 照 bagel 先例：独立 git 仓库、主仓 .gitignore 整体排除、内部自治（自带 README，不受主仓「文档只两份」约束）；② 机密零入库：.env（API keys）与 static_proxy/config.yaml（节点凭据）只进 gitignore，仓内只有 *.example 模板；mihomo 二进制不入库（fetch_mihomo.sh 下载/旧机拷贝）；③ Python 环境独立：modelhub/.venv（litellm[proxy]==1.98.0 锁定），不碰主仓 .venv（不动主仓 openai/httpx/pydantic）；④ 消费端零改动启用：网关为 OpenAI 兼容端点，既有脚本换 LLM_BASE_URL=http://127.0.0.1:4000/v1 即接入，逐步迁移；上游端点与 key 全部 .env 可配置，启动时 gateway/gen_local_models.py 自动发现各 *_API_BASE 端点的模型并注册进 /v1/models（openrouter/* 通配 litellm 原生展开；Cline 按 OpenAI Compatible 配置网关地址即自动带出模型列表）；⑤ 端口登记（均仅本机监听）：4000 网关 / 7891 mihomo mixed / 9091 mihomo API / 1053 mihomo DNS。
>
> **架构决策（2026-08-24）**：英文版标签体系入库（用户拍板三则：扩白名单同居 meta/、实例名轻量清洗、查看器 --lang en 独立页面）。「融合世界标签体系 v3.1」交付包英文底稿（taxonomy_tree_instances_en.csv：21,406 行，中文路径/英文路径/英文实例清单三列）由 `taxonomy/upgrade_v31_en.py` 建成一套完全独立的英文平行数据（干跑→--apply，照中文版惯例；不动中文三件套与 alias_western.json）。定案：① taxonomy_en.json / instances_en.json 同居 meta/，2.2 白名单扩两行 + .gitignore 例外链放行入 git；② 前缀归一为中文 norm_path 的英文同款（剥根 Fused World Label System + General Classification Tags，换根 demiwtg），底稿缺行的 4 个骨架域与中文同源隐式补齐；③ 底稿实测 119 组翻译撞车（不同中文节点译成同一英文路径，如 炊具/锅具 → Cookware，名单重合度中位数仅 0.02）用户拍板自然合并（名单取并集），另 2 个译名折叠展开隐式节点（中文段『帝王蟹/蟹』译成 King Crab / Crab 两段）→ 英文树 21,291 节点 = 中文树 21,409 - 撞车合并 120 + 折叠展开 2，域级对齐（撞车与折叠清单落 state/taxonomy/en_merge_report.json 供后续精译修复）；④ 英文实例名轻量清洗（按词：全小写/全大写词转首字母大写，全大写缩写与混排词保留），清洗后同名大小写变体合并（name 唯一主键），413,329 → 382,341 全量 source=derived 占位入库（不做富知识、不映射中文知识），清洗合并明细落 state/taxonomy/en_clean_report.json；⑤ viewer 复用：build_viewer.py 加 --lang en，读英文两件套写 viewer/build_en/ sidecar（imgs.js 注入 null——英文实例与 images.jsonl 打标零交集，英文版无图是预期），页面 tag_tree_explorer_en.html 由主页面现场替换生成（标题 demiwtg (EN)、sidecar ?v=1 独立缓存号、fetch 回退改英文两件套），页面入 git、build_en/ 产物不入。结构对齐验证以中文路径列为桥：归一中文路径 21,405 ⊆ 中文树，差集恰为 4 骨架域。纯新增零覆写，故无入库前备份。图数据（images.jsonl/metadata.jsonl）零改动。
>
> **架构决策（2026-08-24）**：`data/datasets/` 整体升为项目根目录 `datasets/`（用户拍板：自建与开源数据集同居一处；data/ 保留原名只住代码）。理由：`data` 作顶层名太泛，且目录里早已住着 collect_v2/taxonomy 两套代码名实不符；拆开后 `datasets/` 语义精准、`data/` 收敛为数据构建代码区。定案：① 同盘 rename 零拷贝（~1.1TB：自建 demiwtg 631G + 23 个开源数据集；24 目录全量在位，blobs/meta 完好）；② 路径改址全链路：data/collect_v2 与 data/taxonomy 的 REPO_ROOT/ROOT 推导补一层（此前代码被搬入 data/ 后已暗指 data/ 而非仓库根，state//logs/ 类路径实已失效，本次一并修复）+ 数据集路径常量去 `"data"` 段，import 实链路断言验证；benchmark t2i/edit 的 eval_sample 默认路径同步；viewer 页面/构建脚本改址并重建 build/ 产物（20,100 实体）；③ .gitignore 例外链改 `datasets/*` 逐级放行三件套；④ 历史决策块旧路径为当时快照不回改；⑤ curation 模块在搬移中被误删的 dataset_analysis.ipynb 已由快照恢复归位 data/curation/（20-cell 终态：基线 16 格逐字对齐 8/22 快照 + 全部补丁/内联写命令按时序重放 + 拆分后删尾四格；抢救过程产物在 state/curation/_nb_recovery/，可清理），路径与 import 已按 data/ 新位置与顶层 datasets/ 调整。本会话未动 .qoder 交接文档与 logs/ 归档脚本（历史快照）。
>
> **架构决策（2026-08-24）**：benchmark 按三大题型拆成三子模块（用户拍板）：`vlm/`（理解）、`t2i/`（生成）、`edit/`（编辑），推翻上一条「评测数据在 benchmark/ 根三目录」布局。定案：① t2i/edit 各带完整四件套（eval_sample/eval_synthesize/eval_score 由原跨赛道脚本拆分，通用工具各留一份子模块自闭环，不建 common）；vlm 赛道协议未拍板暂不拆代码，只放 notebook；② 旧评测数据三目录（eval_v1/eval_v2/taxonomy_sample_cases，合计 ~2GB）用户拍板删除重抽，重抽时指定目录分落三子模块 `data/`（不入 git，.gitignore 登记；仅保留契约文件：出题 prompt 两册与 edit_score_prompts.json 随子模块入 git）；③ notebook 两册变一册：每子模块 question_dev.ipynb（合并原 eval_analysis + eval_review：抽样委托 + 分布分析 + 题库审阅，for 题目构造）与 results_review.ipynb（原 wkbench_review.ipynb 改名+按赛道裁剪：打分/评估结果分析，由各自 gen_results_review.py 生成）；④ 抽样排除集改三子模块 data/samples.jsonl 互斥（跨赛道防重复出题）；⑤ bagel 侧 `run_wkbench.py` 同步改址：DEFAULT_QUESTIONS 改指 t2i 赛道新题库（`benchmark/t2i/data/synth_gen/questions.jsonl`），样本图根目录随 --questions 位置自动推导（题库目录与其上级两级候选，适配各赛道 data/imgs/ 布局）。判分/出题协议本身零改动（FACETS 权威源、φ 映射、三维钳制、9 类轮转配额原样随迁）。
>
> **架构决策（2026-08-24）**：公共环境 `env/` 改名 `.venv/`（用户拍板；最初提议 `.env`，因与 dotenv 密钥文件约定撞名——密钥扫描/搜索排除类工具对 `.env` 有特殊处理——改用 Python 环境惯例隐藏名）。同盘 rename 零拷贝；bin/ 内 100 处旧路径（97 shebang + config 脚本）批量重写，conda-meta/lib 零引用无需动（内部亦无绝对路径软链）；bagel 侧 4 处脚本引用与 Jupyter kernel demiwtg 同步改址；.gitignore 登记改 `.venv/`。历史决策块中的 env/ 旧路径为当时快照，不回改。

> **架构决策（2026-08-24）**：`bagel/env` 提升为项目公共环境 `/tank/demiwtg/env`，`.venv-notebook` 退役（用户拍板）。理由：主仓侧实验（水印检测 pilot 等）需要 torch 推理栈，为它单独建环境浪费且易漂移；环境本体与 bagel 代码仓无关，提升后 bagel 脚本与主仓实验共用一套。同盘 rename 零拷贝；bin/ 85 个 shebang + bagel 内 4 处脚本引用 + conda-meta 批量重写（二进制内嵌串与 conda history 旧路径不影响运行，沿用 2026-08-23 迁移先例）。`.venv-notebook` 唯一消费者是 Jupyter kernel demiwtg，改指 env/ 并补装分析栈（duckdb/ipykernel；pandas 用 env 既有 2.3.3），dataset_analysis.ipynb 全 12 cell 复跑零错误后删除。`models/.venv-vllm` 维持 vLLM 部署专用不动。env/ 不入 git（.gitignore 登记）。

> **架构决策（2026-08-24）**：标签体系升级 v3.1（用户拍板三则：死名保留回挂、新增实例分批入库、英文底稿仅扩名单）。外部交付的「融合世界标签体系 v3.1」终版底稿（29 域 / 21,406 路径 / 29.6 万实例，三批迁移终版：域级前缀替换 + 22 个 IP 域吸收并入通用域 + 知识与学科清理）由 `taxonomy/upgrade_v31.py` 一次性升级进仓（干跑→裁定确认→--apply，幂等：每批实例入库后可重跑刷新树名单）。定案：① taxonomy.json 按底稿重建（21,410 节点 = 底稿 21,406 + 4 个底稿缺失域骨架隐式补齐；schema 1.1.0），节点 instances 名单只留在册名（∩ instances.json，跨节点多挂 9,075 名属契约允许），「IP 分类标签」分支随吸收自然消失；② 失配节点 KB 用名单指纹法抢救（前缀替换名单随行 ⇒ 直接名单/子树名单精确匹配 + Jaccard≥0.85 唯一命中，命中 1,483/1,491，报告落 state/taxonomy/v31_kb_recovery_report.json，未命中 8 条可 gen_taxonomy_kb 重生成）；③ 95 个死名实例（粗伞名，如主战坦克/黄道十二宫，涉及 1,010 次打标）不剥离：保留实例与图，按原挂载路径回挂存活节点（零歧义，无退化）；④ 新增 237,781 实例本次不写 instances.json，按域分组候选清单落 state/taxonomy/v31_pending_instances.json，分批富化入库（用户逐批确认）；⑤ alias_western.json 仅扩名单（+522 条 null 占位，英文实例中英不对齐不做别名填充）。图数据（images.jsonl/metadata.jsonl）零改动。交付包归档 state/taxonomy/v31_交付包/（不入 git）；升级前三件套+metadata.jsonl 物理备份在 state/taxonomy/backup_pre_v31/。
>
> **架构决策（2026-08-24）**：v3.1 新增 237,781 实例占位全量入库（用户拍板，推翻上一条定案④的分批富化后才入库）。理由：树名单只留在册名导致 viewer 只见 5.8 万实例，用户要求全量可见。定案：① 237,781 个待入库名以 source=derived 占位（仅 name，与存量 derived 同构）一次性写入 instances.json（58,229 → 296,010），富知识（desc/aliases/query）留待后续按域分批 LLM 富化；② 重跑 upgrade_v31.py --apply 幂等刷新树名单（丢弃引用 0、死名回挂 95 与 KB 保留 1,493 不变；v31_pending_instances.json 清零）；③ alias_western.json 按既定「仅扩名单」策略 +237,781 null 占位；④ viewer 重建（taxonomy.js 11.1 MB / instances.js 72.9 MB，树名单唯一名 296,010 = 在册数，名单⊆在册校验过）。入库前物理备份在 state/taxonomy/backup_pre_placeholder_ingest/。
>
> **架构决策（2026-08-24）**：标签树路径前缀精简（用户拍板）：根『融合世界标签体系』→ `demiwtg`，废除一级分支『通用分类标签』，29 域直挂根（路径口径 `demiwtg / 域 / 二级 / ...`；历史双树期 IP 路径同样归一）。定案落在 `data/taxonomy/upgrade_v31.py`（norm_path 归一函数：底稿路径与旧树路径统一归一，幂等可重跑不回退旧结构）；底稿根行与中间层行同归 demiwtg 空名单按序合并（合并 1 行，节点 21,409 不变）。重跑 --apply 重建：KB 保留 1,493 / 死名回挂 95 / 实挂引用 346,729 / 唯一名 296,010 全不变；KB 指纹抢救失配 0（上轮已全部归位，本轮路径全精确匹配）。消费端同步：① benchmark t2i/edit eval_sample.py 的 branch_of 注释与层级口径（域现在是 segs[1]，抽样分层粒度随之为 域×二级）；② viewer 页标题改 demiwtg、废除已失效的 IP/通用双树过滤器下拉框、sidecar 缓存号升 ?v=3（build_viewer.py SIDECAR_MARK 同步）；③ curation dataset_analysis.ipynb 注释旧路径示例。图数据零改动；改前备份 state/taxonomy/backup_pre_prefix_simplify/。
>
> **架构决策（2026-08-24）**：评测数据由 state/curation/ 迁入 benchmark/（用户拍板）：eval_v1/（360M）、eval_v2/（893M）、taxonomy_sample_cases/（775M，无代码消费者，人工审阅用）三目录同盘 rename 零拷贝。理由：这些是评测**结果数据**而非运行时状态，与出题/判分代码同模块闭环（沿用 bagel/env 提升的同盘迁移先例）。配套：① 全部引用同步改址——eval_sample/eval_synthesize/eval_score 的默认路径常量改自脚本自推（`Path(__file__).parent`）、三个审阅/分析 notebook、gen_wkbench_review.py、bagel 侧 run_wkbench.py 的题库与图根绝对路径；② .gitignore 登记三目录不入 git（代码仍入库）；③ judge prompt 物化目录随 EVAL_DIR 落在 eval_v2/judge_prompts（仍不入 git）。历史决策块中的旧路径为当时快照，不回改。
>
> **架构决策（2026-08-23）**：覆盖口径带质量门（用户拍板）：op_coverage.load_coverage 新增 min_quality/require_identity 两参（默认 8.0/开，即 notebook 默认过滤同款口径），只数合格行；「有图但全不合格」的实例按 0 图对待继续采，缺 quality 字段的存量迁移行按不合格计。chain 新增 --min-quality/--require-identity（BooleanOptionalAction）。重点下载零合格图实例用 `--skip-covered 1`（当前实测范围 5,001 个）；两门全关退化为旧口径（回归验证与无门时计数完全一致）。分区排序（0 图排队首）自动沿用合格计数。
>
> **架构决策（2026-08-23）**：wkbench 首跑冒烟修复（runner 侧补丁，不动官方模型代码）：edit 任务将输入图过 fp32 VAE 编码，在 bf16 autocast 区内 conv_in 直接炸 dtype 不匹配，且编码产物经 NaiveCache 进 vae2llm 时 accelerate dispatch 钩子挂不上。修法两条，都在 run_wkbench.py：① vae_model.encode 包一层（内部禁 autocast + 显式对齐 VAE 设备，因实例属性影子掉 dispatch 钩子的设备搬运）；② forward_cache_update_vae 包一层 shim，在 vae2llm 自身 device/dtype 上做投影（与 generate_t2i.py 的 decode_image 设备修复对称）。vlm/t2i/edit 三任务单卡冒烟全过后才起双卡分片全量。
>
> **架构决策（2026-08-23）**：bagel 项目由 `/tank/bagel` 整体迁入本仓 `bagel/`，成为登记子项目（用户拍板，顶层目录禁令的登记例外）。同盘 rename 零拷贝；定位：Bagel（BAGEL-7B-MoT 统一模型）的训练/推理/评测全链路（权重 28G、hf_home 78G、LMUData 33G、conda env 5.1G 等重物随迁）。配套定案：① 子项目自成一个独立 git 仓库（迁入时新建，原目录系 rsync 落地无历史），主仓 .gitignore 将 `bagel/` 整体排除，子项目内部 .gitignore 只放行代码与配置、重物全部不入仓；② 全部硬编码路径 `/tank/bagel` 已批量改写为 `/tank/demiwtg/bagel`（101 文本文件 + conda env 的 75 个 shebang + pip/conda-meta 元数据，复查零残留），`Bagel/bagel` 自指符号链接改为相对链接，conda 环境迁移后验证通过（torch 2.6.0+cu124，CUDA 可用）；③ 与主仓的接口不变：评测题库读 `state/curation/eval_v1/`，评测 runner 为 `bagel/Bagel/scripts/run_wkbench.py`；④ `bagel/HANDOVER.md` 为子项目内部的下载/环境交接文档（子项目自治，不受主仓“文档只两份”约束）。
>
> **架构决策（2026-08-23）**：新开 `benchmark/` 顶层模块（用户指定，顶层目录禁令的登记例外），用于多模态世界知识+推理评测基准建设（目标：定位 Bagel 类统一模型的短板）。分工定案：出题 prompt（`state/curation/eval_v1/synthesize_prompt.md`）与合成脚本（`curation/eval_synthesize.py`，qwen3.7-plus API）归属 curation，题库与评测样本落 `state/curation/eval_v1/`（不入 git）；benchmark/ 只放审阅/评测入口。首批预合成 10 样本 × 3 任务（vlm/edit/t2i）= 30 题已产出，出题 prompt 核心机制：证据审计防 OCR 捷径、caption 防幻觉传染、probe_dims 短板探针维度 + expected_failure_modes 失败模式预测、JSON 机读输出。bagel 侧评测 runner 落在 bagel 子项目 `bagel/Bagel/scripts/run_wkbench.py`（单卡 accelerate-dispatch 加载 BAGEL-7B-MoT，三任务推理：vlm 走 think+understanding 出文本、edit 走官方 imgedit 口径 cfg 出图、t2i 沿用 generate_t2i.py 参数；按 qid 断点续跑、支持 --shard 分片与 LORA_PATH 注入，产出 responses_shard*.jsonl + imgs/）。

> **架构决策（2026-08-23）**：eval_v2 生成（T2I）+编辑双赛道评测定案，出题与判分代码归 benchmark/，运行时产物落 state/curation/eval_v2/（不入 git）。题目构成：生成赛道由本仓数据分层抽样驱动（新增 eval_sample.py——质量门 quality≥8.0 且 identity=true 读 metadata.jsonl 权威清单，口径修正：质量字段不在 images.jsonl；(L1,L2) 分支配额 ∝ sqrt 且最大余数法恰分 n；排除集剔 eval_v1+eval_v2 既有样本 sha 防背答案；每实例限张），首批实测 500 张（279 张 edit_ok），图片索引自 1099 续编兼容前序产物；编辑赛道按 ImgEdit-Bench 套系构成（9 类 edit_type 轮转配额 + 每第 5 题强制知识编辑套，改动方向须由图外知识唯一决定）。判分：生成赛道照 Qwen-Image-Bench 协议——双线判分（知识线 implicit_checks 权重加和、通用线按题面 facet_tags 激活 22 个裁剪 facet，各 0.5 合成，主体缺失/主题跑偏封顶 30），刻度 {0,1,2,NA} 经 φ 非线性映射 0/60/100 后自底向上聚合，facet 词表单一权威源在 eval_score.py FACETS；编辑赛道原文采用 ImgEdit 官方 prompts.json 九类三维 5 分制 rubric（benchmark/edit_score_prompts.json 作为评分契约代码入 git，按 edit_type 路由，二三维不得高于一维的硬约束在判分侧强制钳制）。配套：eval_synthesize.py 迁入 benchmark/ 并重构（--task all 维持 v1 语义不变，gen/edit 为 v2 专项批次）；出题 prompt v2 两册（synthesize_prompt_{gen,edit}.md）与 judge 模板物化（judge_prompts/）均不入 git；judge 用本地 Qwen3.8-27B vLLM（localhost:8000，thinking + 确定性解码，解析失败计 0 并入异常率），出题走 Galaxy API（qwen3.7-plus，需 GALAXY_API_KEY）。冒烟已过：edit 判分端到端（原图冒充产出正确判「无变化」给 1 分）、t2i 判分（gate 封顶语义正确、0 解析失败）、抽样分层配额与排除集生效。

> **架构决策（2026-08-22）**：下载侧恢复连接复用（显式推翻 2026-08-21 keepalive=0 定案，用户拍板）。新增下载专用客户端（infra.get_download_client，双池直连/代理，Limits 128/64），只挂 dl: 档流量，检索侧维持禁复用不动。三层防线：① 病根已除（stream yield-in-retry 已修，全链零任务取消源，结构性不再产生半读连接进池）；② op_download 每请求硬超时 90s（asyncio.wait_for），超时取消任务经 stream 的 finally 关响应，未读完的连接销毁不入池，永久阻塞降级成丢一张图；③ read=30s 读超时与 supervise 12 分钟自愈兜底不动。理由：下载打 CDN，每图一次全新 TCP+TLS 握手已成实测主瓶颈（py-spy 实锤堵在 do_handshake），实测 6.8 张/s 对闸门理论上限 105 张/s。回退条件：任何停摆/风控迹象 → DOWNLOAD_LIMITS 的 max_keepalive_connections 改回 0 一行回退，重启即可。
>
> **架构决策（2026-08-22）**：覆盖过滤后实例队列稳定分区：0 图实例排队首、有存量（1~N-1 张）的难啃实例沉底（实现在 chain.py 启动期，只改顺序不改集合）。理由：重试区实测撞车 89%、实例速率不到干净区一半，先吃干净区把进度跑出来，难啃实例最后兜底；用户曾疑「降 --skip-covered 阈值能缩小重试区」，实测分布推翻：重试区主体是 36,205 个 0 图新实例（阈值降到 1 也跳不掉），1~7 张存量实例仅 2,811 个，降阈值只会放弃这批已投过资的实例。

> **架构决策（2026-08-22）**：打标前撞车快查回归，推翻 2026-08-21「职责清晰优先于微优化」定稿。Sink 新增无锁只读快查 contains()，chain 的 annotate_worker 打标前先查 (sha, instance) 索引，命中即跳过打标。理由：--skip-covered 8 重试区实测撞车占下载量 89%，原「微优化」场景已变主导成本——不前置则 ~9 成 VLM 槽位烧在重复图上；咨询语义不变契约：索引漏查（跨进程新行）时照常打标，权威判定仍在 sink 锁内，最坏多打一次标永不双写。

> **架构决策（2026-08-21）**：存量迁移链开工：新增 op_backfill（补标算子，与全量打标同 prompt 同口径，只重打 kb_match 并补 identity/focus，richness/caption 沿用存量）与 migrate.py（记录驱动：images.jsonl 炸开为每实例×图一条记录 → 读 blob 复验 sha256 → 补标 → 补 queries/query_langs → 追加 metadata.jsonl）；原算子（op_annotate/op_sink/chain）零改动。配套定案：① 迁移后一图多行合法（去重键 (sha256, instance)，对 sink 的 sha 撞车跳过契约定向豁免，仅本入口）；② danbooru 处置：双源（danbooru/bulk_danbooru2023，共 16,102 行）整体从迁移链剔除、不写 metadata.jsonl，由用户另走开源数据集元数据链路单独处理（migrate.py EXCLUDE_SOURCES；含早前确定性认领写回的 7,324 行，认领成果随 images.jsonl 留存供其链路复用）；③ 迁移收官后 metadata.jsonl 升为唯一权威主清单、删 images.jsonl（届时同步改 2.2/2.4 与 viewer 读端）。

> **架构决策（2026-08-21）**：删除 curation/filter_vlm.py（VLM 图片质量过滤 run/report）。理由：质量/身份类打分字段（kb_match/richness/identity/caption/focus/quality）已由 collect_v2 的 op_annotate 随采集链路内联产出，独立质检流水线无运行进程；state/filter_vlm/ 目录不存在、无任何结果残留，删前核实全仓无引用。curation/ 脚本化流水线至此全部移除，只留数据分析 notebook。

> **架构决策（2026-08-21）**：`data/dataset/` 更名 `demiwtg` 并入 `data/datasets/`（同盘 rename 零拷贝）；`data/taxonomy/` 三件套（taxonomy.json/instances.json/alias_western.json）迁入 `data/datasets/demiwtg/meta/`，`data/taxonomy/` 目录撤销。理由：自建数据本身就是一个数据集，与开源数据集统一收编到 data/datasets/ 下，消除 data/dataset 与 data/datasets 双轨；标签体系三件套是该数据集的标注模式资产，与 images.jsonl 同居，meta/ 成为 demiwtg 的唯一真相区。配套变更：仓库根由 `--meta` 向上推导由三级改四级；gen_taxonomy_kb/gen_instance_kb 断点缓存改放 state/taxonomy/（运行时缓存不许进 meta/ 白名单，llm_common.JsonlCache 自动建目录）；三件套继续入 git（.gitignore 逐级例外），blobs 与 jsonl 大文件维持不入 git。

> **架构决策（2026-08-21）**：删除 curation/annotate_vlm.py（VLM 知识打标 run/stream/apply）、curation/emerge.py（taxonomy 涌现缺口分析）、curation/util.py（meta_lock 随唯一消费者一并失去意义），及四个一次性脚本 fix_abs_paths.py（绝对路径迁移，实测 images.jsonl 已 0 条残留）/ backfill_provenance.py（溯源回填，数据源 runs/ 批次产物已清）/ probe_identity.py 与 probe_retrieval.py（数据依赖 bulk posts_meta 与 v1 DLQ 均已删）；同期清理 state/annotate_vlm/、state/emerge/ 运行时目录。理由：打标职责已由 collect_v2 的 op_annotate 接管（打标随采集链路内联），独立打标流水线无运行进程、无消费者；emerge 依赖 annotate_vlm 打标产物且产物全部可从 images.jsonl 重算；删前逐一核实无残留 import。curation/ 此后只留 filter_vlm。
>
> **架构决策（2026-08-21）**：删除 collect v1 采集系统（整模块 40 文件）与 curation/retry_failed.py（v1 死信重试器，随 v1 失去意义）；采集职责由 collect_v2 接管。理由：v1 已被 v2 三层架构完全替代且无运行进程；残留耦合仅 curation 两处引用 v1 的 meta_lock 文件锁工具，已原样迁入 curation/util.py（annotate_vlm/backfill_provenance 改 import 该处）。同时清理 COS 迁移过程文件（remote_pull.sh、cos_pull/）：186/186 分片迁移已于 2026-08-21 收官并验证（blobs 496G/323164 文件，内容寻址抽检通过），过程脚本不再需要。
>
> **架构决策（2026-08-21）**：HF 数据集落盘区由 state/collect/datasets/ 迁至 data/datasets/（23 个数据集目录整体搬移，同盘 rename 零拷贝；下载过程脚本留 state/collect/datasets/ 只读归档）。理由：开源数据集是长期数据资产而非运行时状态，data/ 才是数据根（.gitignore 早有 data/datasets/ 预登记）；state/ 回归纯运行时语义。同期清理：coco2017 标注 zip 为 0 字节下载失败残留，已删。解压教训：多线程共享单个 ZipFile 并发解压会因共享文件指针竞态产生大量伪 CRC 错误（首轮误判 38% 文件损坏，串行 testzip 复验全部 zip 实际完好），必须每线程独立打开 ZipFile。
>
> **架构决策（2026-08-19）**：标签体系解耦——instances.json 升为独立权威源（实体资产，生灭与富知识不依赖树），taxonomy.json 降为展示视角（树 + 挂载引用）；废除 instances.taxonomy_paths 字段（schema 2.0，实例表 56,789 条一次性迁移零丢失）并删除 build_unified.py。理由：树决定实例生死的反向控制是唯一残留耦合，斩断后数据处理链路（采集/打标/涌现）全部只读实例表；树可自由重生成/多视角并存而不伤资产。需要挂载关系的消费者（collect gap 聚簇、gen_instance_kb 与 emerge 的 prompt 上下文）改由 taxonomy/mount_map.py 从树现算。
>
> **架构决策（2026-08-17）**：broader/ 模块（Open-BROADER 上下位关系模型）迁出本仓库，回归独立项目 `/root/data/projects/open_broader/`（代码、55G 训练语料、训练产物、历史日志整体搬移，脚本内绝对路径已批量改写至新家）。理由：上下位判断本质依赖世界知识，通用大模型（Qwen3.8-27B 批审计 + 现成 embedding 检索）已可覆盖 taxonomy 树审计场景，且训练语料正确性存疑、课题短期难推进，故冻结训练、语料与 checkpoint 原地归档。本决策推翻 2026-08-16 的并入决策；未来如复活，先做大模型 vs BROADER 的 head-to-head 评测再立项。

- 跨模块 import 一律 `from <包>.<文件> import ...`：`data/` 下的包（collect_v2/taxonomy）以 `data/` 为包根（消费者先 `sys.path.insert(0, REPO_ROOT/'data')`，见 benchmark 各 eval_sample）；viewer/benchmark 直接位于仓库根。
- 路径常量一律从脚本自身向上推导到仓库根（注意脚本所在层级：data/ 下模块需推导三层），不依赖 cwd 之外的魔法。
- 新增脚本必须先归属到一个模块；归不进去的说明职责边界有问题。

## 4. 数据与代码的边界

- `datasets/`、`state/`、`logs/`、`.qoder/` 是本地数据/运行时产物，**不入 git**（.gitignore 强制；例外：datasets/demiwtg/meta 下 taxonomy 三件套）。
- 入库的只有：代码（data/collect_v2、data/taxonomy、data/curation、viewer/、benchmark/，含 viewer 页面 HTML）、约束文档（AGENTS.md、README.md）、以及 `datasets/demiwtg/meta/` 下的权威 JSON（taxonomy.json/instances.json/alias_western.json）。
- 大 JSON（images.jsonl、blobs）永远不进 git；需要备份走独立通道。
- 生成产物（`viewer/build/`）不入 git，数据改动后重跑 build_viewer.py。

## 5. 关键命令

```bash
# 标签体系富化（LLM 各一次调用；需 LLM_API_KEY 等环境变量；dry-run 零成本预览）
python3 data/taxonomy/gen_taxonomy_kb.py --only-empty --write       # 节点 KB（knowledge_intro 等 4 字段）
python3 data/taxonomy/gen_instance_kb.py --only-empty --write   # 实例知识（desc/query/aliases）

# viewer 产物重建（数据改动后）
python3 viewer/build_viewer.py

# 数据策展（无脚本入口；分析 notebook 在 data/curation/ 内直接运行）

# 采集 v2（检索→下载→落盘链路；smoke_* 为分层验证入口；包根在 data/）
python3 -m data.collect_v2.chain ...   # 或 PYTHONPATH=data 后 python3 -m collect_v2.chain ...

# modelhub LLM 网关 + 静态代理（独立子项目；详见 modelhub/README.md）
bash modelhub/start.sh && bash modelhub/smoke.sh   # 启动+冒烟；停止: bash modelhub/stop.sh [--all]
```

## 6. 禁止事项速查

- ❌ 在 `meta/` 里建除 2.2 清单外的任何文件
- ❌ 手改 blobs/ 下的文件（包括"顺手修一下坏图"——正确做法是重新采集）
- ❌ 删除图片目录前不做 blobs 内容比对
- ❌ 新增只写不读的"审计/日志"文件
- ❌ 在 `data/`（collect_v2/taxonomy/curation）、`viewer/`、`benchmark/` 之外新增脚本（`bagel/`、`modelhub/` 子项目内部自治，不受此限）
- ❌ 往 datasets/ 里放代码、页面或生成产物（viewer 页面与产物在 viewer/ 内闭环）
- ❌ 恢复历史过程文档（docs/、子目录 README）
- ❌ 在数据/代码里使用 category、leaf、root 作为分类概念
- ❌ 在 instances.json 里为同一 name 写多条记录（一个实体一条；多处挂载表现为多个树节点名单同名）
- ❌ 往 instances.json 里写树派生字段（挂载路径等）——挂载关系从树现算（taxonomy/mount_map.py），不持久化
- ❌ 把 `datasets/`（demiwtg/meta 三件套例外）、`state/`、`logs/`、`bagel/` 或 `modelhub/` 提交进主仓
- ❌ 把运行时状态塞进 data/（放顶层 state/ 对应模块子目录）

## 7. 网络与下载约定（2026-08-20 新增：环境里残留已宕机代理 100.89.199.67:7890，pip/curl 会被拖死，故将代理策略定死）

- 国内下载**不走代理**，优先找国内源（如 pypi 用 `pypi.tuna.tsinghua.edu.cn`；注意部分域名 DNS 只返回 IPv6 记录而本机无 IPv6，需确认 A 记录可达）。
- 确需访问外网（pypi.org、download.pytorch.org、GitHub 等）时才用代理：

```bash
export http_proxy=http://192.168.10.109:10808
export https_proxy=http://192.168.10.109:10808
# 或
export ALL_PROXY=socks5h://192.168.10.109:10808
export no_proxy="localhost,127.0.0.1,192.168.10.0/24,modelscope.cn,modelscope.org.cn,.modelscope.cn"
```

- 执行任何下载前，先 `env | grep -i proxy` 检查残留：发现已宕机的旧代理（100.89.199.67:7890）必须先 unset 或按上述配置覆盖。
- **外网链路直连优先**（2026-08-22 拍板）：外网源能直连通就直连，只有实测直连不通的才走代理，减少代理流量；代理源名单按实测增删（collect_v2 落点在 `infra._PROXY_SOURCES` 白名单制：2026-08-22 实测 mal/bing_images/yandex_images 直连可通走直连，wikimedia(_zh)/anilist/pixiv/deviantart 直连超时留代理池）。
