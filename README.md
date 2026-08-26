# demiwtg

标签体系（taxonomy + instances）治理与 IP 图片数据湖项目。

- 架构约束、数据契约、dataset 硬约束：见 **[AGENTS.md](AGENTS.md)**（唯一权威文档）。
- 代码模块：`data/taxonomy/`（体系构建富化）、`data/collect_v2/`（图片采集）、`data/curation/`（数据策展：分析 notebook）、`viewer/`（查看器：页面 + 构建脚本 + 产物闭环）、`benchmark/`（评测基准：vlm/t2i/edit 三子模块）。
- 数据：`datasets/`（数据集根；demiwtg = 自建数据集：meta/ 下 taxonomy 三件套入 git，blobs 与清单不入 git）。
