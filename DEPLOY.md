# 多机分片采集部署手册（D3）

三机（腾讯云新加坡，内网互联，共享 COS 挂载 /lhcos-data）：

| 机器 | 内网 | 公网 | hostname |
|---|---|---|---|
| A | 10.3.4.14 | 43.160.215.28 | VM-4-14-ubuntu |
| B | 10.3.4.16 | 43.160.238.29 | VM-4-16-ubuntu |
| C（本机） | 10.3.0.14 | 43.160.250.196 | — |

SSH：`ssh pipeline-a` / `ssh pipeline-b`（~/.ssh/config 已配，密钥 ~/.ssh/lighthouse_key）。

## 共享存储布局（/lhcos-data/demiwtg-data/）

```
datasets/demiwtg/
├── meta/            # 权威 JSON 五件套（296,010 实例）+ 分片清单汇聚 + 合并产物
└── blobs/           # 内容寻址图片（跨机共享，写一次 rename 发布）
```

各机本地 `~/lake/`：分片清单（追加型写入，**不放 COS**——cosfs 追加语义差且慢）+ 分片词表。

## 各机部署（已完成）

```
~/pipeline/demiflow + ~/pipeline/demiwtg-data（GitHub clone）
~/pipeline/venv（pip install -e demiflow + pillow httpx）
~/pipeline/demiwtg-data/webgate（SearXNG 本机 127.0.0.1:8080，已启动）
冒烟：python -m smokes.<seed|search|...|shard>（全 mock，hermetic）
```

更新：`cd ~/pipeline/<repo> && git pull`。

## 运行（全局限速预算 = 原值；N=总分片数，各机认领互异分片号）

```bash
# 每机（示例：A 认领 0 与 3，B 认领 1 与 4，C 认领 2 与 5，N=6）
cd ~/pipeline/demiwtg-data
../venv/bin/python -m supervise -- \
  --shard 0/6 --skip-covered 8 \
  --instances /lhcos-data/demiwtg-data/datasets/demiwtg/meta/instances.json \
  --dataset ~/lake --alias-cache ~/lake/alias.json \
  --blob-root /lhcos-data/demiwtg-data/datasets/demiwtg \
  --top-n 2 --vlm-concurrency 24 --search-concurrency 8 --download-concurrency 16
```

**VLM 端点（正式跑必须）**：`export DEMIFLOW_VLM_BASE_URL=http://<vLLM主机>:8000/v1/chat/completions`
（端点注册表 env 覆盖；无 vLLM 时 annotate 快速失败、行落盘无标注，seed 只产中文种子。）

## 收尾合并（任一机执行）

```bash
# 1) 各机分片清单上传 COS（带主机前缀防撞名）
scp pipeline-a:~/lake/meta/metadata-shard-0-of-6.jsonl \
    /lhcos-data/demiwtg-data/datasets/demiwtg/meta/metadata-shard-a0-of-6.jsonl
# …（b/c 同理；多次运行先 mv 旧分片归档）
# 2) 合并（(sha256, instances) 去重，原子写 metadata.jsonl）
cd ~/pipeline/demiwtg-data && ../venv/bin/python merge_shards.py \
  --dataset /lhcos-data/demiwtg-data/datasets/demiwtg
```

## 运维注意（实测教训）

1. **cosfs 最终一致性**：大文件（如 instances.json 72MB）写入后需等上传沉降，
   跨机使用前先在目标机校验（读实例数）；追加型文件（清单/词表）一律本地写。
2. **重跑去重**：各机 `~/lake` 保留时重跑自动去重（manifest 索引）；换新实验
   前清 `~/lake`（blob 是内容寻址，不用清）。
3. `--limit` 语义 = **每分片**（分片切片先于 offset/limit）。
4. 首次部署需 `sudo apt install python3.12-venv`；海外机 webgate 用
   `PYSRC=https://pypi.org/simple bash start.sh`。
