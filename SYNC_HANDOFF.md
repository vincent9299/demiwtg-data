# 增量回湖管线交接（2026-09-08）

## 一句话

lake 常驻 `lake_sync.py` 每小时一轮：镜像七机清单增量 → 拉缺 blob
（sha256 复验入库）→ 过 24h 宽限后**先记账再清理**采集端 COS 原对象。
采集端凭组共享账本跳过已回湖 sha，不重下。

## 组件与位置

| 件 | 位置 | 说明 |
|---|---|---|
| lake_sync.py（常驻 pid 见 pgrep） | 湖 `/yzp/zhaozy/yangzepeng/0905/demiwtg/` | 每小时一轮；日志 `sync_daemon.log` + `sync/sync.log` |
| 断点状态 | 湖 `sync/state.json` | 各 (节点,清单) 字节偏移；轮转自动归零重读 |
| 已校验/已删除 | 湖 `sync/verified.jsonl` / `deleted.jsonl` | 追加型，重启现算加载 |
| 清单镜像 | 湖 `sync/manifests/<节点>/<文件>` | 增量 tail 的累积副本 |
| blob 店 | 湖 `datasets/demiwtg/blobs/aa/sha.ext` | 内容寻址；**实存=已同步**（幂等账本） |
| 防重下账本 | 组桶 `/lhcos-data/demiwtg-data/meta/synced_shas.jsonl` | jsonl {"s":sha}；SG 组五机共享一份、CN 组 E/F 共享一份（各在自家桶） |

## 数据流

1. **镜像**：读七机 `~/lake/meta/{image-shard,docs,backfill-shard,dead-shard}*.jsonl`
   增量（只消费完整行）。
2. **现算缺集**：镜像行取 blob_path → 湖侧不存在的即待拉。
3. **拉取**：SG 组按哈希桶前缀 aa 分摊五机并行 tar 流；CN 组只走 E
   （E/F 共享 GZ 桶）。临时区逐文件 sha256 复验后原子发布。
4. **清理**（回执闭环）：已校验 + >24h + 湖侧仍在的 blob，
   先追加 synced_shas.jsonl（一台写、组内全见），再 `rm` COS 原对象
   （cosfs 删除=组内全可见），每轮上限 2 万。

## 采集端对接（防重下）

- **backfill.py** 已加 `--synced-ledger <path>`：命中账本 sha 即跳过。
  **后续所有 backfill 启动都带**：
  `--synced-ledger /lhcos-data/demiwtg-data/meta/synced_shas.jsonl`
  （当前在跑的七机补采是旧参数——它们 3 小时~2 天内自然结束，
  且清理 24h 宽限未到，无冲突；重启/续跑时务必带新参数。）
- **flow.py**（概念采集线）不受清理影响：概念覆盖按清单行计数，
  不依赖 blob 实存。已知小损耗：极少数 sha 相同的新 URL 会重下后被
  清单去重丢弃，属可接受浪费。

## 湖侧合并（清单 → 权威 images.jsonl）

- 镜像在 `sync/manifests/<节点>/`，行键：concepts/source/content_url/
  sha256/ext/blob_path/size_bytes（docs 行另有页面文本字段）。
- 合并口径：按 **(sha256, concept)** join 湖侧 `meta/images.jsonl`
  （原 instance_images.jsonl，2026-09-08 更名），未命中行追加，
  元数据（license/author）湖侧在册，零丢失。此步是湖侧 curation
  职责，管线不代做。

## 运维要点

- **pod 重启会带走常驻**（隧道 lake_tunnel.sh 同理）：重拉命令
  `cd /yzp/zhaozy/yangzepeng/0905/demiwtg && setsid nohup python3 lake_sync.py > sync_daemon.log 2>&1 < /dev/null &`
- 手动单轮（不动常驻）：`python3 lake_sync.py --once [--no-cleanup]`
- 带宽参考：湖↔节点单流 2.7~7.7 MB/s；一轮上限受批数与文件量自然节流，
  backlog 75k 文件 ≈ 数小时追平
- 磁盘：湖 /yzp 已用 96%（4TB 余量）——27GB SG + 数 GB CN 无压力，
  但长期需关注
- 纯 stdlib，湖 pod 直跑；对节点的唯一要求是 ssh 别名与健康
