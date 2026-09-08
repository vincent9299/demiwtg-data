# 增量回湖管线交接（2026-09-08；2026-09-09 增 pages 回湖）

## 一句话

lake 常驻 `lake_sync.py` 每小时一轮：镜像节点清单增量 → 拉缺 blob
（sha256 复验入库）+ 拉缺 pages（docs 线正文，URL 寻址+版本化闸门）
→ 过 24h 宽限后**先记账再清理**采集端 COS 原对象（清理仅 blobs；
pages 暂不清理）。采集端凭组共享账本跳过已回湖 sha，不重下。

## 组件与位置

| 件 | 位置 | 说明 |
|---|---|---|
| lake_sync.py（常驻 pid 见 pgrep） | 湖 `/yzp/zhaozy/yangzepeng/0905/demiwtg/` | 每小时一轮；日志 `sync_daemon.log` + `sync/sync.log` |
| 断点状态 | 湖 `sync/state.json` | 各 (节点,清单) 字节偏移；轮转自动归零重读 |
| 已校验/已删除 | 湖 `sync/verified.jsonl` / `deleted.jsonl` | 追加型，重启现算加载 |
| pages 审计 | 湖 `sync/verified_pages.jsonl` | {sha, got(实收内容哈希), ts, group}；拉取判定只认湖侧实存，本文件是审计+未来清理 pending |
| 清单镜像 | 湖 `sync/manifests/<节点>/<文件>` | 增量 tail 的累积副本（节点 meta 的逐字节镜像，未合并） |
| 真 meta 总账 | 湖 `datasets/demiwtg/meta/{docs,images}.jsonl` | **merge_meta.py 例行合并**（lake_sync 每轮末尾自动调用，--no-merge-meta 跳过；也可手动 --once）。docs=全量重合并原子替换（28,128 镜像行→10,023 行）；images=**增量追加**（偏移状态 sync/merge_state.json + 键集 (sha256,instances) 去重，不重写 285 万行大账；首轮 63,883 镜像行→追加 50,882，35 秒）。backfill/dead 镜像不进账（复原操作/死信留档）。fcntl 锁防手动与轮次重叠。meta/ 下另有既有 concepts.json/taxonomy.json |
| blob 店 | 湖 `datasets/demiwtg/blobs/aa/sha.ext` | 内容寻址；**实存=已同步**（幂等账本） |
| pages 店 | 湖 `datasets/demiwtg/pages/aa/sha(url).md` | **URL 寻址**（page.py：page_sha=sha256(url)，同 URL 重抓覆盖） |
| 防重下账本 | 组桶 `/lhcos-data/demiwtg-data/meta/synced_shas.jsonl` | jsonl {"s":sha}；两组各在自家桶（SG 五机一份、CN 五机一份） |

## 数据流

1. **镜像**：读各节点 `~/lake/meta/{image-shard,docs,backfill-shard,dead-shard}*.jsonl`
   增量（只消费完整行）。
2. **pages 回湖**（先于 blobs：知识正文优先于图片）：镜像 `docs*` 行取
   path（须 `pages/` 前缀）→ 行闸门 `sha256(url)==page_sha` → **跨组全局
   去重**（同 rel 先到组先得，sg 优先——同 URL 两队都抓过只拉一份）→
   湖侧实存即已同步。拉取 = 两组各按 aa 前缀分摊全部 readers（各五口）
   tar 流；发布闸门版本化：行带 `content_sha`（2026-09-09 起 page.py 记账）
   → sha256(内容) 强复验，不符拒发布；旧行缺省 → 宽松门（非空+文件名
   stem==page_sha）。审计落 verified_pages.jsonl。
3. **blob 缺集现算**：镜像行取 blob_path → 湖侧不存在的即待拉。
4. **blob 拉取**：两组均按哈希桶前缀 aa 分摊组内 readers 并行 tar 流；
   临时区逐文件 sha256 复验后原子发布。
5. **清理**（回执闭环，仅 blobs）：已校验 + >24h + 湖侧仍在的 blob，
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
- **operators/page.py**（2026-09-09 起）：页面产物行与 docs 清单行新增
  `content_sha`/`page_bytes`（所落字节的内容哈希与长度）——pages 按 URL
  寻址，湖侧内容级闸门需要源头记账。旧行缺这两键，湖侧自动降级宽松门
  （闸门随数据版本升级，无需回补）。

## 冒烟

- `smokes/lake_pages.py`：pages 回湖纯逻辑冒烟（行闸门/跨组去重/实存
  跳过/版本化发布门），本地直跑不 ssh。
- 已知失效：`smokes/docs.py` 的 `wiki_rows` 断言在**本轮改动之前**已
  失败（基线对照验证过，疑与 09-07 robots/wiki 直取改造后 mock 路径
  变化有关），待修。

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
