# Cache retention 仿真功能交接

本文交接 Optimizer 中 cache block 生命周期与淘汰前空闲时间的采集功能，重点说明当前实现、
`birth` / `last valid read hit` 的判定口径、已经完成的实验以及已知限制。

## 1. 背景与目标

线上流量突增时，cache 容量需求可能先于扩容快速上涨，导致频繁 eviction 和命中率下降。
当前实验希望观察被淘汰 block 的生命周期，以及它距离最后一次有效读取已经过去多久，评估这些指标能否作为更及时的扩容信号。

最初只在 hierarchical replay 中采集。为减少多层级、调度和 P2P 路径对口径的干扰，现已把同一套事件追踪接入 theoretical/global pooled replay，主要实验也以 theoretical replay 为准。

## 2. 已完成功能

- 按 block 的一次驻留生命周期追踪 `birth_time` 和 `last_read_hit_time`。
- 在 block 物理淘汰时生成以下时长样本：
  - `lifetime = eviction_time - birth_time`
  - `idle_after_reuse = eviction_time - last_read_hit_time`
  - `all_block_last_touch_age = eviction_time - (last_read_hit_time if present else birth_time)`
- 按**淘汰发生的分钟**聚合 average、p10、p50、p75、p95、p99。
- 分别统计淘汰总数、淘汰前曾 read-hit 的 block 数，以及从未 read-hit 的 block 数。
- 增加诊断统计：不同 eviction/last-read 时间戳数、最大 last-read cohort、同一 eviction timestamp 下的 idle spread。
- 支持指定一分钟导出逐 block 的 `BIRTH` / `EVICTION` 原始事件，便于核对聚合结果。
- 绘图脚本默认展示 average、p10、p50、p95，并支持调整分钟聚合窗口。
- instance 结果与 service 结果均可导出；service 分位数由原始样本合并后计算，不是 instance 分位数的平均。

主要实现位于：

- `analysis/tracker/cache_retention_tracker.h/cc`：生命周期状态、淘汰采样、分钟聚合和 CSV 导出。
- `index/radix_tree_index.cc`：单层/theoretical cache 的 birth、read-hit 和 eviction 事件源。
- `storage_pool/hash_storage_pool_manager.cc`：storage pool 对应事件源。
- `analysis/script/plot/cache_retention_plot.py`：timeline 绘图。
- `test/cache_retention_tracker_test.cc`：生命周期、右删失、service 聚合和分位数测试。

## 3. `birth` 和 `last read` 的精确定义

### 3.1 一次 lifecycle 的边界

一次 lifecycle 从 block **物理进入 cache** 开始，到它被**物理移出 cache**结束。
同一个 block key 被淘汰后再次 admission，会产生全新的 lifecycle；新的 birth 会覆盖上一轮历史，last read 重新为空。

| 事件 | birth 如何处理 | last read 如何处理 | 是否产生 duration 样本 |
|---|---|---|---|
| miss 后新 block 被实际插入 cache | 设为插入时间 | 初始化为空 | 否 |
| 已存在 block 的 write touch | 不变 | 不变 | 否 |
| 有效 cache read hit | 不变 | 更新为本次 read 时间 | 否 |
| cache miss | 不变 | 不变 | 否 |
| 容量/TTL/clear 等导致物理淘汰 | 结束 lifecycle | 读取淘汰前保存值 | 是 |
| 淘汰后相同 key 再次 admission | 建立新的 birth | 清空 | 否 |
| trace 结束仍然驻留 | lifecycle 右删失 | 保留但不输出 | 否 |

### 3.2 Birth 的判定

`OnBlockBirth(instance_id, block, timestamp)` 在新 `BlockEntry` 被实际加入 cache 时调用。
因此 birth 不是请求到达时间，也不是第一次 read 时间，更不是每次 write 的时间。

需要特别注意：

- miss 不一定单独等价于 birth；只有后续 write/admission 真正插入新 block 才形成 birth。
- 对已经驻留的 block 再 write，不开启新 lifecycle。
- block 被淘汰后重新插入，即使 key 相同，也视为新 lifecycle。
- 当前 request-mode 实验先执行 read，再在 `request_timestamp + write_delay_ns` 执行 write；本轮使用的 `write_delay_ns` 为 1 ns。

### 3.3 Last valid read hit 的判定

`OnBlockReadHit(instance_id, block, timestamp)` 只在查询确认 block 当前有效且命中 cache 时调用。
每次有效命中都会覆盖 `last_read_hit_time`，所以淘汰时保存的是本 lifecycle 内最后一次有效 read hit。

以下事件**不更新** tracker 的 last read：

- miss；
- write/admission；
- 已存在 block 的 write touch；
- block 已经淘汰后的查询；
- 仅仅因为时间推进而仍驻留 cache。

这里必须区分两套时间：淘汰策略内部可能把 read 或 write 都作为 `last access` 来刷新 LRU 顺序；
retention tracker 的 `last valid read hit` 只表示有效读取。两者含义不同，不能互换。

### 3.4 从未复用的 block

若 block 从 birth 到 eviction 之间没有有效 read hit：

- 仍然产生 lifetime 样本；
- 不产生 `idle_after_reuse` 样本；
- 计入 `NeverReusedEvictedBlocks`；
- 在 `all_block_last_touch_age` 中以 birth 作为初始 touch，因此该值等于 lifetime。

### 3.5 分钟归桶

所有 duration 都按 block 自己的 eviction timestamp 计算，然后放入 **eviction 所在分钟**：

```text
bucket = floor(eviction_time / 60s)
idle_i = eviction_time_i - last_read_time_i
```

当前指标不是“在某分钟边界对全部 resident block 做年龄快照”。trace 结束仍存活的 block 属于右删失样本，当前不进入任何 duration 分布。

## 4. 为什么 idle 的 p10 与 p95 很接近

以 2026-07-13 10:30:00–10:31:00 为例，逐 block 诊断显示：

| 项目 | 数值 |
|---|---:|
| 请求数 | 16,863 |
| 输入/read block 数 | 624,048 |
| cache hit block 数 | 530,570 |
| miss / unique admission block 数 | 93,478 |
| eviction block 数 | 93,478 |
| 淘汰前曾 read-hit | 56,084 |
| 淘汰前从未 read-hit | 37,394 |
| admission/eviction 的不同时间戳数 | 9,582 |

被淘汰 block 的 birth 范围为 04:06:57.443225–10:28:59.843074；曾复用 block 的 last-read 范围为
10:27:51.771756–10:28:59.843074。last-read 本身并没有集中在几秒内：其 p10 到 p95 跨约 58 秒。

但是 eviction 时间也随这一分钟内的请求向前移动，且 last-read 与 eviction 时间高度同步：

| 时间/时长 | p10 | p50 | p95 |
|---|---:|---:|---:|
| last-read 时刻 | 10:27:58.580939 | 10:28:25.425380 | 10:28:56.694456 |
| eviction 时刻 | 10:30:04.826067 | 10:30:29.499126 | 10:30:56.799659 |
| `eviction - last-read` | 120.846076 s | 123.930698 s | 126.964288 s |

last-read 与 eviction timestamp 的相关系数约为 0.99972。也就是说，后读的 cohort 通常也更晚被淘汰；
两个都横跨接近一分钟的绝对时间相减后，只剩较窄的 retention 差异。这解释了 p10 与 p95 接近，不能据此推断“所有 block 的 last read 发生在同几秒”。

这也说明当前结果很大程度上体现 exact LRU/global pool 的队列行为。global pooled theoretical replay 把所有 pod 的 cache 合成一个全局 cache，会抹去 pod 内容、调度和局部负载差异；它不能直接代表真实多 pod 场景的分布宽度。

## 5. 已完成实验

主要数据与参数：

- 服务/trace：qwen3.7-plus，2026-07-13 00:00–18:00，共 7,610,250 个请求。
- `block_size = 2048 tokens`。
- `bytes_per_token = 46811 bytes`，对应约 45.714 KiB/token。
- 1x 容量：27625 GiB，约 309,402 blocks。
- 淘汰策略：global pooled exact LRU。
- 1x 首次填满并开始 eviction：00:06:56.196185。
- 3x 容量验证：82875 GiB，回放前 3 小时；首次填满并开始 eviction：00:29:13.309362。

3x 实验延后了填满时间并改变了绝对 retention，但没有显著扩大 idle 的相对分布宽度；这支持“p10/p95 接近主要来自 eviction 与 last-read 的配对移动，而非仅仅因为 1x 容量太小”的判断。

当前本机产物位于 `~/Downloads`，包括 retention timeline、1x/3x 对比、每分钟 eviction 容量、
unique admission rate、cohort diagnostics，以及 10:30 单分钟逐 block 明细。

## 6. 输出字段

`<instance>_cache_retention_by_minute.csv` 主要列如下：

- `MinuteStartNs`, `MinuteStart`：eviction 分钟。
- `EvictedBlocks`, `ReusedEvictedBlocks`, `NeverReusedEvictedBlocks`。
- `Lifetime{Average,P10,P50,P75,P95,P99}Seconds`。
- `IdleAfterReuse{Average,P10,P50,P75,P95,P99}Seconds`。
- `AllBlockLastTouchAge{Average,P10,P50,P95}Seconds`。
- `LruTimeSpanSeconds`：分钟结束边界上 LRU 当前最新 access time 与最老 access time 的差值。
- `DistinctEvictionTimestamps`, `DistinctLastReadTimestamps`。
- `LargestLastReadCohortBlocks`, `LargestLastReadCohortFraction`。
- `ReuseEvictionBatches`, `BatchIdleSpread{Average,P50,P95}Seconds`。

指定诊断分钟时，还会生成 `cache_retention_diagnostic_<minute_start_ns>.csv`，包含事件类型、block key、birth、last read、eviction、lifetime、idle 和 all-block age。诊断窗口按 eviction/event timestamp 选择一分钟；为解释该分钟淘汰对象，EVICTION 行会携带其可能早于该分钟的 birth 和 last read。

## 7. 运行与复现

开发机编译：

```bash
bazelisk --host_jvm_args=-XX:+DisableAttachMechanism build --config=py311 \
  //kv_cache_manager/optimizer:optimizer_main
```

运行有限容量 theoretical replay：

```bash
bazel-bin/kv_cache_manager/optimizer/optimizer_main \
  /path/to/config.json --export-cache-retention
```

导出指定一分钟的逐 block 诊断；环境变量值必须是该分钟起点的 Unix epoch ns：

```bash
KVCM_RETENTION_DIAGNOSTIC_MINUTE_START_NS=1783909800000000000 \
bazel-bin/kv_cache_manager/optimizer/optimizer_main \
  /path/to/config.json --export-cache-retention
```

10:30 诊断结果在开发机的示例路径：

```text
/mnt/lvtian/tair-kvcache/.optimizer-runs/qwen3.7-plus/
capacity_27625GiB_21db_h00_h18_minute1030_diagnostics_20260717/output/
cache_retention_diagnostic_1783909800000000000.csv
```

运行单元测试：

```bash
bazelisk --host_jvm_args=-XX:+DisableAttachMechanism test --config=py311 \
  //kv_cache_manager/optimizer/test:CacheRetentionTrackerTest
```

该测试已在开发机通过。

## 8. 已知限制与后续建议

1. 当前 theoretical 实验是 global pooled cache，不满足真实线上“KVCache 仅在同一 instance 内复用”的部署约束。若要判断 pod 差异和调度策略对 p10/p95 的贡献，需要回到 instance-isolated replay，并保持相同事件口径。
2. 当前只统计已淘汰样本。若目标是扩容预警，建议补一个固定分钟边界的 resident-cache age snapshot，直接观察当时所有驻留 block 的 `t_snapshot - last_read_or_birth`，不要与 eviction-conditioned idle 混称。
3. `unique admission rate` 图目前可从 resident block 增量与 eviction 数重建；若作为长期正式指标，建议在 tracker 中直接导出 admission blocks/bytes，避免依赖差分。
4. 原始一分钟诊断由环境变量控制，属于调试接口；若需要常态化使用，建议改成正式命令行参数，并增加最大行数或 block 采样选项。
5. 正式回放必须显式确认 `block_size`、page size 和 `bytes_per_token`。这些值直接决定容量换算，不能仅从 trace 猜测。

## 9. Global pooled retention 扩缩容回放

单层 global pooled replay 支持基于 `IdleAfterReuseP10Seconds` 的容量闭环。配置示例：

```json
"cache_autoscaling": {
  "enabled": true,
  "scale_out_threshold_seconds": 300,
  "scale_in_threshold_seconds": 360,
  "scale_step_tib": 27,
  "scale_out_delay_seconds": 300
}
```

- 完整分钟的指标严格小于 300 秒时触发扩容，300 秒后 `quota_capacity` 增加 27 TiB。
- 指标严格大于 360 秒且存在此前扩出的容量时，立即缩容 27 TiB。
- 300–360 秒为弹性区间；无淘汰或无 reused eviction 样本的分钟不触发动作。
- 同一时间只允许一个 pending 动作，容量不会缩到初始 `quota_capacity` 以下。
- 只支持单个、非 hierarchical instance group，并要求 group 内只有一个 synthetic global-pool instance；
  27 TiB 修改的是配置 `quota_capacity`，实际驱逐线仍为 `quota_capacity * used_percentage`。
- 事件写入 `cache_capacity_scaling_events.csv`，同时记录触发、计划生效和实际生效时间。
