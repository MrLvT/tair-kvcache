# 存算一体场景下的 KVS 扩缩容指标设计

## 1. 背景

当前部署采用存算一体架构，每个推理实例同时提供算力和 KVS 容量。现有 autoscaler 主要由以下算力指标驱动：

- Prefill batch size；
- Non-cache Prefill TPS。

它们能发现算力不足，却不能提前识别 KVS 容量不足。

以 3.7 Plus 0713 事件为例：

- 13:00 后可用实例数从约 73 持续缩至约 55，总 KVS 容量下降；
- 单实例 Input TPM 和 KV 写入压力上升；
- Prefix Cache 命中率从 13:00 开始缓慢下降；
- 13:28 左右 Queue Length 才开始明显上涨；
- 13:31 左右 Prefill batch size 才超过 autoscaler target=4；
- 13:56 左右开始创建新增实例，14:11 左右第一批实例 Ready。

因此需要增加一个简单、直接的 KVS 扩缩容指标，在算力尚未饱和时识别缓存容量问题。

## 2. 核心指标

新增指标定义为：

\[
\boxed{capacity\_miss\_tps}
\]

含义：

> 每秒有多少 Prompt Token，因为对应 KV 曾被 KVS 容量淘汰，导致本次请求必须重新执行 Prefill。

它只统计“增加 KVS 容量可能避免”的重复计算，不统计第一次访问的冷数据，也不统计单纯的路由错误。

## 3. 为什么不直接使用 Hit Rate

低命中率不一定代表 KVS 容量不足：

- 全新的冷流量命中率天然很低，扩容无法命中第一次访问；
- Prefix 在其他 Pod 上但没有路由过去，属于 Routing miss；
- 模型版本、Chat Template 或 Hash 规则变化会造成失效；
- 只有 KV 因容量不足被淘汰，之后又被重新访问，才说明扩容 KVS 可能有效。

Hit Rate 继续作为结果指标观察，但不直接驱动 KVS 扩容。

## 4. Miss 分类

| 类型 | 含义 | 计入 `capacity_miss_tps` | 应对策略 |
|---|---|---:|---|
| Cold miss | Prefix 第一次出现，从未缓存过 | 否 | 由 Non-cache TPS 算力控制器处理 |
| Routing miss | KV 仍在其他 Pod/KVS，但请求未路由过去 | 否 | 优化 cache-aware 路由或复制热点 |
| Capacity miss | KV 曾存在，因容量或缩容被淘汰，随后再次访问 | 是 | 禁止缩容或扩容 KVS |

以下原因不计入 Capacity miss：

- 模型版本切换；
- Chat Template 或 Hash 规则变化；
- 主动失效；
- 业务 TTL 到期；
- 数据损坏；
- Prompt 本轮新增的 Token。

## 5. 所需数据

### 5.1 Live Directory

记录 KV block 当前仍存在哪些节点：

```text
block_hash -> [pod-1, pod-7]
```

它用于判断当前 Pod 是否有 KV、其他 Pod 是否还有 KV，以及 KV 是否已经从整个集群消失。如果 cache-aware scheduler 已有 KV 位置索引，可以直接复用。

### 5.2 Ghost Directory

KV block 被淘汰后，不保存 KV payload，只保留少量元数据：

```text
block_hash
model_revision
evicted_at
eviction_reason
block_tokens
last_access_at
```

示例：

```text
abc123 -> {
  model_revision: qwen3.7-plus-0522,
  evicted_at: 13:00:15,
  eviction_reason: capacity,
  block_tokens: 256,
  last_access_at: 12:58:30
}
```

Ghost 只保存 Hash 和元数据，建议保留 15～30 分钟。

计入的淘汰原因：

- `capacity`；
- `scale_in`。

不计入：

- `ttl_expired`；
- `model_changed`；
- `manual_invalidate`；
- `corrupted`。

## 6. 单请求计算方法

假设 KV block size 为 256 tokens，一个请求的历史 Prefix 被拆为：

```text
B1 B2 B3 ... B16
```

对该请求计算三个连续 Prefix 长度：

1. `L_local`：当前 Pod 实际能够连续命中的 Prefix tokens；
2. `L_global`：整个集群任意 Pod/KVS 仍存在的连续 Prefix tokens；
3. `L_counterfactual`：把最近因容量淘汰、仍在 Ghost 中的 block 视为存在时，本来能够连续命中的 Prefix tokens。

计算公式：

```text
routing_miss_tokens
  = L_global - L_local

capacity_miss_tokens
  = L_counterfactual - L_global

cold_miss_tokens
  = cacheable_prompt_tokens - L_counterfactual
```

每完成一个请求：

```text
kvs_capacity_miss_tokens_total += capacity_miss_tokens
```

必须按“从 Prompt 开头连续可复用的 Prefix”计算，不能把中间断开后的 block 单独计入。

## 7. 计算示例

### 7.1 真正的容量不足

一个请求包含 4096 个 Prefix Token，共 16 个 block：

```text
B1～B8：集群中仍然存在
B9～B14：因容量不足被淘汰，存在于 Ghost Directory
B15～B16：本轮第一次出现
```

计算结果：

```text
实际命中：       8 × 256 = 2048 tokens
capacity miss：  6 × 256 = 1536 tokens
cold miss：      2 × 256 =  512 tokens
```

本次请求：

```text
kvs_capacity_miss_tokens_total += 1536
```

最后 512 tokens 从未缓存过，扩容也无法命中，不能计入 Capacity miss。

### 7.2 全新冷流量

一个全新的 8192-token Prompt：

```text
Live Directory：没有
Ghost Directory：没有

capacity_miss_tokens = 0
cold_miss_tokens = 8192
```

即使命中率为 0%，也不会推动 KVS 扩容。Non-cache TPS 可能推动算力扩容。

### 7.3 路由错误

请求包含 4096 个可复用 Token：

```text
当前 Pod：只有前 2048 tokens
其他 Pod：完整保存 4096 tokens
Ghost Directory：没有

routing_miss_tokens = 2048
capacity_miss_tokens = 0
cold_miss_tokens = 0
```

此时应调整路由或复制热点，不应扩总 KVS 容量。

### 7.4 Agent 历史被淘汰

Agent 下一轮请求包含 32K 历史上下文：

```text
实际仍可命中：       20K
Ghost 中连续存在：    10K
本轮新增内容：          2K

capacity_miss_tokens = 10K
cold_miss_tokens = 2K
```

这 10K Token 是明确的容量损失。如果同类请求持续出现，`capacity_miss_tps` 会快速升高。

## 8. Deployment 级指标

请求级 Counter：

```text
kvs_capacity_miss_tokens_total
cacheable_prompt_tokens_total
kvs_routing_miss_tokens_total
kvs_cold_miss_tokens_total
```

核心指标：

```promql
sum(rate(kvs_capacity_miss_tokens_total[5m]))
```

结果即：

```text
capacity_miss_tps
```

辅助比例：

```promql
sum(rate(kvs_capacity_miss_tokens_total[5m]))
/
sum(rate(cacheable_prompt_tokens_total[5m]))
```

结果为 `capacity_miss_ratio`：

- `capacity_miss_tps` 表示绝对重复计算成本，作为主扩缩容指标；
- `capacity_miss_ratio` 只用于解释容量问题占流量的比例。

## 9. 混合流量示例

假设每秒流量为：

| 流量类型 | 请求数 | 单请求 Capacity miss |
|---|---:|---:|
| Agent 请求 | 100 req/s | 1536 tokens |
| 全新冷请求 | 50 req/s | 0 |
| Routing miss 请求 | 20 req/s | 0 |

那么：

```text
capacity_miss_tps
= 100 × 1536
= 153,600 tokens/s
```

假设总 Cacheable Prompt 流量为 901,120 tokens/s：

```text
capacity_miss_ratio
= 153,600 / 901,120
≈ 17%
```

如果单实例 Non-cache Prefill target 为 60,000 tokens/s，容量淘汰导致的重复计算约等价于：

```text
153,600 / 60,000
= 2.56 个实例的 Prefill 能力
```

## 10. Autoscaler 策略

最终实例数：

```text
desired_instances
= max(
    prefill_batch_scaler,
    noncache_tps_scaler,
    capacity_miss_tps_scaler
  )
```

### 10.1 禁止缩容

满足以下任一条件时，KVS 控制器禁止缩容：

```text
capacity_miss_tps 持续上升
```

或者：

```text
capacity_miss_ratio > 1%～2%，持续 2～3 分钟
```

阈值需要通过历史流量回放校准。

### 10.2 触发扩容

使用单实例 Non-cache TPS target 作为直观标尺。假设：

```text
per_instance_noncache_tps_target = 60,000
```

第一版可设置：

```text
capacity_miss_tps > 30,000～60,000
并持续 2～3 分钟
```

则触发固定 step 扩容。

新增实例 Ready 后：

- 如果 `capacity_miss_tps` 明显下降，停止继续扩容；
- 如果仍高于阈值，继续扩下一批；
- 如果指标下降但 Hit Rate 仍低，说明剩余 miss 主要是 Cold miss 或 Routing miss。

第一版不使用复杂模型计算一次扩多少台，直接复用现有 autoscaler 的固定 step 和 stabilization 机制。

### 10.3 Scale-in

缩容必须同时满足：

- Prefill batch size 允许缩容；
- Non-cache TPS 允许缩容；
- `capacity_miss_tps` 长时间处于低位；
- 当前不存在 KVS 容量恶化趋势。

Scale-out 可以由任一控制器触发；Scale-in 必须所有控制器共同同意。

## 11. 不同工况下的行为

| 工况 | `capacity_miss_tps` | 策略 |
|---|---:|---|
| 常态低命中、请求很少复用 | 低 | 不扩 KVS |
| 高命中 Agent，历史 KV 被淘汰 | 高 | 禁止缩容并扩容 |
| 全新冷流量冲击 | 接近 0 | KVS 不扩，算力控制器可能扩 |
| KV 在其他 Pod，路由没有命中 | 接近 0 | 优化路由或热点复制 |
| Hot Set 小且稳定 | 接近 0 | 无需继续扩 KVS |
| 缩容导致历史 KV 丢失并被重新访问 | 上升 | 停止缩容，必要时恢复容量 |
| 新节点冷启动 | 可能暂时升高 | 等待 Ready 后观察，避免连续过冲 |

## 12. 对 3.7 Plus 0713 的验证

使用 13:00～15:00 的请求级 Prefix block hash 和 KV 淘汰记录回放：

1. 计算每分钟 `capacity_miss_tps`；
2. 分离 Cold miss、Routing miss 和 Capacity miss；
3. 检查 13:00 左右命中率下降时，该指标是否同步上升；
4. 检查该指标是否在 13:28 Queue 上升前发出信号；
5. 模拟首次越过阈值时禁止从约 73 台继续缩容；
6. 验证是否能够避免 13:48 后的 503；
7. 检查扩容 Ready 后该指标是否如预期下降。

如果 Hit Rate 下降但 `capacity_miss_tps` 仍接近 0，说明下降主要来自流量结构变化、Cold miss 或 Routing miss，不应归因于 KVS 总容量不足。

## 13. 建议埋点

KVS 淘汰事件：

```text
kv_evict_total{
  deployment,
  instance,
  model_revision,
  reason
}
```

请求级累计指标：

```text
kvs_capacity_miss_tokens_total
kvs_routing_miss_tokens_total
kvs_cold_miss_tokens_total
cacheable_prompt_tokens_total
```

辅助观测：

```text
kvs_ghost_entries
kvs_ghost_lookup_total
kvs_ghost_hit_total
kvs_live_replica_count
kvs_eviction_age_seconds
```

建议按 deployment、model revision、caller、instance/routing domain 和 eviction reason 聚合。

避免保存完整 Prompt 或明文内容，只保存经过模型版本隔离的 block hash。

## 14. 第一版落地范围

第一版只实现：

1. Live Directory 查询；
2. Capacity eviction Ghost Directory；
3. 请求级连续 Prefix 差值计算；
4. `kvs_capacity_miss_tokens_total`；
5. 5 分钟窗口的 `capacity_miss_tps`；
6. 禁止缩容和固定 step 扩容。

第一版不实现：

- MRC；
- 复杂边际收益模型；
- 自动推导最优实例数；
- Prefix 未来复用概率预测；
- 复杂业务优先级模型。

先通过 3.7 Plus 0713 和典型工况回放证明该指标能够正确区分 Capacity miss、Cold miss 和 Routing miss，再逐步优化扩容 step 与阈值。

## 15. 最终定义

> `capacity_miss_tps`：单位时间内，因为 KV 曾被容量或缩容淘汰、随后又被请求，导致必须重新执行 Prefill 的连续 Prefix Token 数。

它作为存算一体部署中，除 Prefill batch size 和 Non-cache TPS 之外的第三个扩缩容指标。
