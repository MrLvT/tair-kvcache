# Hierarchical Replay Capacity Miss 指标

状态：completed
日期：2026-07-15
Plan：../plans/2026-07-15-capacity-miss-metric.md

## 需求

在 Optimizer 中实现 `kvs_autoscaling_metric_design.md` 定义的第一版 Capacity Miss 指标，用离线 hierarchical replay 区分 Capacity miss、Routing miss 和 Cold miss，并输出五分钟窗口 `capacity_miss_tps`。

## 范围

- 仅作用于 hierarchical replay。
- 维护 engine/storage pool 的 Live Directory 和 capacity/scale-in Ghost Directory。
- 按连续可复用 Prefix 计算请求级 miss 分类。
- 输出累计 token counter、五分钟 TPS 和 ratio。
- 暴露 C++/Python 请求级结果并补充测试和文档。

## 非目标

- 不实现在线 Prometheus 埋点。
- 不实现 autoscaler 禁止缩容或固定 step 扩容控制器。
- 不修改普通 optimizer 和 multi-infer replay 的命中率语义。
- 第一版不支持开启业务 TTL 时计算该指标。

## 需要人确认的决策

- 仅 hierarchical replay：已确认。
- 第一版开启指标时要求 TTL 关闭：已确认。
- 使用独立 `hierarchical_capacity_miss.csv`：已确认。
- `block_size`、容量和 `bytes_per_token` 仍由用户/部署配置确认，不从 trace 猜测。

## 校验

- 新增 cold/routing/capacity/scale-in/ghost expiry/连续前缀测试。
- 验证五分钟窗口输出。
- 验证功能关闭时既有行为不变。
- 执行相关 Bazel tests 和 Python 语法检查。

## 完成记录

- 已实现 Live/Ghost Directory、连续 Prefix miss 分类、五分钟 TPS/ratio 和独立 CSV。
- 已通过 CapacityMissTracker 与 HierarchicalReplayManager 相关 Bazel 测试、pybind/主程序编译和 Python 语法检查。
- 已使用 qwen3.7-plus 2026-07-13 12:00–15:00 四个 service trace 完成并行仿真并生成结果图。
