# Hierarchical Replay Capacity Miss 指标实现计划

状态：implemented
日期：2026-07-15
Task：../tasks/2026-07-15-capacity-miss-metric.md

## 当前状态

Hierarchical replay 已按 engine -> P2P -> storage pool 执行读请求并输出 combined hit-rate。`TierGlobalTracker` 仅维护配置过的 P2P tier，storage pool 淘汰和 cache drop 没有统一的逐 block 全局存在性事件，因此不能直接判断最后一个副本是否因容量或缩容消失。

## 设计方案

- 新增 hierarchical 专用 CapacityMissTracker，以 storage pool 为 scope，以 engine/pool 为 holder 维护 block live holders。
- engine 写入/最终容量淘汰复用 tier-flow key events；storage pool 写入和容量淘汰补充 mutation events；cache drop 在清空前快照 live keys并记录 scale-in removal。
- 仅当最后一个 live holder 因 capacity/scale-in 消失时创建 Ghost；Ghost 按 event time 保留。
- 请求执行前保存 `L_global` 和 `L_counterfactual`，执行现有读链后从 satisfied mask 得到 `L_actual`。三类 miss 一律按 index 0 开始的连续 prefix 计算。
- `cacheable_prompt_tokens = keys.size() * block_size`，不完整尾部 token 不参与第一版分类。
- 独立输出 `hierarchical_capacity_miss.csv`，包含请求级、累计和完整五分钟窗口指标。

## 待用户确认的设计决策

- 能力只作用于 hierarchical replay：已确认。
- `capacity_miss_metric.enabled=true` 时业务 TTL 必须关闭：已确认。
- 输出使用独立 CSV，不改变既有 hit-rate CSV：已确认。
- 默认关闭以保持兼容；Ghost 默认 1800 秒，窗口默认 300 秒；retention 必须不小于 window：按已讨论方案执行。
- block key 必须已经按模型版本隔离，第一版不增加 trace model_revision 字段：按已讨论方案执行。

## 用户确认记录

2026-07-15：用户明确回复“三项均确认”，并授权在创建当前 Optimizer 基线提交后开始执行。

## 文件和模块

- `config/hierarchical_replay_config.*`：新增配置与校验。
- `analysis/tracker/capacity_miss_tracker.*`：Live/Ghost、请求分类、窗口统计和 CSV。
- `manager/hierarchical_replay_manager.*`：汇聚 presence event、计算并返回请求指标、导出结果。
- `manager/optimizer_manager.*` / index：cache drop 前 live-key 快照。
- `storage_pool/hash_storage_pool_manager.*`：返回 pool insert/capacity removal events。
- `pybind/py_optimizer_binding.cc`：暴露请求级字段。
- `analysis/script/run/hierarchical_replay.py`：打印结果摘要。
- `test/hierarchical_replay_manager_test.cc`：语义边界测试。
- `docs/hierarchical_replay.md`、`docs/strategy_config.md`：配置和输出说明。

## 兼容性

默认 `enabled=false`。功能关闭时不创建 tracker、不增加既有 CSV 列，也不改变命中率、IO、调度和 cache 行为。新增结果字段默认是 0。

## 校验

- 相关 C++ 单元测试。
- pybind 编译与 Python 语法检查。
- CSV header/累计值/五分钟窗口断言。
- `git diff --check`。

## 执行结果

- 以事件驱动方式维护正常读写路径的 holder，避免每次操作全量扫描 cache。
- cache drop 仅在显式缩容时快照 live keys，用于生成 scale-in Ghost。
- 相关编译和指标测试已通过；四个 service 仿真进程均成功返回。

## 开源 PR 说明

该能力不依赖私有数据或服务名，适合作为通用 optimizer 仿真能力提交到 `https://github.com/alibaba/tair-kvcache`。
