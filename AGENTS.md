# AGENTS.md

项目文档见 [docs/README.md](docs/README.md)。

## 开发环境

- **本机**：macOS，代码仓库路径 `/Users/mrlvt/codes/tair-kvcache`
- **开发机**：`admin@11.123.68.231`，代码路径 `/mnt/lvtian/tair-kvcache`
- **同步方式**：本机修改代码后用 `scp` 同步到开发机对应路径
- **Bazel 位置**：`/mnt/tair-kvcache/.optimizer-runs/bin/bazelisk`（已写入 `~/.bashrc`）
- **Bazel 编译参数**：`bazelisk --host_jvm_args=-XX:+DisableAttachMechanism build --config=py311`（`-XX:+DisableAttachMechanism` 禁止阿里 JVM Sandbox attach 导致 bazel crash；`--config=py311` 使用 Python 3.11，系统默认 python3 是 3.6 不满足 pybind11 >= 3.8 要求）

## 项目概述

tair-kvcache 是 LLM 推理场景的 KV Cache 管理系统，提供多层级缓存存储（HBM → DRAM → Storage Pool）、跨实例缓存复用、P2P 缓存读取等能力。

### 当前关注点：Optimizer 仿真模块

Optimizer 是一个离线 trace replay 仿真器，输入线上采集的推理 trace，模拟不同缓存策略下的命中率，用于离线评估容量规划和调度策略。

#### Optimizer 核心架构

```
kv_cache_manager/optimizer/
├── config/                   # 配置定义与解析
│   ├── hierarchical_replay_config.h/cc   # 多层级仿真配置
│   └── optimizer_config.h/cc             # 单实例仿真配置
├── manager/                  # 仿真引擎
│   ├── hierarchical_replay_manager.h/cc  # 多instance联合仿真（支持P2P、调度策略）
│   ├── infer_engine_scheduler.h/cc       # 调度器（preserve_trace/round_robin/prefix_hit/load_balance）
│   └── optimizer_manager.h/cc            # 单instance仿真
├── analysis/script/          # Python 分析脚本
│   └── run/
│       ├── hierarchical_replay.py   # 多层级仿真入口（支持predictor注入、multi-lane并行）
│       ├── multi_infer_replay.py    # 多实例独立仿真
│       └── tradeoff.py              # 容量-命中率 tradeoff 分析
├── pybind/                   # Python C++ 绑定
│   └── py_optimizer_binding.cc
└── test/                     # 单元测试
```

#### 当前目标

1. **load_balance 调度仿真**：模拟线上 load-aware 调度器，根据 prefill 时间预测选择最空闲的 engine instance
2. **multi-lane 并行模型**：每个 engine 支持 N 路并发 prefill（`infer_concurrency` 配置），反映线上 batch 并行
3. **P2P 缓存读取**：同集群内跨 pod 的缓存复用，减少 remote 读取
4. **容量规划**：通过 capacity sweep 评估不同 HBM/DRAM 容量下的命中率分布

## 约束

- **Instance 隔离**：KVCache 仅在同一个 `instance_id` 内复用，跨 Instance 不匹配。
- **仿真参数口径确认**：`block_size` / `page size` / `bytes_per_token` 会直接影响 token hit 与容量换算，不能仅凭 trace 自动推断；除非用户已明确给出可用于仿真的有效值（例如 `2174(2048)` 中括号内 `2048`），否则必须先向用户确认后再跑正式仿真。

<!--
约束收录原则：只放会导致方向性错误的系统级约束，不放通用工程实践。
组件级实现细节放在对应模块文档中。
-->
