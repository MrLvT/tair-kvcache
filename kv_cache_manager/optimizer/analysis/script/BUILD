load("@rules_python//python:py_binary.bzl", "py_binary")
load("@rules_python//python:py_library.bzl", "py_library")

package(default_visibility = ["//visibility:public"])

PYBIND_DEP = "//kv_cache_manager/optimizer/pybind:kvcm_py_optimizer_lib"

# =========================================================================
# Library 层：imports=["."] 使 script/ 成为包根，run/ 内可直接
# from utils.csv_loader import ... / from plot.hit_rate_plot import ...
# =========================================================================

py_library(
    name = "utils_lib",
    srcs = [
        "utils/__init__.py",
        "utils/csv_loader.py",
        "utils/optimizer_runner.py",
        "utils/plot_utils.py",
        "utils/window_aggregator.py",
    ],
    imports = ["."],
    deps = [PYBIND_DEP],
)

py_library(
    name = "plot_lib",
    srcs = [
        "plot/__init__.py",
        "plot/cache_read_interval_plot.py",
        "plot/cache_read_interval_distribution_plot.py",
        "plot/cache_read_interval_cdf_plot.py",
        "plot/hit_rate_plot.py",
        "plot/lifecycle_plot.py",
        "plot/cache_retention_plot.py",
        "plot/radix_tree_plot.py",
    ],
    imports = ["."],
)

py_binary(
    name = "cache_read_interval_plot",
    srcs = ["plot/cache_read_interval_plot.py"],
    main = "plot/cache_read_interval_plot.py",
)

py_binary(
    name = "cache_read_interval_distribution_plot",
    srcs = ["plot/cache_read_interval_distribution_plot.py"],
    main = "plot/cache_read_interval_distribution_plot.py",
)

py_binary(
    name = "cache_read_interval_cdf_plot",
    srcs = ["plot/cache_read_interval_cdf_plot.py"],
    main = "plot/cache_read_interval_cdf_plot.py",
)

py_library(
    name = "analysis_lib",
    srcs = [
        "analysis/__init__.py",
        "analysis/lifecycle_analysis.py",
    ],
    imports = ["."],
)

# =========================================================================
# 入口二进制
# =========================================================================

# 单次 optimizer 运行 + 命中率时序图
py_binary(
    name = "optimizer_run",
    srcs = ["run/optimizer_run.py"],
    main = "run/optimizer_run.py",
    deps = [":utils_lib", ":plot_lib"],
)

# Pareto 曲线分析（单策略 / 多策略自动切换）
py_binary(
    name = "tradeoff",
    srcs = ["run/tradeoff.py"],
    main = "run/tradeoff.py",
    deps = [":utils_lib", ":plot_lib"],
)

# Lightweight infinite-capacity warmup checker.  It intentionally avoids the
# pybind optimizer path so it can be used to cross-check tradeoff warmup output.
py_binary(
    name = "check_warmup_prefix",
    srcs = ["run/check_warmup_prefix.py"],
    main = "run/check_warmup_prefix.py",
)

# RadixTree 导出 + 可视化
py_binary(
    name = "export_tree",
    srcs = ["run/export_tree.py"],
    main = "run/export_tree.py",
    deps = [":utils_lib", ":plot_lib"],
)

# Block Lifecycle 分析（统计 + CDF + 直方图）
py_binary(
    name = "analyze_lifecycle",
    srcs = ["run/analyze_lifecycle.py"],
    main = "run/analyze_lifecycle.py",
    deps = [":analysis_lib", ":plot_lib"],
)

# Hierarchical replay with P2P / load_balance scheduling
py_binary(
    name = "hierarchical_replay",
    srcs = ["run/hierarchical_replay.py"],
    main = "run/hierarchical_replay.py",
    deps = [":utils_lib"],
)

# Per-inference-instance replay: one optimizer process per inference trace plus token-hit-rate aggregation
py_binary(
    name = "multi_infer_replay",
    srcs = ["run/multi_infer_replay.py"],
    main = "run/multi_infer_replay.py",
    deps = [":utils_lib"],
)

py_library(
    name = "multi_infer_replay_lib",
    srcs = [
        "run/__init__.py",
        "run/multi_infer_replay.py",
    ],
    imports = ["."],
)

py_library(
    name = "deepseek_trace_lib",
    srcs = [
        "run/prepare_deepseek_trace.py",
    ],
    imports = ["run"],
)

py_binary(
    name = "prepare_deepseek_trace",
    srcs = ["run/prepare_deepseek_trace.py"],
    main = "run/prepare_deepseek_trace.py",
)

py_binary(
    name = "split_deepseek_trace_by_service",
    srcs = ["run/split_deepseek_trace_by_service.py"],
    main = "run/split_deepseek_trace_by_service.py",
    deps = [":deepseek_trace_lib"],
)

py_binary(
    name = "prepare_deepseek_stream_config",
    srcs = ["run/prepare_deepseek_stream_config.py"],
    main = "run/prepare_deepseek_stream_config.py",
    deps = [":deepseek_trace_lib"],
)

py_binary(
    name = "stream_deepseek_optimizer_trace",
    srcs = ["run/stream_deepseek_optimizer_trace.py"],
    main = "run/stream_deepseek_optimizer_trace.py",
    deps = [":deepseek_trace_lib"],
)

py_binary(
    name = "prepare_tokenized_with_ids_trace",
    srcs = ["run/prepare_tokenized_with_ids_trace.py"],
    main = "run/prepare_tokenized_with_ids_trace.py",
)
