#!/usr/bin/env python3
"""
Hierarchical Replay runner — runs HierarchicalReplayManager with P2P / load_balance scheduling.

Converts an existing per-instance OptimizerConfig (as used by multi_infer_replay.py) into a
HierarchicalReplayConfig, or accepts a pre-built hierarchical config directly.

Usage examples:

  # From existing OptimizerConfig — preserve_trace scheduling (baseline)
  python run/hierarchical_replay.py \
      --from-optimizer-config prepared_1ff1_sorted/1ff1_config.json \
      --output-dir /tmp/hier_1ff1_baseline \
      --scheduling-strategy preserve_trace

  # With P2P enabled on dram tier
  python run/hierarchical_replay.py \
      --from-optimizer-config prepared_1ff1_sorted/1ff1_config.json \
      --output-dir /tmp/hier_1ff1_p2p \
      --scheduling-strategy preserve_trace \
      --p2p-tier dram

  # With load_balance scheduling + linear predictor + 4 concurrent lanes
  python run/hierarchical_replay.py \
      --from-optimizer-config prepared_1ff1_sorted/1ff1_config.json \
      --output-dir /tmp/hier_1ff1_lb \
      --scheduling-strategy load_balance \
      --predictor-intercept-ms 359 \
      --predictor-slope-ms-per-block 13.27 \
      --concurrency 4

  # From pre-built hierarchical config
  python run/hierarchical_replay.py \
      --hierarchical-config hier_config.json \
      --output-dir /tmp/hier_out
"""

import argparse
import json
import os
import sys
import time


def _init_logger(log_level=4):
    from kv_cache_manager.optimizer.pybind import kvcm_py_optimizer

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True)
    os.environ.setdefault("KVCM_LOG_TO_CONSOLE", "1")
    kvcm_py_optimizer.LoggerBroker.InitLogger("", False)
    kvcm_py_optimizer.LoggerBroker.SetLogLevel(log_level)


def convert_optimizer_to_hierarchical_config(
    optimizer_config: dict,
    output_dir: str,
    scheduling_strategy: str = "preserve_trace",
    p2p_tier: str = None,
    p2p_peer_read_touch: bool = True,
    tier_name: str = "shared",
    storage_pool_id: str = "model_l3",
    write_delay_ns: int = 1,
) -> dict:
    """Convert a multi-instance OptimizerConfig JSON into a HierarchicalReplayConfig JSON."""

    groups = optimizer_config.get("instance_groups", [])
    if not groups:
        raise ValueError("OptimizerConfig has no instance_groups")

    # Collect all instance IDs and validate they share the same model config.
    infer_ids = []
    representative_instance = None
    representative_block_size = None
    representative_bytes_per_token = None
    tier_capacity = None

    for group in groups:
        instances = group.get("instances", [])
        if len(instances) != 1:
            raise ValueError(
                f"Expected exactly 1 instance per group for hierarchical conversion, "
                f"group '{group.get('group_name')}' has {len(instances)}"
            )
        instance = instances[0]
        instance_id = instance["instance_id"]
        infer_ids.append(instance_id)

        block_size = instance.get("block_size", 0)
        bytes_per_token = instance.get("bytes_per_token", 0)

        if representative_instance is None:
            representative_instance = instance
            representative_block_size = block_size
            representative_bytes_per_token = bytes_per_token
            storages = group.get("storages", [])
            if storages:
                tier_capacity = storages[0].get("capacity", -1.0)
        else:
            if block_size != representative_block_size:
                raise ValueError(
                    f"Instance {instance_id} has block_size={block_size}, "
                    f"expected {representative_block_size}"
                )

    eviction_policy = representative_instance.get("eviction_policy_type", "lru")
    eviction_params = representative_instance.get("eviction_policy_params", {"sample_rate": 1.0})

    # Build tier config — use a single tier (the first storage) from the original config.
    # For hierarchical configs, capacity is in GB. Must be > 0 for C++ validation.
    capacity_gb = tier_capacity if (tier_capacity is not None and tier_capacity > 0) else 999999.0

    tiers = [{"name": tier_name, "capacity": capacity_gb}]

    # Build P2P flows if requested.
    p2p_read_flows = []
    if p2p_tier:
        p2p_read_flows.append({
            "tier": p2p_tier,
            "peer_read_touch_enabled": p2p_peer_read_touch,
        })

    # Build storage pool flow.
    storage_pool_flow = {
        "write_mode": "write_through",
        "local_read_touch_enabled": False,
        "shadow_write_touch_enabled": False,
        "selective_write_threshold": 2,
    }

    # Build the infer_cluster.
    infer_cluster = {
        "storage_pool_id": storage_pool_id,
        "engine_read_query_type": "prefix_match",
        "model": {
            "block_size": representative_block_size,
            "bytes_per_token": representative_bytes_per_token,
            "eviction_policy_type": eviction_policy,
            "eviction_policy_params": eviction_params,
        },
        "infer_ids": sorted(infer_ids),
        "ttl_config": {
            "default_block_ttl_seconds": 0,
            "refresh_on_read": True,
        },
        "tiers": tiers,
        "storage_pool_flow": storage_pool_flow,
    }
    if p2p_read_flows:
        infer_cluster["p2p_read_flows"] = p2p_read_flows

    # Build storage pool config.
    # capacity must be > 0.0 for C++ validation; use a very large value for "unlimited".
    storage_pool_capacity_gb = 999999.0

    storage_pool = {
        "output_result_path": os.path.join(output_dir, "pool"),
        "storage_name": "l3_pool",
        "capacity": storage_pool_capacity_gb,
        "eviction_params": optimizer_config.get("eviction_params", {
            "eviction_mode": 3,
            "eviction_batch_size_per_instance": 100,
        }),
        "ttl_config": {
            "default_block_ttl_seconds": 0,
            "refresh_on_read": True,
        },
        "pools": [{
            "pool_id": storage_pool_id,
            "model": {
                "block_size": representative_block_size,
                "bytes_per_token": representative_bytes_per_token,
                "eviction_policy_type": eviction_policy,
                "eviction_policy_params": eviction_params,
            },
        }],
    }

    # Build the full hierarchical config.
    trace_replay_mode = optimizer_config.get("trace_replay", {}).get("mode", "request")

    hierarchical_config = {
        "trace_file_path": optimizer_config["trace_file_path"],
        "output_result_path": output_dir,
        "infer_eviction_params": optimizer_config.get("eviction_params", {
            "eviction_mode": 3,
            "eviction_batch_size_per_instance": 100,
        }),
        "trace_replay": {
            "mode": trace_replay_mode,
            "write_delay_ns": write_delay_ns,
        },
        "infer_scheduling_strategy": scheduling_strategy,
        "infer_active_windows_from_trace": True,
        "infer_concurrency": 1,  # overridden by caller if needed
        "infer_clusters": [infer_cluster],
        "storage_pool": storage_pool,
    }

    return hierarchical_config


def make_linear_predictor(intercept_ns, slope_ns_per_block, block_size):
    """Create a linear prefill duration predictor.

    T(input_len, cache_hit_len) = intercept_ns + slope_ns_per_block * miss_blocks

    where miss_blocks = ceil((input_len - cache_hit_len) / block_size)

    Concurrency (batch parallelism) is handled by the C++ scheduler's
    multi-lane model, not by dividing the predicted time.
    """
    import math

    def predictor(input_len: int, cache_hit_len: int) -> int:
        miss_tokens = max(0, input_len - cache_hit_len)
        miss_blocks = math.ceil(miss_tokens / block_size) if block_size > 0 else miss_tokens
        duration_ns = intercept_ns + slope_ns_per_block * miss_blocks
        return max(1, int(duration_ns))

    return predictor


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run HierarchicalReplayManager with P2P / load_balance scheduling",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    config_group = parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument(
        "--from-optimizer-config",
        help="Path to existing OptimizerConfig JSON (multi_infer_replay style)",
    )
    config_group.add_argument(
        "--hierarchical-config",
        help="Path to pre-built HierarchicalReplayConfig JSON",
    )

    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument(
        "--scheduling-strategy",
        default="preserve_trace",
        choices=["preserve_trace", "round_robin", "prefix_hit", "load_balance"],
        help="Infer scheduling strategy",
    )

    # P2P options
    parser.add_argument("--p2p-tier", default=None, help="Enable P2P read on this tier name (e.g. 'shared')")
    parser.add_argument("--no-p2p-touch", action="store_true", help="Disable peer_read_touch on P2P")

    # Predictor options for load_balance
    parser.add_argument(
        "--predictor-intercept-ms", type=float, default=None,
        help="Linear predictor intercept in milliseconds",
    )
    parser.add_argument(
        "--predictor-slope-ms-per-block", type=float, default=None,
        help="Linear predictor slope in ms per miss block",
    )

    # Tier config
    parser.add_argument("--tier-name", default="shared", help="Tier name for converted config")
    parser.add_argument("--tier-capacity-gb", type=float, default=-1.0, help="Tier capacity in GB (-1 for unlimited)")

    # Concurrency
    parser.add_argument(
        "--concurrency", type=int, default=1,
        help="Number of concurrent prefill lanes per engine (default 1 = serial)",
    )

    # Write delay
    parser.add_argument("--write-delay-ns", type=int, default=1, help="Write delay in nanoseconds")

    parser.add_argument("--log-level", type=int, default=4)

    return parser.parse_args()


def main():
    args = parse_args()
    _init_logger(args.log_level)
    start_time = time.time()

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # ----------------------------------------------------------------
    # Build or load hierarchical config
    # ----------------------------------------------------------------
    if args.from_optimizer_config:
        print(f"Converting OptimizerConfig: {args.from_optimizer_config}")
        with open(args.from_optimizer_config, "r") as f:
            optimizer_config = json.load(f)

        hier_config_dict = convert_optimizer_to_hierarchical_config(
            optimizer_config,
            output_dir=output_dir,
            scheduling_strategy=args.scheduling_strategy,
            p2p_tier=args.p2p_tier,
            p2p_peer_read_touch=not args.no_p2p_touch,
            tier_name=args.tier_name,
            write_delay_ns=args.write_delay_ns,
        )

        # Override tier capacity if specified.
        if args.tier_capacity_gb > 0:
            for cluster in hier_config_dict["infer_clusters"]:
                for tier in cluster["tiers"]:
                    tier["capacity"] = args.tier_capacity_gb

        # Set concurrency.
        if args.concurrency > 1:
            hier_config_dict["infer_concurrency"] = args.concurrency

        # Ensure output paths are absolute.
        hier_config_dict["output_result_path"] = output_dir
        hier_config_dict["storage_pool"]["output_result_path"] = os.path.join(output_dir, "pool")
    else:
        print(f"Loading hierarchical config: {args.hierarchical_config}")
        with open(args.hierarchical_config, "r") as f:
            hier_config_dict = json.load(f)
        hier_config_dict["output_result_path"] = output_dir
        if args.scheduling_strategy != "preserve_trace":
            hier_config_dict["infer_scheduling_strategy"] = args.scheduling_strategy
        if args.concurrency > 1:
            hier_config_dict["infer_concurrency"] = args.concurrency

    # Save the generated config for reproducibility.
    generated_config_path = os.path.join(output_dir, "hierarchical_config.json")
    with open(generated_config_path, "w") as f:
        json.dump(hier_config_dict, f, indent=2)
    print(f"Config saved: {generated_config_path}")

    # ----------------------------------------------------------------
    # Load config via C++ loader
    # ----------------------------------------------------------------
    from kv_cache_manager.optimizer.pybind import kvcm_py_optimizer

    config_loader = kvcm_py_optimizer.HierarchicalReplayConfigLoader()
    if not config_loader.load(generated_config_path):
        print("ERROR: Failed to load hierarchical config")
        sys.exit(1)

    config = config_loader.config()

    # Print summary.
    infer_ids = []
    for cluster in hier_config_dict.get("infer_clusters", []):
        infer_ids.extend(cluster.get("infer_ids", []))

    print("\n" + "=" * 70)
    print("  Hierarchical Replay")
    print("=" * 70)
    print(f"  Trace:      {hier_config_dict['trace_file_path']}")
    print(f"  Output:     {output_dir}")
    print(f"  Scheduling: {hier_config_dict.get('infer_scheduling_strategy', 'preserve_trace')}")
    print(f"  Instances:  {len(infer_ids)}")
    print(f"  Concurrency: {hier_config_dict.get('infer_concurrency', 1)} lanes/engine")
    print(f"  P2P:        {'enabled (' + args.p2p_tier + ')' if args.p2p_tier else 'disabled'}")

    # ----------------------------------------------------------------
    # Create manager and optionally inject predictor
    # ----------------------------------------------------------------
    manager = kvcm_py_optimizer.HierarchicalReplayManager(config)
    if not manager.Init():
        print("ERROR: HierarchicalReplayManager.Init() failed")
        sys.exit(1)

    if args.scheduling_strategy == "load_balance":
        if args.predictor_intercept_ms is not None and args.predictor_slope_ms_per_block is not None:
            # Extract block_size from the first cluster.
            block_size = hier_config_dict["infer_clusters"][0]["model"]["block_size"]
            intercept_ns = int(args.predictor_intercept_ms * 1_000_000)
            slope_ns = int(args.predictor_slope_ms_per_block * 1_000_000)
            predictor = make_linear_predictor(intercept_ns, slope_ns, block_size)
            manager.SetPrefillDurationPredictor(predictor)
            print(f"  Predictor:  T = {args.predictor_intercept_ms}ms + "
                  f"{args.predictor_slope_ms_per_block}ms/block")
        else:
            print("  Predictor:  fallback (unit cost per request)")

    print("=" * 70 + "\n")

    # ----------------------------------------------------------------
    # Run
    # ----------------------------------------------------------------
    print("Running hierarchical replay...")
    run_start = time.time()
    manager.DirectRun()
    run_elapsed = time.time() - run_start
    print(f"Replay done in {run_elapsed:.2f}s")

    print("Analyzing results...")
    manager.AnalyzeResults()

    # ----------------------------------------------------------------
    # Print hit rate summary from CSV
    # ----------------------------------------------------------------
    combined_csv = os.path.join(output_dir, "combined", "hierarchical_hit_rates.csv")
    if os.path.isfile(combined_csv):
        _print_hit_rate_summary(combined_csv)
    else:
        print(f"  (no combined CSV found at {combined_csv})")

    capacity_miss_csv = os.path.join(output_dir, "combined", "hierarchical_capacity_miss.csv")
    if os.path.isfile(capacity_miss_csv):
        _print_capacity_miss_summary(capacity_miss_csv)

    total_elapsed = time.time() - start_time
    print(f"\nTotal time: {total_elapsed:.2f}s")


def _print_hit_rate_summary(csv_path: str):
    """Print aggregate hit rate from the hierarchical combined CSV."""
    import csv as csv_mod

    total_read = 0
    total_local_hit = 0
    total_peer_hit = 0
    total_remote_hit = 0
    total_hit = 0
    request_count = 0

    with open(csv_path, "r") as f:
        reader = csv_mod.DictReader(f)
        for row in reader:
            read_blocks = int(row.get("ReadBlocks", 0))
            local_hit = int(row.get("LocalHitBlocks", 0))
            peer_hit = int(row.get("PeerHitBlocks", 0))
            remote_hit = int(row.get("RemoteHitBlocks", 0))
            hit_blocks = int(row.get("HitBlocks", 0))

            total_read += read_blocks
            total_local_hit += local_hit
            total_peer_hit += peer_hit
            total_remote_hit += remote_hit
            total_hit += hit_blocks
            request_count += 1

    if total_read == 0:
        print("  No read blocks recorded.")
        return

    print("\n" + "=" * 70)
    print("  Hit Rate Summary")
    print("=" * 70)
    print(f"  Requests:     {request_count:,}")
    print(f"  Read blocks:  {total_read:,}")
    print(f"  Hit blocks:   {total_hit:,}  ({total_hit / total_read:.4%})")
    print(f"    Local:      {total_local_hit:,}  ({total_local_hit / total_read:.4%})")
    print(f"    Peer:       {total_peer_hit:,}  ({total_peer_hit / total_read:.4%})")
    print(f"    Remote:     {total_remote_hit:,}  ({total_remote_hit / total_read:.4%})")
    print("=" * 70)


def _print_capacity_miss_summary(csv_path: str):
    """Print cumulative counters and the last complete capacity-miss window."""
    import csv as csv_mod

    last_row = None
    last_window_row = None
    with open(csv_path, "r") as f:
        for row in csv_mod.DictReader(f):
            last_row = row
            if row.get("CapacityMissTps5m", "") != "":
                last_window_row = row
    if last_row is None:
        return

    print("\n" + "=" * 70)
    print("  Capacity Miss Summary")
    print("=" * 70)
    print(f"  Capacity miss tokens: {int(last_row['AccCapacityMissTokens']):,}")
    print(f"  Routing miss tokens:  {int(last_row['AccRoutingMissTokens']):,}")
    print(f"  Cold miss tokens:     {int(last_row['AccColdMissTokens']):,}")
    if last_window_row is not None:
        print(f"  Capacity miss TPS:    {float(last_window_row['CapacityMissTps5m']):,.2f}")
        print(f"  Capacity miss ratio:  {float(last_window_row['CapacityMissRatio5m']):.4%}")
    else:
        print("  Capacity miss TPS:    unavailable (trace covers less than one full window)")
    print("=" * 70)


if __name__ == "__main__":
    main()
