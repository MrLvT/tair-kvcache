#!/usr/bin/env python3
"""
Prepare DeepSeek enriched JSONL traces for optimizer replay.

The input trace carries raw block hash ids in input_block_hash_ids. Optimizer
replay needs prefix-dependent block keys, so this script applies the same
Jenkins-style prefix hash used by tools/trace_converter/utils/prefix_hash.py.
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Optional, Tuple


UINT64_MASK = 0xFFFFFFFFFFFFFFFF
INT64_SIGN = 0x8000000000000000
UINT64_MOD = 0x10000000000000000
GOLDEN_RATIO = 0x9E3779B97F4A7C15


def main():
    parser = argparse.ArgumentParser(
        description="Convert DeepSeek enriched trace JSONL to optimizer JSONL plus replay config")
    parser.add_argument("--input", action="append", required=True,
                        help="Input enriched JSONL. Can be passed multiple times")
    parser.add_argument("--output-dir", required=True,
                        help="Output directory for optimizer trace, config, and metadata")
    parser.add_argument("--name", default="deepseek",
                        help="Output name prefix")
    parser.add_argument("--block-size", type=int, default=256)
    parser.add_argument("--bytes-per-token", type=int, default=92773)
    parser.add_argument("--quota-capacity", type=float, default=128.0,
                        help="Capacity in GB for each generated instance group")
    parser.add_argument("--eviction-policy", default="lru")
    parser.add_argument("--eviction-mode", type=int, default=3,
                        help="Optimizer eviction_mode; 3 is instance precise")
    parser.add_argument("--eviction-batch-size-per-instance", type=int, default=100)
    parser.add_argument("--grouping", choices=["per-instance", "single-group"], default="per-instance",
                        help="per-instance matches one pod per instance group; single-group pools all instances")
    _add_bool_arg(parser, "--sort", default=True,
                  help="Sort emitted optimizer JSONL by timestamp_ns using external sort")
    parser.add_argument("--keep-unsorted", action="store_true",
                        help="Keep the intermediate unsorted optimizer JSONL when --sort is enabled")
    _add_bool_arg(parser, "--prefix-hash", default=True,
                  help="Apply prefix hash to raw input_block_hash_ids")
    parser.add_argument("--trace-mode", choices=["get-write", "request"], default="get-write",
                        help="Emit explicit get/write rows, or request rows whose write time is simulated by optimizer")
    parser.add_argument("--compute-preset", choices=["none", "qwen36-short-p50"], default="none",
                        help="Use a known compute-time formula preset. qwen36-short-p50 supports 1ff1/e1b8")
    parser.add_argument("--compute-base-latency-ns", type=int, default=None,
                        help="Enable trace_replay.compute_time with this base latency")
    parser.add_argument("--compute-miss-block-latency-ns", type=int, default=0,
                        help="Additional simulated latency per missed block")
    parser.add_argument("--compute-miss-block-position-latency-ns", type=int, default=0,
                        help="Additional simulated latency per missed block position")
    parser.add_argument("--compute-latency-offset-ns", type=int, default=0,
                        help="Fixed additive latency offset, useful for deterministic p90/p99 replay")
    parser.add_argument("--compute-scheduler-load-divisor", type=float, default=1.0,
                        help="Deprecated throughput approximation; use --compute-scheduler-lane-count instead")
    parser.add_argument("--compute-scheduler-lane-count", type=int, default=1,
                        help="Number of concurrent execution lanes per scheduler key")
    parser.add_argument("--compute-scheduler-lane-count-alternate", type=int, default=0,
                        help="Optional alternate lane count assigned to half of scheduler keys by stable hash")
    parser.add_argument("--compute-noise-offsets-ns", default="",
                        help="Comma-separated sampled noise offsets in ns, e.g. 0,23866000000,57474000000")
    parser.add_argument("--compute-noise-weights", default="",
                        help="Comma-separated weights matching --compute-noise-offsets-ns, e.g. 0.9,0.09,0.01")
    parser.add_argument("--compute-noise-seed", type=int, default=0,
                        help="Seed mixed into deterministic per-trace noise sampling")
    parser.add_argument("--limit", type=int, default=None,
                        help="Convert at most this many source rows")
    parser.add_argument("--config-path-root", default=None,
                        help="Optional repo root path to write in config, e.g. /workspace/tair-kvcache")
    parser.add_argument("--output-result-subdir", default="output_stream",
                        help="Subdirectory under output-dir for optimizer CSV output")
    args = parser.parse_args()

    if args.block_size <= 0 or args.bytes_per_token <= 0:
        raise SystemExit("--block-size and --bytes-per-token must be positive")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_output = output_dir / f"{args.name}.optimizer.jsonl"
    final_output = output_dir / (
        f"{args.name}.sorted.optimizer.jsonl" if args.sort else f"{args.name}.optimizer.jsonl")
    temp_sort_input = output_dir / f".{args.name}.sort_input.tsv"

    stats = _convert_inputs(
        inputs=[Path(p) for p in args.input],
        raw_output=raw_output,
        temp_sort_input=temp_sort_input if args.sort else None,
        block_size=args.block_size,
        prefix_hash=args.prefix_hash,
        trace_mode=args.trace_mode,
        limit=args.limit,
    )

    if args.sort:
        _sort_optimizer_trace(temp_sort_input, final_output)
        _unlink_if_exists(temp_sort_input)
        if not args.keep_unsorted and raw_output != final_output:
            _unlink_if_exists(raw_output)

    _write_service_mapping(output_dir / "pod_service_mapping.csv", stats["pod_to_service"])

    output_result_path = output_dir / args.output_result_subdir
    output_result_path.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / f"{args.name}_config.json"
    config = _build_config(
        trace_path=_config_path(final_output, args.config_path_root),
        output_result_path=_config_path(output_result_path, args.config_path_root),
        instances=sorted(stats["instances"]),
        name=args.name,
        grouping=args.grouping,
        quota_capacity=args.quota_capacity,
        block_size=args.block_size,
        bytes_per_token=args.bytes_per_token,
        eviction_policy=args.eviction_policy,
        eviction_mode=args.eviction_mode,
        eviction_batch_size=args.eviction_batch_size_per_instance,
        compute_time_config=_compute_time_config(args, stats["services"]),
    )
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, sort_keys=False)
        f.write("\n")

    manifest = dict(stats)
    manifest["instances"] = len(stats["instances"])
    manifest["services"] = len(stats["services"])
    manifest["pod_service_pairs"] = len(stats["pod_to_service"])
    manifest.pop("pod_to_service")
    manifest.pop("services")
    manifest["optimizer_trace"] = str(final_output)
    manifest["config"] = str(config_path)
    manifest["prefix_hash"] = args.prefix_hash
    manifest["grouping"] = args.grouping
    manifest["trace_mode"] = args.trace_mode
    with open(output_dir / "prepare_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Rows converted: {stats['rows_converted']}")
    print(f"Optimizer traces: {stats['optimizer_traces']}")
    print(f"Instances: {len(stats['instances'])}")
    print(f"Services: {len(stats['services'])}")
    print(f"Trace: {final_output}")
    print(f"Config: {config_path}")
    if stats["multi_pod_rows"]:
        print(f"Warning: {stats['multi_pod_rows']} rows had multiple pods; the first pod was used")
    if stats["short_hash_rows"]:
        print(f"Warning: {stats['short_hash_rows']} rows had fewer hashes than full input blocks")


def _add_bool_arg(parser: argparse.ArgumentParser, name: str, default: bool, help: str):
    if hasattr(argparse, "BooleanOptionalAction"):
        parser.add_argument(name, action=argparse.BooleanOptionalAction, default=default, help=help)
        return
    parser.add_argument(name, dest=name.lstrip("-").replace("-", "_"), action="store_true", help=help)
    parser.add_argument(
        f"--no-{name.lstrip('-')}",
        dest=name.lstrip("-").replace("-", "_"),
        action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.set_defaults(**{name.lstrip("-").replace("-", "_"): default})


def _unlink_if_exists(path: Path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _convert_inputs(
    inputs: List[Path],
    raw_output: Path,
    temp_sort_input: Optional[Path],
    block_size: int,
    prefix_hash: bool,
    trace_mode: str,
    limit: Optional[int],
) -> Dict:
    stats = {
        "source_rows": 0,
        "rows_converted": 0,
        "optimizer_traces": 0,
        "skipped_rows": 0,
        "multi_service_rows": 0,
        "multi_pod_rows": 0,
        "short_hash_rows": 0,
        "empty_block_rows": 0,
        "instances": set(),
        "services": set(),
        "pod_to_service": {},
    }

    sort_f = open(temp_sort_input, "w", encoding="utf-8") if temp_sort_input else None
    seq = 0
    try:
        with open(raw_output, "w", encoding="utf-8") as out:
            for input_path in inputs:
                with open(input_path, "r", encoding="utf-8") as f:
                    for line_no, line in enumerate(f, 1):
                        if limit is not None and stats["rows_converted"] >= limit:
                            return stats
                        if not line.strip():
                            continue
                        stats["source_rows"] += 1
                        try:
                            row = json.loads(line)
                            traces, row_meta = _convert_row(row, block_size, prefix_hash, trace_mode)
                        except Exception as exc:
                            stats["skipped_rows"] += 1
                            print(f"Warning: skipped {input_path}:{line_no}: {exc}", file=sys.stderr)
                            continue

                        stats["rows_converted"] += 1
                        stats["optimizer_traces"] += len(traces)
                        stats["instances"].add(row_meta["instance_id"])
                        if row_meta["service"]:
                            stats["services"].add(row_meta["service"])
                            _record_pod_service(stats["pod_to_service"], row_meta["pods"], row_meta["service"])
                        if row_meta["multi_service"]:
                            stats["multi_service_rows"] += 1
                        if row_meta["multi_pod"]:
                            stats["multi_pod_rows"] += 1
                        if row_meta["short_hash"]:
                            stats["short_hash_rows"] += 1
                        if row_meta["empty_blocks"]:
                            stats["empty_block_rows"] += 1

                        for trace in traces:
                            payload = json.dumps(trace, separators=(",", ":"))
                            out.write(payload)
                            out.write("\n")
                            if sort_f:
                                sort_f.write(f"{trace['timestamp_ns']}\t{seq}\t{payload}\n")
                                seq += 1

                        if stats["rows_converted"] % 100000 == 0:
                            print(f"Converted {stats['rows_converted']} rows...", flush=True)
    finally:
        if sort_f:
            sort_f.close()

    return stats


def _convert_row(row: Dict, block_size: int, prefix_hash: bool, trace_mode: str = "get-write") -> Tuple[List[Dict], Dict]:
    request_id = str(row.get("request_id") or "")
    if not request_id:
        raise ValueError("missing request_id")

    timestamp = row.get("timestamp")
    if timestamp is None:
        raise ValueError("missing timestamp")
    timestamp_ns = int(float(timestamp) * 1_000_000_000)
    if timestamp_ns <= 0:
        raise ValueError("invalid timestamp")

    input_len = int(row.get("input_length") or 0)
    if input_len <= 0:
        raise ValueError("input_length must be positive")

    pods = [str(p) for p in (row.get("pods") or []) if str(p)]
    if not pods:
        raise ValueError("missing pods")
    instance_id = pods[0]

    services = [str(s) for s in (row.get("service_names") or []) if str(s)]
    service = services[0] if services else ""

    raw_hash_ids = row.get("input_block_hash_ids") or []
    full_blocks = input_len // block_size
    usable_blocks = min(full_blocks, len(raw_hash_ids))
    block_keys = [_parse_signed_i64_hash(v) for v in raw_hash_ids[:usable_blocks]]
    if prefix_hash:
        block_keys = apply_prefix_hash(block_keys)

    request_trace = {
        "type": "request" if trace_mode == "request" else "get",
        "instance_id": instance_id,
        "trace_id": f"trace_{request_id}" if trace_mode == "request" else f"trace_{request_id}_get",
        "timestamp_ns": timestamp_ns,
        "keys": block_keys,
        "input_len": input_len,
        "query_type": "prefix_match",
        "block_mask": [],
        "sw_size": 0,
        "location_spec_names": [],
    }
    traces = [request_trace]
    if trace_mode == "get-write":
        traces.append({
            "type": "write",
            "instance_id": instance_id,
            "trace_id": f"trace_{request_id}_write",
            "timestamp_ns": timestamp_ns + 1,
            "keys": block_keys,
        })
    return traces, {
        "instance_id": instance_id,
        "service": service,
        "pods": pods,
        "multi_service": len(services) != 1,
        "multi_pod": len(pods) != 1,
        "short_hash": len(raw_hash_ids) < full_blocks,
        "empty_blocks": usable_blocks == 0,
    }


def _parse_signed_i64_hash(value) -> int:
    if isinstance(value, int):
        unsigned = value & UINT64_MASK
    elif isinstance(value, str):
        text = value.strip()
        if text.startswith("-"):
            return int(text)
        if text.startswith(("0x", "0X")):
            text = text[2:]
        unsigned = int(text, 16) & UINT64_MASK
    else:
        raise ValueError(f"unsupported hash id type: {type(value).__name__}")
    return _to_signed_i64(unsigned)


def _to_signed_i64(value: int) -> int:
    value &= UINT64_MASK
    if value >= INT64_SIGN:
        return value - UINT64_MOD
    return value


def hash_int64_func(prev_hash: int, current_value: int) -> int:
    hash_unsigned = prev_hash & UINT64_MASK
    value_unsigned = current_value & UINT64_MASK
    left_shift = (hash_unsigned << 12) & UINT64_MASK
    right_shift = hash_unsigned >> 32
    rhs = (value_unsigned + GOLDEN_RATIO + left_shift + right_shift) & UINT64_MASK
    return _to_signed_i64(hash_unsigned ^ rhs)


def apply_prefix_hash(hash_ids: List[int]) -> List[int]:
    block_keys = []
    hash_value = 0
    for hash_id in hash_ids:
        hash_value = hash_int64_func(hash_value, hash_id)
        block_keys.append(hash_value)
    return block_keys


def _record_pod_service(pod_to_service: Dict[str, str], pods: List[str], service: str):
    for pod in pods:
        previous = pod_to_service.get(pod)
        if previous is not None and previous != service:
            raise ValueError(f"pod {pod} maps to both {previous} and {service}")
        pod_to_service[pod] = service


def _sort_optimizer_trace(temp_sort_input: Path, output_path: Path):
    cmd = ["sort", "-s", "-t", "\t", "-k1,1n", "-k2,2n", str(temp_sort_input)]
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    with open(output_path, "w", encoding="utf-8") as out:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            out.write(line.split("\t", 2)[2])
        ret = proc.wait()
    if ret != 0:
        raise subprocess.CalledProcessError(ret, cmd)


def _write_service_mapping(path: Path, pod_to_service: Dict[str, str]):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["pod", "service"])
        for pod, service in sorted(pod_to_service.items()):
            writer.writerow([pod, service])


def _build_config(
    trace_path: str,
    output_result_path: str,
    instances: List[str],
    name: str,
    grouping: str,
    quota_capacity: float,
    block_size: int,
    bytes_per_token: int,
    eviction_policy: str,
    eviction_mode: int,
    eviction_batch_size: int,
    compute_time_config: Optional[Dict] = None,
) -> Dict:
    if not instances:
        raise ValueError("no instances found")
    groups = []
    if grouping == "single-group":
        groups.append(_instance_group(
            group_name=_safe_name(name),
            instances=instances,
            quota_capacity=quota_capacity,
            block_size=block_size,
            bytes_per_token=bytes_per_token,
            eviction_policy=eviction_policy,
        ))
    else:
        for instance in instances:
            groups.append(_instance_group(
                group_name=f"{_safe_name(name)}_{_safe_name(instance)}",
                instances=[instance],
                quota_capacity=quota_capacity,
                block_size=block_size,
                bytes_per_token=bytes_per_token,
                eviction_policy=eviction_policy,
            ))

    config = {
        "trace_file_path": trace_path,
        "output_result_path": output_result_path,
        "eviction_params": {
            "eviction_mode": eviction_mode,
            "eviction_batch_size_per_instance": eviction_batch_size,
        },
        "instance_groups": groups,
    }
    if compute_time_config is not None:
        config["trace_replay"] = {
            "write_delay_ns": 1,
            "compute_time": compute_time_config,
        }
    return config


def _compute_time_config(args, services: Iterable[str]) -> Optional[Dict]:
    if args.compute_preset != "none":
        if (
            args.compute_base_latency_ns is not None
            or args.compute_miss_block_latency_ns != 0
            or args.compute_miss_block_position_latency_ns != 0
            or args.compute_latency_offset_ns != 0
            or args.compute_scheduler_load_divisor != 1.0
            or args.compute_scheduler_lane_count != 1
            or args.compute_scheduler_lane_count_alternate != 0
            or args.compute_noise_offsets_ns
            or args.compute_noise_weights
            or args.compute_noise_seed != 0
        ):
            raise SystemExit("--compute-preset cannot be combined with explicit compute latency parameters")
        return _compute_time_preset(args.compute_preset, services)

    if args.compute_base_latency_ns is None:
        return None
    if (
        args.compute_miss_block_latency_ns < 0
        or args.compute_miss_block_position_latency_ns < 0
        or args.compute_latency_offset_ns < 0
        or args.compute_noise_seed < 0
    ):
        raise SystemExit("compute miss latency, offset, and seed parameters must be non-negative")
    if args.compute_scheduler_load_divisor != 1.0:
        raise SystemExit("--compute-scheduler-load-divisor is deprecated; use --compute-scheduler-lane-count")
    if args.compute_scheduler_lane_count <= 0 or args.compute_scheduler_lane_count_alternate < 0:
        raise SystemExit("--compute-scheduler-lane-count must be positive and alternate must be non-negative")
    noise_offsets = _parse_int_list(args.compute_noise_offsets_ns)
    noise_weights = _parse_float_list(args.compute_noise_weights)
    if noise_offsets and len(noise_offsets) != len(noise_weights):
        raise SystemExit("--compute-noise-offsets-ns and --compute-noise-weights must have the same length")
    if noise_weights and not noise_offsets:
        raise SystemExit("--compute-noise-weights requires --compute-noise-offsets-ns")
    config = {
        "enabled": True,
        "queue_by_instance_group": True,
        "base_latency_ns": args.compute_base_latency_ns,
        "miss_block_latency_ns": args.compute_miss_block_latency_ns,
        "miss_block_position_latency_ns": args.compute_miss_block_position_latency_ns,
        "latency_offset_ns": args.compute_latency_offset_ns,
        "scheduler_load_divisor": args.compute_scheduler_load_divisor,
        "scheduler_lane_count": args.compute_scheduler_lane_count,
    }
    if args.compute_scheduler_lane_count_alternate:
        config["scheduler_lane_count_alternate"] = args.compute_scheduler_lane_count_alternate
    if noise_offsets:
        config["noise_offsets_ns"] = noise_offsets
        config["noise_weights"] = noise_weights
        config["noise_seed"] = args.compute_noise_seed
    return config


def _compute_time_preset(preset: str, services: Iterable[str]) -> Dict:
    service_list = sorted(services)
    if len(service_list) != 1:
        raise SystemExit(f"--compute-preset {preset} requires exactly one service, got {service_list}")
    service = service_list[0]
    if preset == "qwen36-short-p50":
        if service.endswith("-1ff1"):
            return {
                "enabled": True,
                "queue_by_instance_group": True,
                "base_latency_ns": 359_000_000,
                "miss_block_latency_ns": 13_270_000,
                "miss_block_position_latency_ns": 0,
                "latency_offset_ns": 0,
                "scheduler_lane_count": 4,
            }
        if service.endswith("-e1b8"):
            return {
                "enabled": True,
                "queue_by_instance_group": True,
                "base_latency_ns": -323_000_000,
                "miss_block_latency_ns": 4_140_000,
                "miss_block_position_latency_ns": 0,
                "latency_offset_ns": 0,
                "scheduler_lane_count": 2,
                "scheduler_lane_count_alternate": 3,
            }
        raise SystemExit(f"--compute-preset {preset} does not support service {service}")
    raise SystemExit(f"unknown compute preset: {preset}")


def _parse_int_list(raw: str) -> List[int]:
    if not raw:
        return []
    values = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if any(value < 0 for value in values):
        raise SystemExit("integer list values must be non-negative")
    return values


def _parse_float_list(raw: str) -> List[float]:
    if not raw:
        return []
    values = [float(part.strip()) for part in raw.split(",") if part.strip()]
    if any(value < 0 for value in values) or sum(values) <= 0:
        raise SystemExit("float list values must be non-negative and sum to a positive value")
    return values


def _instance_group(
    group_name: str,
    instances: List[str],
    quota_capacity: float,
    block_size: int,
    bytes_per_token: int,
    eviction_policy: str,
) -> Dict:
    return {
        "group_name": group_name,
        "quota_capacity": quota_capacity,
        "used_percentage": 1.0,
        "tier_strategy": {
            "hierarchical_eviction_enabled": False,
            "write_mode": "write_through",
            "access_propagation_enabled": True,
            "promote_enabled": True,
            "selective_write_threshold": 2,
        },
        "default_block_ttl_seconds": 0,
        "ttl_refresh_on_read": True,
        "storages": [
            {
                "unique_name": "shared_00",
                "storage_type": "memory",
                "band_width_mbps": 20000,
                "priority": 0,
                "capacity": quota_capacity,
            }
        ],
        "instances": [
            {
                "instance_id": instance,
                "block_size": block_size,
                "bytes_per_token": bytes_per_token,
                "eviction_policy_type": eviction_policy,
                "eviction_policy_params": {
                    "sample_rate": 1.0,
                    "shard_count": 1,
                    "sample_times": 32,
                    "eviction_amplification_factor": 1.0,
                },
            }
            for instance in instances
        ],
    }


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "group"


def _config_path(path: Path, config_path_root: Optional[str]) -> str:
    resolved = path.resolve()
    if not config_path_root:
        return str(resolved)
    cwd = Path.cwd().resolve()
    try:
        rel = resolved.relative_to(cwd)
    except ValueError:
        rel = Path(resolved.name)
    return str(PurePosixPath(config_path_root) / PurePosixPath(rel.as_posix()))


if __name__ == "__main__":
    main()
