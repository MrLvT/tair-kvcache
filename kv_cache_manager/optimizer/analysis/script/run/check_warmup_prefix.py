#!/usr/bin/env python3
"""Lightweight checker for infinite-capacity warmup hit rates.

This script intentionally models only the parts needed to cross-check
`tradeoff` warmup:

* one in-memory prefix index per optimizer instance_id
* no capacity eviction
* request-mode delayed writes
* token hit rate = hit blocks * block_size / input_len

It does not model TTL, tier flow, P2P, storage pool, scheduler, or Mamba state.
"""

import argparse
import heapq
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple


INT64_MIN = -(1 << 63)
INT64_MAX = (1 << 63) - 1
UINT64_MAX = (1 << 64) - 1


@dataclass
class InstanceConfig:
    instance_id: str
    group_name: str
    block_size: int
    bytes_per_token: int = 0


@dataclass
class ReadStats:
    read_count: int = 0
    input_tokens: int = 0
    local_hit_tokens: int = 0
    remote_hit_tokens: int = 0
    hit_tokens: int = 0

    def add(self, input_tokens: int, block_size: int, local_hit_blocks: int, remote_hit_blocks: int) -> None:
        local_hit_tokens = local_hit_blocks * block_size
        remote_hit_tokens = remote_hit_blocks * block_size
        self.read_count += 1
        self.input_tokens += input_tokens
        self.local_hit_tokens += local_hit_tokens
        self.remote_hit_tokens += remote_hit_tokens
        self.hit_tokens += local_hit_tokens + remote_hit_tokens

    def rates(self) -> Dict[str, float]:
        if self.input_tokens <= 0:
            return {"total": 0.0, "local": 0.0, "remote": 0.0}
        return {
            "total": self.hit_tokens / self.input_tokens,
            "local": self.local_hit_tokens / self.input_tokens,
            "remote": self.remote_hit_tokens / self.input_tokens,
        }


@dataclass
class InstanceRuntime:
    config: InstanceConfig
    index: "PrefixIndex" = field(default_factory=lambda: PrefixIndex())
    all_stats: ReadStats = field(default_factory=ReadStats)
    window_stats: ReadStats = field(default_factory=ReadStats)
    write_count: int = 0
    written_blocks: int = 0
    last_cached_blocks_all_at_read: int = 0


class TrieNode:
    __slots__ = ("children",)

    def __init__(self) -> None:
        self.children: Dict[int, "TrieNode"] = {}


class PrefixIndex:
    def __init__(self) -> None:
        self.root = TrieNode()
        self.block_index = set()
        self.materialized_blocks = 0

    def insert(self, keys: Iterable[int]) -> int:
        node = self.root
        inserted = 0
        for key in keys:
            child = node.children.get(key)
            if child is None:
                child = TrieNode()
                node.children[key] = child
                inserted += 1
                self.materialized_blocks += 1
            self.block_index.add(key)
            node = child
        return inserted

    def prefix_match_count(self, keys: List[int]) -> int:
        node = self.root
        matched = 0
        for key in keys:
            child = node.children.get(key)
            if child is None:
                break
            matched += 1
            node = child
        return matched

    def batch_hit_indices(self, keys: List[int]) -> List[int]:
        return [idx for idx, key in enumerate(keys) if key in self.block_index]


def main() -> None:
    parser = argparse.ArgumentParser(description="Check optimizer warmup hit rate with a simple prefix index")
    parser.add_argument("-c", "--config", required=True, help="Optimizer config JSON")
    parser.add_argument("--trace", default=None, help="Override trace_file_path from config")
    parser.add_argument("--metric-start-ns", type=int, default=None,
                        help="Only count hit-rate metrics from this timestamp onward; replay still starts at trace head")
    parser.add_argument("--output-json", default=None, help="Write machine-readable summary JSON")
    parser.add_argument("--warmup-json", default=None, help="Optional tradeoff warmup_result.json to compare against")
    parser.add_argument("--policy-name", default="default_policy",
                        help="Policy key in --warmup-json, default: default_policy")
    args = parser.parse_args()

    try:
        result = run_check(args.config, args.trace, args.metric_start_ns)
        print_summary(result)
        if args.warmup_json:
            compare_warmup_json(result, args.warmup_json, args.policy_name)
        if args.output_json:
            os.makedirs(os.path.dirname(os.path.abspath(args.output_json)), exist_ok=True)
            with open(args.output_json, "w") as f:
                json.dump(result, f, indent=2, sort_keys=True)
            print(f"\nJSON written to {args.output_json}")
    except Exception as exc:
        raise SystemExit(str(exc)) from exc


def run_check(config_path: str, trace_override: Optional[str], metric_start_ns: Optional[int]) -> dict:
    with open(config_path, "r") as f:
        config = json.load(f)

    trace_path = trace_override or config.get("trace_file_path")
    if not trace_path:
        raise ValueError("config missing trace_file_path; pass --trace to override")

    trace_replay = config.get("trace_replay", {}) or {}
    replay_mode = trace_replay.get("mode", "read_write")
    write_delay_ns = int(trace_replay.get("write_delay_ns", 1))
    if replay_mode not in {"read_write", "request"}:
        raise ValueError(f"unsupported trace_replay.mode={replay_mode!r}")
    if write_delay_ns <= 0:
        raise ValueError("trace_replay.write_delay_ns must be positive")

    instances = load_instance_configs(config)
    warn_unsupported_features(config)
    runtime = {iid: InstanceRuntime(cfg) for iid, cfg in instances.items()}
    pending: List[Tuple[int, int, dict]] = []
    next_pending_seq = 0
    max_cached_blocks_all_at_reads = 0
    total_trace_rows = 0

    for line_no, trace in iter_trace_rows(trace_path, replay_mode):
        total_trace_rows += 1
        timestamp_ns = trace["timestamp_ns"]
        max_cached_blocks_all_at_reads = max(
            max_cached_blocks_all_at_reads,
            flush_pending_writes(pending, runtime, timestamp_ns),
        )

        trace_type = trace["type"]
        if trace_type == "request":
            max_cached_blocks_all_at_reads = max(
                max_cached_blocks_all_at_reads,
                handle_read(trace, runtime, metric_start_ns),
            )
            write_trace = {
                "type": "write",
                "instance_id": trace["instance_id"],
                "trace_id": f"{trace.get('trace_id', '')}:write",
                "timestamp_ns": timestamp_ns + write_delay_ns,
                "keys": trace["keys"],
            }
            heapq.heappush(pending, (write_trace["timestamp_ns"], next_pending_seq, write_trace))
            next_pending_seq += 1
        elif trace_type == "get":
            max_cached_blocks_all_at_reads = max(
                max_cached_blocks_all_at_reads,
                handle_read(trace, runtime, metric_start_ns),
            )
        elif trace_type == "write":
            handle_write(trace, runtime)
        else:
            raise ValueError(f"{trace_path}:{line_no} unknown trace type {trace_type!r}")

    flush_pending_writes(pending, runtime, None)

    result_instances = {}
    final_cached_blocks_all = total_cached_blocks(runtime)
    for iid in sorted(runtime):
        rt = runtime[iid]
        stats = rt.window_stats if metric_start_ns is not None else rt.all_stats
        rates = stats.rates()
        bpb = rt.config.block_size * rt.config.bytes_per_token
        result_instances[iid] = {
            "group_name": rt.config.group_name,
            "block_size": rt.config.block_size,
            "bytes_per_token": rt.config.bytes_per_token,
            "read_count": stats.read_count,
            "write_count": rt.write_count,
            "input_tokens": stats.input_tokens,
            "hit_tokens": stats.hit_tokens,
            "local_hit_tokens": stats.local_hit_tokens,
            "remote_hit_tokens": stats.remote_hit_tokens,
            "total": rates["total"],
            "local": rates["local"],
            "remote": rates["remote"],
            "cached_blocks": rt.index.materialized_blocks,
            "last_cached_blocks_all_at_read": rt.last_cached_blocks_all_at_read,
            "cached_gb": rt.last_cached_blocks_all_at_read * bpb / (1024 ** 3) if bpb > 0 else 0.0,
        }

    return {
        "config": config_path,
        "trace": trace_path,
        "trace_rows": total_trace_rows,
        "trace_replay_mode": replay_mode,
        "write_delay_ns": write_delay_ns,
        "metric_start_ns": metric_start_ns,
        "max_blocks": max_cached_blocks_all_at_reads,
        "final_cached_blocks_all": final_cached_blocks_all,
        "instances": result_instances,
    }


def load_instance_configs(config: dict) -> Dict[str, InstanceConfig]:
    instances: Dict[str, InstanceConfig] = {}
    groups = config.get("instance_groups")
    if not isinstance(groups, list) or not groups:
        raise ValueError("config must contain non-empty instance_groups")

    for group in groups:
        group_name = group.get("group_name")
        if not isinstance(group_name, str) or not group_name:
            raise ValueError("each instance group must have a non-empty group_name")
        for inst in group.get("instances", []):
            instance_id = inst.get("instance_id")
            block_size = inst.get("block_size")
            bytes_per_token = inst.get("bytes_per_token", 0)
            if not isinstance(instance_id, str) or not instance_id:
                raise ValueError(f"group {group_name!r} has instance without non-empty instance_id")
            if type(block_size) is not int or block_size <= 0:
                raise ValueError(f"instance {instance_id!r} must have positive integer block_size")
            if type(bytes_per_token) is not int or bytes_per_token < 0:
                raise ValueError(f"instance {instance_id!r} has invalid bytes_per_token")
            if instance_id in instances:
                raise ValueError(f"duplicate instance_id in config: {instance_id}")
            instances[instance_id] = InstanceConfig(instance_id, group_name, block_size, bytes_per_token)

    if not instances:
        raise ValueError("config instance_groups contain no instances")
    return instances


def warn_unsupported_features(config: dict) -> None:
    warnings = []
    if config.get("mamba_state", {}).get("enabled"):
        warnings.append("mamba_state is enabled; this checker does not model Mamba state")
    for group in config.get("instance_groups", []):
        group_name = group.get("group_name", "<unknown>")
        storages = group.get("storages", [])
        if isinstance(storages, list) and len(storages) > 1:
            warnings.append(f"group {group_name} has multiple storages; tier flow is not modeled")
        ttl = group.get("ttl_config", {}) or {}
        if int(ttl.get("default_block_ttl_seconds", 0) or 0) != 0:
            warnings.append(f"group {group_name} has default TTL; TTL expiry is not modeled")
    for warning in warnings:
        print(f"Warning: {warning}", file=sys.stderr)


def iter_trace_rows(trace_path: str, replay_mode: str):
    with open(trace_path, "r") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{trace_path}:{line_no} invalid JSON: {exc}") from exc
            validate_trace_row(trace_path, line_no, obj, replay_mode)
            obj["keys"] = [_parse_optimizer_key(value, trace_path, line_no) for value in obj["keys"]]
            yield line_no, obj


def validate_trace_row(trace_path: str, line_no: int, obj: dict, replay_mode: str) -> None:
    if not isinstance(obj, dict):
        raise ValueError(f"{trace_path}:{line_no} trace row must be an object")
    trace_type = obj.get("type")
    if trace_type not in {"get", "write", "request"}:
        raise ValueError(f"{trace_path}:{line_no} has invalid type={trace_type!r}")
    if replay_mode == "request" and trace_type != "request":
        raise ValueError(f"{trace_path}:{line_no} trace_replay.mode=request only accepts type=request")
    if replay_mode == "read_write" and trace_type == "request":
        raise ValueError(f"{trace_path}:{line_no} trace_replay.mode=read_write accepts only type=get/write")
    if not isinstance(obj.get("instance_id"), str) or not obj["instance_id"]:
        raise ValueError(f"{trace_path}:{line_no} missing non-empty instance_id")
    if type(obj.get("timestamp_ns")) is not int or obj["timestamp_ns"] <= 0:
        raise ValueError(f"{trace_path}:{line_no} missing positive integer timestamp_ns")
    if not isinstance(obj.get("keys"), list):
        raise ValueError(f"{trace_path}:{line_no} missing keys array")
    if trace_type in {"get", "request"} and (type(obj.get("input_len")) is not int or obj["input_len"] <= 0):
        raise ValueError(f"{trace_path}:{line_no} {trace_type} trace must contain positive integer input_len")
    query_type = obj.get("query_type", "prefix_match")
    if query_type not in {"prefix_match", "batch_get"}:
        raise ValueError(f"{trace_path}:{line_no} unsupported query_type={query_type!r}")
    if "block_mask" in obj:
        block_mask = obj["block_mask"]
        if type(block_mask) is int:
            if block_mask < 0:
                raise ValueError(f"{trace_path}:{line_no} block_mask offset must be non-negative")
        elif isinstance(block_mask, list):
            if any(type(item) is not bool for item in block_mask):
                raise ValueError(f"{trace_path}:{line_no} block_mask vector must contain booleans")
        else:
            raise ValueError(f"{trace_path}:{line_no} block_mask must be integer offset or boolean array")


def _parse_optimizer_key(value, trace_path: str, line_no: int) -> int:
    if type(value) is not int:
        raise ValueError(f"{trace_path}:{line_no} keys must contain integers")
    if INT64_MIN <= value <= INT64_MAX:
        return value
    if INT64_MAX < value <= UINT64_MAX:
        return INT64_MIN + (value - (1 << 63))
    raise ValueError(f"{trace_path}:{line_no} key out of int64/uint64 range: {value}")


def flush_pending_writes(pending: List[Tuple[int, int, dict]],
                         runtime: Dict[str, InstanceRuntime],
                         through_timestamp_ns: Optional[int]) -> int:
    while pending and (through_timestamp_ns is None or pending[0][0] <= through_timestamp_ns):
        _, _, trace = heapq.heappop(pending)
        handle_write(trace, runtime)
    return total_cached_blocks(runtime)


def handle_read(trace: dict, runtime: Dict[str, InstanceRuntime], metric_start_ns: Optional[int]) -> int:
    instance_id = trace["instance_id"]
    if instance_id not in runtime:
        raise ValueError(f"trace references unknown instance_id={instance_id!r}")
    rt = runtime[instance_id]
    keys = trace["keys"]
    block_size = rt.config.block_size
    input_len = trace["input_len"]
    max_full_blocks = input_len // block_size
    if len(keys) > max_full_blocks:
        trace_id = trace.get("trace_id", "")
        raise ValueError(
            f"trace contains partial tail block keys: instance_id={instance_id}, trace_id={trace_id}, "
            f"keys={len(keys)}, input_len={input_len}, block_size={block_size}, max_full_blocks={max_full_blocks}"
        )

    if trace.get("query_type", "prefix_match") == "batch_get":
        hit_indices = rt.index.batch_hit_indices(keys)
    else:
        hit_indices = list(range(rt.index.prefix_match_count(keys)))

    local_hit_blocks = 0
    remote_hit_blocks = 0
    for idx in hit_indices:
        if is_index_in_block_mask(trace.get("block_mask", []), idx):
            local_hit_blocks += 1
        else:
            remote_hit_blocks += 1

    rt.all_stats.add(input_len, block_size, local_hit_blocks, remote_hit_blocks)
    if metric_start_ns is None or trace["timestamp_ns"] >= metric_start_ns:
        rt.window_stats.add(input_len, block_size, local_hit_blocks, remote_hit_blocks)

    cached_blocks_all = total_cached_blocks(runtime)
    rt.last_cached_blocks_all_at_read = cached_blocks_all
    return cached_blocks_all


def handle_write(trace: dict, runtime: Dict[str, InstanceRuntime]) -> None:
    instance_id = trace["instance_id"]
    if instance_id not in runtime:
        raise ValueError(f"trace references unknown instance_id={instance_id!r}")
    rt = runtime[instance_id]
    rt.write_count += 1
    rt.written_blocks += len(trace["keys"])
    rt.index.insert(trace["keys"])


def is_index_in_block_mask(mask, index: int) -> bool:
    if isinstance(mask, list):
        return index < len(mask) and bool(mask[index])
    if type(mask) is int:
        return index < mask
    return False


def total_cached_blocks(runtime: Dict[str, InstanceRuntime]) -> int:
    return sum(rt.index.materialized_blocks for rt in runtime.values())


def print_summary(result: dict) -> None:
    print("=" * 72)
    print("Warmup Prefix Checker")
    print("=" * 72)
    print(f"Config:       {result['config']}")
    print(f"Trace:        {result['trace']}")
    print(f"Replay mode:  {result['trace_replay_mode']}")
    print(f"Write delay:  {result['write_delay_ns']} ns")
    if result["metric_start_ns"] is not None:
        print(f"Metric start: {result['metric_start_ns']} ns")
    print(f"Trace rows:   {result['trace_rows']}")
    print(f"Max blocks:   {result['max_blocks']}  (matches tradeoff warmup max_blocks CSV basis)")
    print(f"Final blocks: {result['final_cached_blocks_all']}  (includes writes after last read)")
    print()
    print("{:<28} {:>9} {:>14} {:>14} {:>10} {:>10} {:>10}".format(
        "Instance", "Reads", "InputTokens", "HitTokens", "Total", "Local", "Remote"))
    print("-" * 105)
    for iid, metrics in sorted(result["instances"].items()):
        print("{:<28} {:>9} {:>14} {:>14} {:>10.6f} {:>10.6f} {:>10.6f}".format(
            iid,
            metrics["read_count"],
            metrics["input_tokens"],
            metrics["hit_tokens"],
            metrics["total"],
            metrics["local"],
            metrics["remote"],
        ))


def compare_warmup_json(result: dict, warmup_json_path: str, policy_name: str) -> None:
    with open(warmup_json_path, "r") as f:
        payload = json.load(f)
    policy = payload.get("policies", {}).get(policy_name)
    if not policy:
        raise ValueError(f"{warmup_json_path} missing policy {policy_name!r}")
    expected_instances = policy.get("instances", {})
    print()
    print("=" * 72)
    print(f"Compare With Warmup JSON: {warmup_json_path} [{policy_name}]")
    print("=" * 72)
    expected_max = policy.get("max_blocks")
    max_delta = None if expected_max is None else result["max_blocks"] - int(expected_max)
    print(f"max_blocks: computed={result['max_blocks']} warmup={expected_max} delta={max_delta}")
    print()
    print("{:<28} {:>12} {:>12} {:>12}".format("Instance", "DeltaTotal", "DeltaLocal", "DeltaRemote"))
    print("-" * 72)
    for iid, computed in sorted(result["instances"].items()):
        expected = expected_instances.get(iid)
        if expected is None:
            print("{:<28} {:>12} {:>12} {:>12}".format(iid, "missing", "missing", "missing"))
            continue
        print("{:<28} {:>12.9f} {:>12.9f} {:>12.9f}".format(
            iid,
            computed["total"] - float(expected.get("total", 0.0)),
            computed["local"] - float(expected.get("local", 0.0)),
            computed["remote"] - float(expected.get("remote", 0.0)),
        ))


if __name__ == "__main__":
    main()
