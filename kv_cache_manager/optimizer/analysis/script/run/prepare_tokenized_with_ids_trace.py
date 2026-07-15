#!/usr/bin/env python3
"""Convert tokenized-with-ids JSONL traces to optimizer request traces."""

import argparse
import json
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


UINT64_MASK = 0xFFFFFFFFFFFFFFFF
INT64_SIGN = 0x8000000000000000
UINT64_MOD = 0x10000000000000000
GOLDEN_RATIO = 0x9E3779B97F4A7C15


def main():
    parser = argparse.ArgumentParser(
        description="Convert JSONL rows with request_time/n_tokens/token_ids to optimizer request traces")
    parser.add_argument("--input", action="append", required=True,
                        help="Input JSONL or JSONL.zst. Can be passed multiple times")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--name", default="tokenized_with_ids")
    parser.add_argument("--instance-id", default=None,
                        help="Single optimizer instance_id. Defaults to sanitized --name")
    parser.add_argument("--block-size", type=int, default=256)
    parser.add_argument("--bytes-per-token", type=int, default=1,
                        help="Only used for GB display in analysis scripts")
    parser.add_argument("--quota-capacity", type=float, default=1.0,
                        help="Placeholder capacity in GB; warmup-only overrides it to unlimited")
    parser.add_argument("--eviction-policy", default="lru")
    parser.add_argument("--eviction-mode", type=int, default=3)
    parser.add_argument("--eviction-batch-size-per-instance", type=int, default=100)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--progress-rows", type=int, default=100000)
    parser.add_argument("--sort", action="store_true",
                        help="Sort emitted traces by timestamp_ns with external sort")
    parser.add_argument("--keep-unsorted", action="store_true",
                        help="Keep intermediate unsorted JSONL when --sort is used")
    args = parser.parse_args()

    if args.block_size <= 0:
        raise SystemExit("--block-size must be positive")
    if args.bytes_per_token <= 0:
        raise SystemExit("--bytes-per-token must be positive")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    instance_id = args.instance_id or _safe_name(args.name)

    unsorted_trace = output_dir / f"{args.name}.optimizer.unsorted.jsonl"
    final_trace = output_dir / (
        f"{args.name}.sorted.optimizer.jsonl" if args.sort else f"{args.name}.optimizer.jsonl"
    )
    stats = _convert_inputs(
        input_refs=args.input,
        output_path=unsorted_trace if args.sort else final_trace,
        block_size=args.block_size,
        instance_id=instance_id,
        limit=args.limit,
        progress_rows=args.progress_rows,
    )

    if args.sort:
        _sort_optimizer_trace(unsorted_trace, final_trace)
        if not args.keep_unsorted:
            _unlink_if_exists(unsorted_trace)

    output_result_path = output_dir / "warmup_run"
    output_result_path.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / f"{args.name}_config.json"
    config = _build_optimizer_config(
        trace_path=final_trace,
        output_result_path=output_result_path,
        name=args.name,
        instance_id=instance_id,
        quota_capacity=args.quota_capacity,
        block_size=args.block_size,
        bytes_per_token=args.bytes_per_token,
        eviction_policy=args.eviction_policy,
        eviction_mode=args.eviction_mode,
        eviction_batch_size=args.eviction_batch_size_per_instance,
    )
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, sort_keys=False)
        f.write("\n")

    manifest = dict(stats)
    manifest.update({
        "block_size": args.block_size,
        "bytes_per_token": args.bytes_per_token,
        "instance_id": instance_id,
        "optimizer_trace": str(final_trace),
        "config": str(config_path),
        "trace_replay_mode": "request",
        "inputs": list(args.input),
    })
    with open(output_dir / "prepare_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Rows converted: {stats['rows_converted']}")
    print(f"Rows skipped: {stats['skipped_rows']}")
    print(f"Optimizer traces: {stats['optimizer_traces']}")
    print(f"Out-of-order rows: {stats['out_of_order_rows']}")
    print(f"Max full blocks: {stats['max_full_blocks']}")
    print(f"Trace: {final_trace}")
    print(f"Config: {config_path}")


@contextmanager
def _open_text_input(input_ref: str):
    if input_ref.endswith(".zst"):
        proc = subprocess.Popen(
            ["zstd", "-dc", input_ref],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        assert proc.stdout is not None
        try:
            yield proc.stdout
        finally:
            if proc.poll() is None:
                proc.terminate()
            ret = proc.wait()
            # A negative return code is expected when a limited sample stops early.
            if ret not in (0, -15):
                raise subprocess.CalledProcessError(ret, ["zstd", "-dc", input_ref])
        return
    with open(input_ref, "r", encoding="utf-8") as f:
        yield f


def _convert_inputs(
    input_refs: Iterable[str],
    output_path: Path,
    block_size: int,
    instance_id: str,
    limit: Optional[int],
    progress_rows: int,
) -> Dict:
    stats = {
        "source_rows": 0,
        "rows_converted": 0,
        "optimizer_traces": 0,
        "skipped_rows": 0,
        "short_tail_rows": 0,
        "empty_block_rows": 0,
        "out_of_order_rows": 0,
        "min_input_len": None,
        "max_input_len": 0,
        "max_full_blocks": 0,
        "min_timestamp_ns": None,
        "max_timestamp_ns": 0,
        "abtests": {},
    }
    abtests = {}
    last_timestamp_ns = None

    with open(output_path, "w", encoding="utf-8") as out:
        for input_ref in input_refs:
            with _open_text_input(input_ref) as f:
                for line_no, line in enumerate(f, 1):
                    if limit is not None and stats["rows_converted"] >= limit:
                        stats["abtests"] = abtests
                        return stats
                    if not line.strip():
                        continue
                    stats["source_rows"] += 1
                    try:
                        row = json.loads(line)
                        trace, meta = _convert_row(row, block_size, instance_id)
                    except Exception as exc:
                        stats["skipped_rows"] += 1
                        print(f"Warning: skipped {input_ref}:{line_no}: {exc}", file=sys.stderr)
                        continue

                    timestamp_ns = trace["timestamp_ns"]
                    if last_timestamp_ns is not None and timestamp_ns < last_timestamp_ns:
                        stats["out_of_order_rows"] += 1
                    last_timestamp_ns = timestamp_ns

                    out.write(json.dumps(trace, separators=(",", ":")))
                    out.write("\n")

                    stats["rows_converted"] += 1
                    stats["optimizer_traces"] += 1
                    if meta["tail_tokens"]:
                        stats["short_tail_rows"] += 1
                    if meta["full_blocks"] == 0:
                        stats["empty_block_rows"] += 1
                    input_len = meta["input_len"]
                    stats["min_input_len"] = (
                        input_len if stats["min_input_len"] is None else min(stats["min_input_len"], input_len)
                    )
                    stats["max_input_len"] = max(stats["max_input_len"], input_len)
                    stats["max_full_blocks"] = max(stats["max_full_blocks"], meta["full_blocks"])
                    stats["min_timestamp_ns"] = (
                        timestamp_ns if stats["min_timestamp_ns"] is None else min(stats["min_timestamp_ns"], timestamp_ns)
                    )
                    stats["max_timestamp_ns"] = max(stats["max_timestamp_ns"], timestamp_ns)
                    abtest = meta.get("abtest") or ""
                    if abtest:
                        abtests[abtest] = abtests.get(abtest, 0) + 1

                    if progress_rows > 0 and stats["rows_converted"] % progress_rows == 0:
                        print(f"Converted {stats['rows_converted']} rows...", flush=True)

    stats["abtests"] = abtests
    return stats


def _convert_row(row: Dict, block_size: int, instance_id: str):
    request_id = str(row.get("request_id") or "")
    if not request_id:
        raise ValueError("missing request_id")

    request_time = str(row.get("request_time") or "")
    if not request_time:
        raise ValueError("missing request_time")
    timestamp_ns = _parse_request_time_ns(request_time)

    token_ids = row.get("token_ids")
    if not isinstance(token_ids, list):
        raise ValueError("token_ids must be a list")
    n_tokens = int(row.get("n_tokens") or 0)
    if n_tokens <= 0:
        raise ValueError("n_tokens must be positive")
    if n_tokens != len(token_ids):
        raise ValueError(f"n_tokens {n_tokens} != len(token_ids) {len(token_ids)}")

    full_blocks = n_tokens // block_size
    usable_tokens = full_blocks * block_size
    block_keys = tokens_to_block_ids(token_ids[:usable_tokens], block_size)
    trace = {
        "type": "request",
        "instance_id": instance_id,
        "trace_id": f"trace_{request_id}",
        "timestamp_ns": timestamp_ns,
        "keys": block_keys,
        "input_len": n_tokens,
        "query_type": "prefix_match",
        "block_mask": [],
        "sw_size": 0,
        "location_spec_names": [],
    }
    return trace, {
        "input_len": n_tokens,
        "full_blocks": full_blocks,
        "tail_tokens": n_tokens - usable_tokens,
        "abtest": row.get("abtest"),
    }


def _parse_request_time_ns(value: str) -> int:
    normalized = value.replace(",", ".")
    dt = datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S.%f")
    timestamp_ns = int(dt.timestamp() * 1_000_000_000)
    if timestamp_ns <= 0:
        raise ValueError(f"invalid request_time {value}")
    return timestamp_ns


def hash_int64_func(prev_hash: int, current_value: int) -> int:
    hash_unsigned = prev_hash & UINT64_MASK
    value_unsigned = int(current_value) & UINT64_MASK
    left_shift = (hash_unsigned << 12) & UINT64_MASK
    right_shift = hash_unsigned >> 32
    rhs = (value_unsigned + GOLDEN_RATIO + left_shift + right_shift) & UINT64_MASK
    result = (hash_unsigned ^ rhs) & UINT64_MASK
    if result >= INT64_SIGN:
        result -= UINT64_MOD
    return result


def tokens_to_block_ids(token_ids: List[int], block_size: int) -> List[int]:
    block_ids = []
    prev_hash = 0
    for i in range(0, len(token_ids), block_size):
        block_tokens = token_ids[i:i + block_size]
        if len(block_tokens) < block_size:
            break
        block_hash = prev_hash
        for token in block_tokens:
            block_hash = hash_int64_func(block_hash, token)
        block_ids.append(block_hash)
        prev_hash = block_hash
    return block_ids


def _sort_optimizer_trace(input_path: Path, output_path: Path):
    temp_tsv = input_path.with_suffix(input_path.suffix + ".sort.tsv")
    with open(input_path, "r", encoding="utf-8") as src, open(temp_tsv, "w", encoding="utf-8") as tsv:
        for seq, line in enumerate(src):
            row = json.loads(line)
            tsv.write(f"{row['timestamp_ns']}\t{seq}\t{line}")
    cmd = ["sort", "-s", "-t", "\t", "-k1,1n", "-k2,2n", str(temp_tsv)]
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    with open(output_path, "w", encoding="utf-8") as out:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True, env=env)
        assert proc.stdout is not None
        for line in proc.stdout:
            out.write(line.split("\t", 2)[2])
        ret = proc.wait()
    _unlink_if_exists(temp_tsv)
    if ret != 0:
        raise subprocess.CalledProcessError(ret, cmd)


def _build_optimizer_config(
    trace_path: Path,
    output_result_path: Path,
    name: str,
    instance_id: str,
    quota_capacity: float,
    block_size: int,
    bytes_per_token: int,
    eviction_policy: str,
    eviction_mode: int,
    eviction_batch_size: int,
) -> Dict:
    return {
        "trace_file_path": str(trace_path.resolve()),
        "output_result_path": str(output_result_path.resolve()),
        "eviction_params": {
            "eviction_mode": eviction_mode,
            "eviction_batch_size_per_instance": eviction_batch_size,
        },
        "trace_replay": {
            "mode": "request",
            "write_delay_ns": 1,
        },
        "instance_groups": [
            {
                "group_name": _safe_name(name),
                "quota_capacity": quota_capacity,
                "used_percentage": 1.0,
                "default_block_ttl_seconds": 0,
                "ttl_refresh_on_read": True,
                "storages": [
                    {
                        "unique_name": "shared_00",
                        "storage_type": "memory",
                        "band_width_mbps": 20000,
                        "capacity": quota_capacity,
                    }
                ],
                "instances": [
                    {
                        "instance_id": instance_id,
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
                ],
            }
        ],
    }


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_") or "instance"


def _unlink_if_exists(path: Path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    main()
