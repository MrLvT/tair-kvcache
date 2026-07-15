#!/usr/bin/env python3
"""Prepare config/metadata for streaming DeepSeek optimizer replay."""

import argparse
import io
import json
import sys
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse
from urllib.request import urlopen

from prepare_deepseek_trace import (
    _build_config,
    _compute_time_config,
    _config_path,
    _write_service_mapping,
)


def _is_url(input_ref: str) -> bool:
    return urlparse(input_ref).scheme in ("http", "https")


@contextmanager
def _open_text_input(input_ref: str):
    if _is_url(input_ref):
        with urlopen(input_ref) as response:
            yield io.TextIOWrapper(response, encoding="utf-8")
    else:
        with open(input_ref, "r", encoding="utf-8") as f:
            yield f


def main():
    parser = argparse.ArgumentParser(
        description="Scan DeepSeek enriched JSONL inputs and write optimizer config for streaming replay")
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--name", default="deepseek_stream")
    parser.add_argument("--block-size", type=int, default=256)
    parser.add_argument("--bytes-per-token", type=int, default=92773)
    parser.add_argument("--quota-capacity", type=float, default=128.0)
    parser.add_argument("--eviction-policy", default="lru")
    parser.add_argument("--eviction-mode", type=int, default=3)
    parser.add_argument("--eviction-batch-size-per-instance", type=int, default=100)
    parser.add_argument("--grouping", choices=["per-instance", "single-group"], default="per-instance")
    parser.add_argument("--instance-id-override", default=None,
                        help="Use one optimizer instance_id for all rows, e.g. service-level pooled cache")
    parser.add_argument("--config-path-root", default=None)
    parser.add_argument("--output-result-subdir", default="pareto_stream")
    parser.add_argument("--progress-rows", type=int, default=100000)
    parser.add_argument("--compute-preset", choices=["none", "qwen36-short-p50"], default="none",
                        help="Convenience preset for trace_replay compute_time; defaults to disabled")
    parser.add_argument("--compute-base-latency-ns", type=int, default=None)
    parser.add_argument("--compute-miss-block-latency-ns", type=int, default=0)
    parser.add_argument("--compute-miss-block-position-latency-ns", type=int, default=0)
    parser.add_argument("--compute-latency-offset-ns", type=int, default=0)
    parser.add_argument("--compute-scheduler-load-divisor", type=float, default=1.0)
    parser.add_argument("--compute-scheduler-lane-count", type=int, default=1)
    parser.add_argument("--compute-scheduler-lane-count-alternate", type=int, default=0)
    parser.add_argument("--compute-noise-offsets-ns", default="")
    parser.add_argument("--compute-noise-weights", default="")
    parser.add_argument("--compute-noise-seed", type=int, default=0)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    instances = set()
    services = set()
    pod_to_service = {}
    source_rows = 0
    skipped_rows = 0
    multi_service_rows = 0
    multi_pod_rows = 0
    pod_service_conflicts = 0

    for input_ref in args.input:
        with _open_text_input(input_ref) as f:
            for line_no, line in enumerate(f, 1):
                if not line.strip():
                    continue
                source_rows += 1
                try:
                    row = json.loads(line)
                    pods = [str(p) for p in (row.get("pods") or []) if str(p)]
                    if not pods:
                        raise ValueError("missing pods")
                    services_in_row = [str(s) for s in (row.get("service_names") or []) if str(s)]
                    service = services_in_row[0] if services_in_row else ""
                    instance_id = args.instance_id_override or pods[0]
                except Exception as exc:
                    skipped_rows += 1
                    print(f"Warning: skipped metadata {input_ref}:{line_no}: {exc}", file=sys.stderr)
                    continue

                instances.add(instance_id)
                if service:
                    services.add(service)
                    for pod in pods:
                        previous = pod_to_service.get(pod)
                        if previous is None:
                            pod_to_service[pod] = service
                        elif previous != service:
                            pod_service_conflicts += 1
                if len(services_in_row) != 1:
                    multi_service_rows += 1
                if len(pods) != 1:
                    multi_pod_rows += 1

                if args.progress_rows > 0 and source_rows % args.progress_rows == 0:
                    print(f"Scanned {source_rows} rows...", flush=True)

    _write_service_mapping(output_dir / "pod_service_mapping.csv", pod_to_service)
    output_result_path = output_dir / args.output_result_subdir
    output_result_path.mkdir(parents=True, exist_ok=True)

    config_path = output_dir / f"{args.name}_config.json"
    stream_trace_path = output_dir / f"{args.name}.stream.fifo"
    config = _build_config(
        trace_path=_config_path(stream_trace_path, args.config_path_root),
        output_result_path=_config_path(output_result_path, args.config_path_root),
        instances=sorted(instances),
        name=args.name,
        grouping=args.grouping,
        quota_capacity=args.quota_capacity,
        block_size=args.block_size,
        bytes_per_token=args.bytes_per_token,
        eviction_policy=args.eviction_policy,
        eviction_mode=args.eviction_mode,
        eviction_batch_size=args.eviction_batch_size_per_instance,
        compute_time_config=_compute_time_config(args, services),
    )
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, sort_keys=False)
        f.write("\n")

    manifest = {
        "source_rows": source_rows,
        "skipped_rows": skipped_rows,
        "instances": len(instances),
        "services": len(services),
        "pod_service_pairs": len(pod_to_service),
        "multi_service_rows": multi_service_rows,
        "multi_pod_rows": multi_pod_rows,
        "pod_service_conflicts": pod_service_conflicts,
        "config": str(config_path),
        "stream_trace_path": str(stream_trace_path),
        "grouping": args.grouping,
        "instance_id_override": args.instance_id_override,
        "prefix_hash": True,
        "inputs": [str(p) for p in args.input],
        "compute_time_enabled": args.compute_base_latency_ns is not None,
    }
    with open(output_dir / "prepare_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"Rows scanned: {source_rows}")
    print(f"Instances: {len(instances)}")
    print(f"Services: {len(services)}")
    print(f"Pod/service conflicts: {pod_service_conflicts}")
    print(f"Config: {config_path}")


if __name__ == "__main__":
    main()
