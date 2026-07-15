#!/usr/bin/env python3
"""Split DeepSeek enriched JSONL inputs into per-service shards."""

import argparse
import json
import shutil
import sys
from pathlib import Path

from prepare_deepseek_trace import _safe_name


def _source_shard_name(path):
    name = path.name
    marker = "_blksz_256."
    if marker in name:
        suffix = name.split(marker, 1)[1]
        if suffix.endswith(".enriched.jsonl"):
            return suffix[:-len(".enriched.jsonl")] + ".jsonl"
    return path.stem + ".jsonl"


def main():
    parser = argparse.ArgumentParser(
        description="Split DeepSeek enriched JSONL files into per-service JSONL shards")
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--progress-rows", type=int, default=100000)
    parser.add_argument("--force", action="store_true",
                        help="Remove existing output dir before splitting")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    manifest_path = output_dir / "services_manifest.json"
    if manifest_path.exists() and not args.force:
        print(f"Manifest exists, skip split: {manifest_path}")
        return

    if output_dir.exists() and args.force:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    service_dirs = {}
    service_stats = {}
    source_rows = 0
    skipped_rows = 0

    def writer_for(service, input_path, writers):
        service_dir = service_dirs.get(service)
        if service_dir is None:
            safe = _safe_name(service)
            service_dir = output_dir / safe
            service_dir.mkdir(parents=True, exist_ok=True)
            service_dirs[service] = service_dir
            service_stats[service] = {
                "safe_name": safe,
                "rows": 0,
                "instances": set(),
                "shards": set(),
            }
            with open(service_dir / "service_name.txt", "w", encoding="utf-8") as f:
                f.write(service)
                f.write("\n")
        shard = _source_shard_name(input_path)
        key = (service, shard)
        if key not in writers:
            path = service_dir / shard
            writers[key] = open(path, "w", encoding="utf-8")
        return writers[key], shard

    for input_path in [Path(p) for p in args.input]:
        writers = {}
        try:
            with open(input_path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    source_rows += 1
                    try:
                        row = json.loads(line)
                        services = [str(s) for s in (row.get("service_names") or []) if str(s)]
                        if not services:
                            raise ValueError("missing service_names")
                        service = services[0]
                        pods = [str(p) for p in (row.get("pods") or []) if str(p)]
                        if not pods:
                            raise ValueError("missing pods")
                    except Exception as exc:
                        skipped_rows += 1
                        print(f"Warning: skipped split {input_path}:{line_no}: {exc}", file=sys.stderr)
                        continue

                    out, shard = writer_for(service, input_path, writers)
                    out.write(line)
                    if not line.endswith("\n"):
                        out.write("\n")
                    stats = service_stats[service]
                    stats["rows"] += 1
                    stats["instances"].update(pods)
                    stats["shards"].add(shard)

                    if args.progress_rows > 0 and source_rows % args.progress_rows == 0:
                        print(f"Split {source_rows} rows...", flush=True)
        finally:
            for f in writers.values():
                f.close()

    services_manifest = {}
    for service, stats in sorted(service_stats.items()):
        service_dir = service_dirs[service]
        shards = sorted(stats["shards"])
        services_manifest[service] = {
            "safe_name": stats["safe_name"],
            "dir": str(service_dir),
            "rows": stats["rows"],
            "instances": sorted(stats["instances"]),
            "shards": [str(service_dir / shard) for shard in shards],
        }

    manifest = {
        "source_rows": source_rows,
        "skipped_rows": skipped_rows,
        "services": services_manifest,
        "inputs": [str(Path(p)) for p in args.input],
    }
    tmp_path = manifest_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp_path.replace(manifest_path)

    print(f"Rows split: {source_rows}")
    print(f"Rows skipped: {skipped_rows}")
    print(f"Services: {len(services_manifest)}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
