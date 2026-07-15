#!/usr/bin/env python3
"""Stream DeepSeek enriched JSONL as standard optimizer JSONL to stdout."""

import argparse
import io
import json
import sys
from contextlib import contextmanager
from urllib.parse import urlparse
from urllib.request import urlopen

from prepare_deepseek_trace import _convert_row


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
        description="Convert DeepSeek enriched JSONL inputs to optimizer JSONL on stdout")
    parser.add_argument("--input", action="append", required=True)
    parser.add_argument("--block-size", type=int, default=256)
    _add_bool_arg(parser, "--prefix-hash", default=True)
    parser.add_argument("--trace-mode", choices=["get-write", "request"], default="get-write")
    parser.add_argument("--instance-id-override", default=None,
                        help="Use one optimizer instance_id for all emitted traces")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--progress-rows", type=int, default=100000)
    args = parser.parse_args()

    converted = 0
    skipped = 0
    emitted = 0
    for input_ref in args.input:
        with _open_text_input(input_ref) as f:
            for line_no, line in enumerate(f, 1):
                if args.limit is not None and converted >= args.limit:
                    print(
                        f"streamed rows={converted} traces={emitted} skipped={skipped}",
                        file=sys.stderr,
                        flush=True,
                    )
                    return
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    traces, _ = _convert_row(row, args.block_size, args.prefix_hash, args.trace_mode)
                    if args.instance_id_override:
                        for trace in traces:
                            trace["instance_id"] = args.instance_id_override
                except Exception as exc:
                    skipped += 1
                    print(f"Warning: skipped {input_ref}:{line_no}: {exc}", file=sys.stderr)
                    continue
                converted += 1
                for trace in traces:
                    sys.stdout.write(json.dumps(trace, separators=(",", ":")))
                    sys.stdout.write("\n")
                    emitted += 1
                if args.progress_rows > 0 and converted % args.progress_rows == 0:
                    print(
                        f"streamed rows={converted} traces={emitted} skipped={skipped}",
                        file=sys.stderr,
                        flush=True,
                    )
    print(f"streamed rows={converted} traces={emitted} skipped={skipped}", file=sys.stderr, flush=True)


def _add_bool_arg(parser: argparse.ArgumentParser, name: str, default: bool):
    if hasattr(argparse, "BooleanOptionalAction"):
        parser.add_argument(name, action=argparse.BooleanOptionalAction, default=default)
        return
    dest = name.lstrip("-").replace("-", "_")
    parser.add_argument(name, dest=dest, action="store_true")
    parser.add_argument(f"--no-{name.lstrip('-')}", dest=dest, action="store_false")
    parser.set_defaults(**{dest: default})


if __name__ == "__main__":
    main()
