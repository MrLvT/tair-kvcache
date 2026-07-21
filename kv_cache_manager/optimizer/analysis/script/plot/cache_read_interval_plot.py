#!/usr/bin/env python3
"""Plot minute-level consecutive cache read-hit interval summaries."""

import argparse
import csv
import os
from datetime import datetime, timedelta, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


SHANGHAI = timezone(timedelta(hours=8))


def _optional_float(value):
    return float(value) if value not in (None, "") else float("nan")


def plot_csv(csv_path, output_path=None, minute_interval=None, display_label=None):
    with open(csv_path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("empty cache read interval CSV: %s" % csv_path)

    times = [datetime.fromtimestamp(int(row["MinuteStartNs"]) / 1e9, tz=SHANGHAI) for row in rows]
    metrics = [
        ("average", "ReadIntervalAverageSeconds"),
        ("p50", "ReadIntervalP50Seconds"),
        ("p95", "ReadIntervalP95Seconds"),
    ]

    fig, axis = plt.subplots(figsize=(16, 5.5), constrained_layout=True)
    for label, column in metrics:
        axis.plot(
            times,
            [_optional_float(row[column]) for row in rows],
            label=label,
            linewidth=1.4,
            marker=".",
        )
    axis.set_title("Consecutive cache read-hit interval")
    axis.set_ylabel("seconds")
    axis.set_xlabel("Time (Asia/Shanghai)")
    axis.grid(alpha=0.25)
    axis.legend(ncol=3)
    if minute_interval is None and len(rows) > 360:
        axis.xaxis.set_major_locator(mdates.HourLocator(interval=1, tz=SHANGHAI))
    else:
        if minute_interval is None:
            minute_interval = 1 if len(rows) <= 15 else 10 if len(rows) <= 120 else 30
        axis.xaxis.set_major_locator(mdates.MinuteLocator(interval=minute_interval, tz=SHANGHAI))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=SHANGHAI))

    instance = os.path.basename(csv_path).removesuffix("_cache_read_interval_by_minute.csv")
    title = "Cache read interval timeline — %s" % instance
    if display_label:
        title += " — %s" % display_label
    fig.suptitle(title)
    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(csv_path),
            "%s_cache_read_interval_timeline.png" % instance,
        )
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", nargs="+", help="*_cache_read_interval_by_minute.csv")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--minute-interval", type=int, default=None)
    parser.add_argument("--label", default=None)
    args = parser.parse_args()
    if args.minute_interval is not None and args.minute_interval <= 0:
        parser.error("--minute-interval must be positive")
    for csv_path in args.csv:
        output_path = None
        if args.output_dir:
            os.makedirs(args.output_dir, exist_ok=True)
            output_path = os.path.join(
                args.output_dir,
                os.path.basename(csv_path).replace("_by_minute.csv", "_timeline.png"),
            )
        print(plot_csv(csv_path, output_path, args.minute_interval, args.label))


if __name__ == "__main__":
    main()
