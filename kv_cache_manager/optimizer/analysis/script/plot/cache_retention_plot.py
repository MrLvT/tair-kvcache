#!/usr/bin/env python3
"""Plot minute-level cache retention summaries already aggregated by service."""

import argparse
import csv
import os
from datetime import datetime, timezone, timedelta

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


SHANGHAI = timezone(timedelta(hours=8))


def _optional_float(value):
    return float(value) if value not in (None, "") else float("nan")


def _load(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows.append(row)
    return rows


def plot_csv(csv_path, output_path=None, minute_interval=None):
    rows = _load(csv_path)
    if not rows:
        raise ValueError("empty cache retention CSV: %s" % csv_path)

    times = [datetime.fromtimestamp(int(row["MinuteStartNs"]) / 1e9, tz=SHANGHAI) for row in rows]
    series = [
        (
            "Lifetime",
            [
                ("average", "LifetimeAverageSeconds"),
                ("p10", "LifetimeP10Seconds"),
                ("p50", "LifetimeP50Seconds"),
                ("p95", "LifetimeP95Seconds"),
            ],
        ),
        (
            "Idle after last valid read hit",
            [
                ("average", "IdleAfterReuseAverageSeconds"),
                ("p10", "IdleAfterReuseP10Seconds"),
                ("p50", "IdleAfterReuseP50Seconds"),
                ("p95", "IdleAfterReuseP95Seconds"),
            ],
        ),
    ]

    fig, axes = plt.subplots(2, 1, figsize=(16, 9), sharex=True, constrained_layout=True)
    for axis, (title, metrics) in zip(axes, series):
        for label, column in metrics:
            axis.plot(
                times,
                [_optional_float(row[column]) for row in rows],
                label=label,
                linewidth=1.4,
                marker=".",
            )
        if title == "Idle after last valid read hit" and "LruTimeSpanSeconds" in rows[0]:
            axis.plot(
                times,
                [_optional_float(row["LruTimeSpanSeconds"]) for row in rows],
                label="LRU time span (minute-end snapshot)",
                linewidth=1.8,
                linestyle="--",
                color="black",
            )
        axis.set_title(title)
        axis.set_ylabel("seconds")
        axis.grid(alpha=0.25)
        axis.legend(ncol=4)
    axes[-1].set_xlabel("Time (Asia/Shanghai)")
    if minute_interval is None:
        minute_interval = (
            1
            if len(rows) <= 15
            else 10
            if len(rows) <= 120
            else 30
            if len(rows) <= 360
            else 60
        )
    if minute_interval % 60 == 0:
        axes[-1].xaxis.set_major_locator(
            mdates.HourLocator(interval=minute_interval // 60, tz=SHANGHAI)
        )
    else:
        axes[-1].xaxis.set_major_locator(
            mdates.MinuteLocator(interval=minute_interval, tz=SHANGHAI)
        )
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=SHANGHAI))

    service = os.path.basename(csv_path)
    if service.startswith("service_"):
        service = service[len("service_") :]
    service = service.removesuffix("_cache_retention_by_minute.csv")
    fig.suptitle("Cache retention timeline — %s" % service)

    if output_path is None:
        output_path = os.path.join(os.path.dirname(csv_path), "service_%s_cache_retention_timeline.png" % service)
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", nargs="+", help="service_*_cache_retention_by_minute.csv")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument(
        "--minute-interval",
        type=int,
        default=None,
        help="fixed x-axis major tick interval in minutes (default: automatic)",
    )
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
        print(plot_csv(csv_path, output_path, args.minute_interval))


if __name__ == "__main__":
    main()
