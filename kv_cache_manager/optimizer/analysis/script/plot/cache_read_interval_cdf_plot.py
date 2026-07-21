#!/usr/bin/env python3
"""Plot a linear-axis CDF from cache read-interval histogram output."""

import argparse
import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter


def plot_csv(csv_path, output_path=None, max_seconds=5000):
    with open(csv_path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("empty cache read interval histogram CSV: %s" % csv_path)

    seconds = [int(row["IntervalUpperSeconds"]) for row in rows]
    samples = [int(row["IntervalSamples"]) for row in rows]
    total = sum(samples)
    if total <= 0:
        raise ValueError("cache read interval histogram has no samples: %s" % csv_path)

    cumulative = []
    running = 0
    for count in samples:
        running += count
        cumulative.append(running / total)

    plot_seconds = list(seconds)
    plot_cumulative = list(cumulative)
    if plot_seconds[-1] < max_seconds:
        plot_seconds.append(max_seconds)
        plot_cumulative.append(plot_cumulative[-1])

    fig, (full_axis, tail_axis) = plt.subplots(
        2,
        1,
        figsize=(16, 8),
        sharex=True,
        constrained_layout=True,
    )
    for axis in (full_axis, tail_axis):
        axis.step(plot_seconds, plot_cumulative, where="post", linewidth=1.6, color="tab:blue")
        axis.set_xlim(0, max_seconds)
        axis.set_ylabel("cumulative interval samples")
        axis.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
        axis.grid(alpha=0.25)
    full_axis.set_ylim(0, 1.005)
    full_axis.set_title("Full CDF")
    tail_axis.set_ylim(0.90, 1.0005)
    tail_axis.set_title("Tail detail (90%–100%)")
    tail_axis.set_xlabel("Consecutive read-hit interval upper bound (seconds, linear scale)")

    marker_seconds = list(range(1000, max_seconds + 1, 1000))
    marker_cdf = []
    row_index = 0
    for threshold in marker_seconds:
        while row_index + 1 < len(seconds) and seconds[row_index + 1] <= threshold:
            row_index += 1
        value = cumulative[row_index] if seconds[row_index] <= threshold else 0.0
        marker_cdf.append(value)
    tail_axis.scatter(marker_seconds, marker_cdf, color="tab:orange", zorder=3)
    for threshold, value in zip(marker_seconds, marker_cdf):
        tail_axis.annotate(
            "%.2f%%" % (value * 100),
            (threshold, value),
            xytext=(0, 7),
            textcoords="offset points",
            ha="center",
            fontsize=9,
        )

    instance = os.path.basename(csv_path).removesuffix("_cache_read_interval_histogram.csv")
    fig.suptitle("Cache read interval CDF — %s — %s samples" % (instance, f"{total:,}"))
    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(csv_path),
            "%s_cache_read_interval_cdf.png" % instance,
        )
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="*_cache_read_interval_histogram.csv")
    parser.add_argument("--output", default=None)
    parser.add_argument("--max-seconds", type=int, default=5000)
    args = parser.parse_args()
    if args.max_seconds <= 0:
        parser.error("--max-seconds must be positive")
    print(plot_csv(args.csv, args.output, args.max_seconds))


if __name__ == "__main__":
    main()
