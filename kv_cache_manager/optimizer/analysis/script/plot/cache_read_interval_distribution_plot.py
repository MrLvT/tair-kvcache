#!/usr/bin/env python3
"""Plot exact one-second cache read-interval counts and their cliff curve."""

import argparse
import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def plot_csv(csv_path, output_path=None):
    with open(csv_path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("empty cache read interval histogram CSV: %s" % csv_path)

    seconds = [int(row["IntervalUpperSeconds"]) for row in rows]
    samples = [int(row["IntervalSamples"]) for row in rows]
    at_or_above = [int(row["SamplesAtOrAboveBucket"]) for row in rows]
    positive = [index for index, value in enumerate(seconds) if value > 0]
    if not positive:
        raise ValueError("cache read interval histogram has no positive interval: %s" % csv_path)

    x = [seconds[index] for index in positive]
    counts = [samples[index] for index in positive]
    cliff = [at_or_above[index] for index in positive]
    total = sum(samples)

    fig, (hist_axis, cliff_axis) = plt.subplots(
        2,
        1,
        figsize=(16, 8),
        sharex=True,
        constrained_layout=True,
    )
    hist_axis.step(x, counts, where="post", linewidth=1.1, color="tab:blue")
    hist_axis.set_xscale("log")
    hist_axis.set_yscale("log")
    hist_axis.set_ylabel("interval samples")
    hist_axis.set_title("Histogram — exact 1-second upper-bound buckets")
    hist_axis.grid(alpha=0.25, which="both")

    cliff_axis.step(x, cliff, where="post", linewidth=1.4, color="tab:orange")
    cliff_axis.set_xscale("log")
    cliff_axis.set_yscale("log")
    cliff_axis.set_xlabel("Consecutive read-hit interval upper bound (seconds, log scale)")
    cliff_axis.set_ylabel("samples at or above bucket")
    cliff_axis.set_title("Cliff — reverse cumulative interval count")
    cliff_axis.grid(alpha=0.25, which="both")

    instance = os.path.basename(csv_path).removesuffix("_cache_read_interval_histogram.csv")
    fig.suptitle(
        "Cache read interval distribution — %s — %s samples" % (instance, f"{total:,}"),
        fontsize=14,
    )
    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(csv_path),
            "%s_cache_read_interval_distribution.png" % instance,
        )
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="*_cache_read_interval_histogram.csv")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    print(plot_csv(args.csv, args.output))


if __name__ == "__main__":
    main()
