"""
Reads benchmark_results.csv and plots:
  - p50, p90, p95, p99 latency: ClickHouse vs Cache Hit (per size)
  - Min latency: ClickHouse vs Cache Hit (per size)

Usage:
    python plot_benchmark.py
"""

import csv
from collections import defaultdict

import matplotlib.pyplot as plt


def load_csv(path: str = "benchmark_results.csv") -> list[dict[str, str]]:
    with open(path) as f:
        return list(csv.DictReader(f))


def percentile(values: list[float], p: int) -> float:
    sorted_vals = sorted(values)
    idx = min(int(len(sorted_vals) * p / 100), len(sorted_vals) - 1)
    return sorted_vals[idx]


def main() -> None:
    rows = load_csv()

    # Group by size label (preserve order from CSV)
    size_order: list[str] = []
    ch_latencies: dict[str, list[float]] = defaultdict(list)
    hit_latencies: dict[str, list[float]] = defaultdict(list)

    for row in rows:
        label = row["size_label"]
        if label not in size_order:
            size_order.append(label)
        ch_latencies[label].append(float(row["ch_latency_ms"]))
        hit_latencies[label].append(float(row["total_hit_ms"]))

    labels = size_order
    x = range(len(labels))

    percentiles = [
        (50, "p50"),
        (90, "p90"),
        (95, "p95"),
        (99, "p99"),
        (0, "min"),
    ]

    fig, axes = plt.subplots(1, 5, figsize=(28, 6), sharey=True)

    for ax, (pval, pname) in zip(axes, percentiles):
        if pname == "min":
            ch_vals = [min(ch_latencies[l]) for l in labels]
            hit_vals = [min(hit_latencies[l]) for l in labels]
        else:
            ch_vals = [percentile(ch_latencies[l], pval) for l in labels]
            hit_vals = [percentile(hit_latencies[l], pval) for l in labels]

        ax.plot(x, ch_vals, "o-", color="#e74c3c", linewidth=2, markersize=5,
                label="ClickHouse")
        ax.plot(x, hit_vals, "s-", color="#2ecc71", linewidth=2, markersize=5,
                label="S3 Cache Hit")

        # Shade: green where cache wins, red where CH wins
        ax.fill_between(x, hit_vals, ch_vals, alpha=0.1, color="#2ecc71",
                        where=[h < c for h, c in zip(hit_vals, ch_vals)])
        ax.fill_between(x, hit_vals, ch_vals, alpha=0.1, color="#e74c3c",
                        where=[h >= c for h, c in zip(hit_vals, ch_vals)])

        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
        ax.set_xlabel("Response Size")
        ax.set_title(f"{pname.upper()} Latency", fontweight="bold")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_yscale("log")

    axes[0].set_ylabel("Latency (ms, log scale)")

    fig.suptitle("ClickHouse vs S3 Cache Hit Latency by Percentile", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig("benchmark_plot.png", dpi=150, bbox_inches="tight")
    print("Saved benchmark_plot.png")


if __name__ == "__main__":
    main()
