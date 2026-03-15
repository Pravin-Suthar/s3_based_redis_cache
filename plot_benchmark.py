"""
Reads benchmark_results.csv and plots:
  - p99 latency: ClickHouse vs Cache Hit (per size)
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
    ch_p99 = [percentile(ch_latencies[l], 99) for l in labels]
    ch_min = [min(ch_latencies[l]) for l in labels]
    hit_p99 = [percentile(hit_latencies[l], 99) for l in labels]
    hit_min = [min(hit_latencies[l]) for l in labels]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), sharey=True)

    # ── p99 plot ──
    x = range(len(labels))
    ax1.plot(x, ch_p99, "o-", color="#e74c3c", linewidth=2, markersize=6, label="ClickHouse (p99)")
    ax1.plot(x, hit_p99, "s-", color="#2ecc71", linewidth=2, markersize=6, label="S3 Cache Hit (p99)")
    ax1.fill_between(x, hit_p99, ch_p99, alpha=0.1, color="#2ecc71",
                     where=[h < c for h, c in zip(hit_p99, ch_p99)])
    ax1.fill_between(x, hit_p99, ch_p99, alpha=0.1, color="#e74c3c",
                     where=[h >= c for h, c in zip(hit_p99, ch_p99)])
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(labels, rotation=45, ha="right")
    ax1.set_xlabel("Response Size")
    ax1.set_ylabel("Latency (ms)")
    ax1.set_title("p99 Latency: ClickHouse vs S3 Cache Hit")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_yscale("log")

    # ── Min plot ──
    ax2.plot(x, ch_min, "o-", color="#e74c3c", linewidth=2, markersize=6, label="ClickHouse (min)")
    ax2.plot(x, hit_min, "s-", color="#2ecc71", linewidth=2, markersize=6, label="S3 Cache Hit (min)")
    ax2.fill_between(x, hit_min, ch_min, alpha=0.1, color="#2ecc71",
                     where=[h < c for h, c in zip(hit_min, ch_min)])
    ax2.fill_between(x, hit_min, ch_min, alpha=0.1, color="#e74c3c",
                     where=[h >= c for h, c in zip(hit_min, ch_min)])
    ax2.set_xticks(list(x))
    ax2.set_xticklabels(labels, rotation=45, ha="right")
    ax2.set_xlabel("Response Size")
    ax2.set_title("Min Latency: ClickHouse vs S3 Cache Hit")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_yscale("log")

    plt.tight_layout()
    plt.savefig("benchmark_plot.png", dpi=150, bbox_inches="tight")
    print("Saved benchmark_plot.png")


if __name__ == "__main__":
    main()
