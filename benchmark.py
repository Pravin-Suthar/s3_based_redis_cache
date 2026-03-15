"""
Quick start:
    docker run -d -p 6379:6379 redis:7-alpine
    source .venv/bin/activate
    python benchmark.py

Requires:
    - Redis running on localhost:6379
    - AWS credentials configured (aws configure / env vars / IAM role)
    - An existing S3 bucket
"""

import csv
import math
import random
import string
import time

from s3cache import CacheManager, cached
from s3cache.key import make_cache_key
from s3cache.serializer import deserialize, serialize

mgr = CacheManager.initialize(
    s3_bucket="s3-cache-testing-bucket",
    s3_prefix="s3-cache",
    redis_url="redis://localhost:6379/0",
    aws_region="us-east-1",
    default_ttl=3600,
    compression="zstd",
    serialization_format="auto",
)

# this needs to be false in production here we are doing sequential for performance metrics
mgr._sync_mode = True

# ── Realistic ClickHouse latency model ──
#
# Based on typical ClickHouse analytical query performance:
#
#   Component               | Estimate
#   ─────────────────────── | ─────────────────────────────────────────
#   Query parse + plan      | ~10ms (constant)
#   Column scan (compressed)| ~50ms base + near-zero for small results
#   Aggregation / merge     | ~20ms base, scales with row count
#   Result serialization    | ~50ms/MB (row→JSON/Native format)
#   Network transfer        | ~80ms/MB (cross-AZ or app↔CH latency)
#   ─────────────────────── | ─────────────────────────────────────────
#   Total model             | 0.08s + 0.13s * size_mb
#
# Examples:
#   1KB  → ~80ms   (overhead-dominated, fast point/agg query)
#   100KB→ ~93ms   (still mostly overhead)
#   1MB  → ~210ms  (serialization + transfer start to matter)
#   10MB → ~1.38s  (transfer-dominated)
#   50MB → ~6.58s  (large analytical export)
#
# Source: ClickHouse docs benchmarks, real-world observability on MergeTree
# tables with 100M-1B rows, queries over LAN/cross-AZ networking.

# Based on real ClickHouse perf: ~10ms parse/plan, ~50ms base scan, ~50ms/MB serialization, ~80ms/MB network transfer
CH_BASE_SEC = 0.08       # fixed overhead: parse + plan + base scan
CH_PER_MB_SEC = 0.13     # serialization + network per MB of result


def ch_latency(size_mb: float) -> float:
    """Estimated ClickHouse query latency for a given result size."""
    return CH_BASE_SEC + CH_PER_MB_SEC * size_mb


# Sizes from tiny KB payloads (where caching may not help) up to large MB results
SIZES = [
    ("1KB", 1 / 1024),
    ("10KB", 10 / 1024),
    ("50KB", 50 / 1024),
    ("100KB", 100 / 1024),
    ("500KB", 500 / 1024),
    ("1MB", 1),
    ("2MB", 2),
    ("5MB", 5),
    ("10MB", 10),
    ("20MB", 20),
    ("30MB", 30),
    ("50MB", 50),
]

ITERATIONS = 10
CSV_FILE = "benchmark_results.csv"


def generate_data(size_mb: float) -> list[dict[str, str]]:
    row_size = 200
    num_rows = max(1, int((size_mb * 1024 * 1024) / row_size))
    return [
        {
            "id": str(i),
            "name": "".join(random.choices(string.ascii_letters, k=20)),
            "email": "".join(random.choices(string.ascii_lowercase, k=10)) + "@example.com",
            "data": "".join(random.choices(string.ascii_letters, k=100)),
        }
        for i in range(num_rows)
    ]


def percentile(values: list[float], p: int) -> float:
    sorted_vals = sorted(values)
    idx = min(int(len(sorted_vals) * p / 100), len(sorted_vals) - 1)
    return sorted_vals[idx]


def fmt_ms(ms: float) -> str:
    if ms >= 1000:
        return f"{ms / 1000:.2f}s"
    return f"{ms:.1f}ms"


# CSV rows collector
csv_rows: list[dict[str, object]] = []

print(f"Sizes: {[s[0] for s in SIZES]} | Iterations: {ITERATIONS}")
print(f"ClickHouse latency model: {CH_BASE_SEC}s base + {CH_PER_MB_SEC}s/MB")
print(f"  1KB={ch_latency(1/1024)*1000:.0f}ms, 1MB={ch_latency(1)*1000:.0f}ms, "
      f"10MB={ch_latency(10):.2f}s, 50MB={ch_latency(50):.2f}s\n")

print("Generating test data...")
pregenerated: dict[str, list[dict[str, str]]] = {}
for label, size_mb in SIZES:
    pregenerated[label] = generate_data(size_mb)
    row_count = len(pregenerated[label])
    print(f"  {label:>5} -> {row_count:>7} rows")
print()

mgr.clear()

# ── Benchmark ──
for label, size_mb in SIZES:
    sim_db_sec = ch_latency(size_mb)
    raw_bytes = int(size_mb * 1024 * 1024)

    serialize_times: list[float] = []
    upload_times: list[float] = []
    download_times: list[float] = []
    deserialize_times: list[float] = []
    total_miss_times: list[float] = []
    total_hit_times: list[float] = []
    compressed_sizes: list[int] = []

    for i in range(ITERATIONS):
        data = pregenerated[label]
        query = f"SELECT * FROM table_{label}_{i}"
        cache_key = make_cache_key(query, (label, i, sim_db_sec), {}, "benchmark")

        # ── MISS PATH: simulate DB + serialize + compress + upload + redis write ──
        t_total = time.perf_counter()

        # 1) Simulated ClickHouse query time
        time.sleep(sim_db_sec)

        # 2) Serialize + compress
        t0 = time.perf_counter()
        compressed, fmt = serialize(data, "auto", "zstd")
        ser_ms = (time.perf_counter() - t0) * 1000
        serialize_times.append(ser_ms)
        compressed_sizes.append(len(compressed))

        # 3) S3 upload + Redis write
        t0 = time.perf_counter()
        s3_path = f"{cache_key}.{fmt}"
        mgr.storage.put(s3_path, compressed)
        mgr.redis.hset(mgr._redis_key(cache_key), mapping={
            "s3_path": s3_path, "format": fmt, "compression": "zstd",
            "created_at": str(int(time.time())), "ttl": "3600",
            "size_bytes": str(len(compressed)),
        })
        mgr.redis.expire(mgr._redis_key(cache_key), 3600)
        upl_ms = (time.perf_counter() - t0) * 1000
        upload_times.append(upl_ms)

        miss_ms = (time.perf_counter() - t_total) * 1000
        total_miss_times.append(miss_ms)

        # ── HIT PATH: redis lookup + S3 download + decompress + deserialize ──
        t_total = time.perf_counter()

        # 1) S3 download
        t0 = time.perf_counter()
        raw = mgr.storage.get(s3_path)
        dl_ms = (time.perf_counter() - t0) * 1000
        download_times.append(dl_ms)

        # 2) Decompress + deserialize
        t0 = time.perf_counter()
        _ = deserialize(raw, fmt, "zstd")
        deser_ms = (time.perf_counter() - t0) * 1000
        deserialize_times.append(deser_ms)

        hit_ms = (time.perf_counter() - t_total) * 1000
        total_hit_times.append(hit_ms)

        # Per-iteration CSV row
        csv_rows.append({
            "size_label": label,
            "size_mb": round(size_mb, 4),
            "iteration": i + 1,
            "raw_size_bytes": raw_bytes,
            "compressed_size_bytes": len(compressed),
            "compression_ratio_pct": round((1 - len(compressed) / max(raw_bytes, 1)) * 100, 2),
            "ch_latency_ms": round(sim_db_sec * 1000, 2),
            "serialize_ms": round(ser_ms, 2),
            "s3_upload_ms": round(upl_ms, 2),
            "total_miss_ms": round(miss_ms, 2),
            "s3_download_ms": round(dl_ms, 2),
            "deserialize_ms": round(deser_ms, 2),
            "total_hit_ms": round(hit_ms, 2),
            "speedup_pct": round(((miss_ms - hit_ms) / miss_ms) * 100, 2),
        })

        # cleanup for next iteration
        mgr.storage.delete(s3_path)
        mgr.redis.delete(mgr._redis_key(cache_key))

    avg_compressed = sum(compressed_sizes) / len(compressed_sizes)

    print(f"{'=' * 70}")
    print(f"  {label} Response | Compressed: {avg_compressed / 1024:.1f}KB "
          f"({(1 - avg_compressed / max(raw_bytes, 1)) * 100:.0f}% reduction)")
    print(f"{'=' * 70}")
    print()

    # Miss breakdown
    print(f"  CACHE MISS (cold path = ClickHouse + serialize + S3 upload):")
    print(f"    ClickHouse query (sim)   : {fmt_ms(sim_db_sec * 1000):>10}")
    print(f"    Serialize + compress     : {fmt_ms(sum(serialize_times) / ITERATIONS):>10}  "
          f"(p50: {fmt_ms(percentile(serialize_times, 50))}, "
          f"p95: {fmt_ms(percentile(serialize_times, 95))})")
    print(f"    S3 upload + Redis write  : {fmt_ms(sum(upload_times) / ITERATIONS):>10}  "
          f"(p50: {fmt_ms(percentile(upload_times, 50))}, "
          f"p95: {fmt_ms(percentile(upload_times, 95))})")
    avg_miss = sum(total_miss_times) / ITERATIONS
    print(f"    ─────────────────────────")
    print(f"    Total                    : {fmt_ms(avg_miss):>10}  "
          f"(p50: {fmt_ms(percentile(total_miss_times, 50))}, "
          f"p95: {fmt_ms(percentile(total_miss_times, 95))})")
    print()

    # Hit breakdown
    print(f"  CACHE HIT (warm path = S3 download + deserialize):")
    print(f"    S3 download              : {fmt_ms(sum(download_times) / ITERATIONS):>10}  "
          f"(p50: {fmt_ms(percentile(download_times, 50))}, "
          f"p95: {fmt_ms(percentile(download_times, 95))})")
    print(f"    Decompress + deserialize : {fmt_ms(sum(deserialize_times) / ITERATIONS):>10}  "
          f"(p50: {fmt_ms(percentile(deserialize_times, 50))}, "
          f"p95: {fmt_ms(percentile(deserialize_times, 95))})")
    avg_hit = sum(total_hit_times) / ITERATIONS
    print(f"    ─────────────────────────")
    print(f"    Total                    : {fmt_ms(avg_hit):>10}  "
          f"(p50: {fmt_ms(percentile(total_hit_times, 50))}, "
          f"p95: {fmt_ms(percentile(total_hit_times, 95))})")
    print()

    saved_pct = ((avg_miss - avg_hit) / avg_miss) * 100
    verdict = "CACHE WINS" if avg_hit < sim_db_sec * 1000 else "CACHE SLOWER THAN CH"
    print(f"  {verdict} | Speedup: {saved_pct:.1f}% | "
          f"Miss: {fmt_ms(avg_miss)} -> Hit: {fmt_ms(avg_hit)} "
          f"(vs CH alone: {fmt_ms(sim_db_sec * 1000)})")
    print()

# ── Write CSV ──
fieldnames = [
    "size_label", "size_mb", "iteration", "raw_size_bytes", "compressed_size_bytes",
    "compression_ratio_pct", "ch_latency_ms", "serialize_ms", "s3_upload_ms",
    "total_miss_ms", "s3_download_ms", "deserialize_ms", "total_hit_ms", "speedup_pct",
]
with open(CSV_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(csv_rows)

print(f"\nResults written to {CSV_FILE} ({len(csv_rows)} rows)")
print(f"Stats: {mgr.stats()}")
mgr.clear()
print("Cache cleared.")
