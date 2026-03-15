"""
Quick start:
    docker run -d -p 6379:6379 redis:7-alpine
    source .venv/bin/activate
    python example.py

Requires:
    - Redis running on localhost:6379
    - AWS credentials configured (aws configure / env vars / IAM role)
    - An existing S3 bucket
"""

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

SIZES_MB = sorted(random.sample(range(1, 20), 4))
ITERATIONS = 3

DB_BASE_SEC = 3


def db_latency(size_mb: int) -> float:
    """Simulate realistic DB latency: 3s base, log growth."""
    return DB_BASE_SEC * (1 + math.log2(size_mb))


def generate_data(size_mb: int) -> list[dict[str, str]]:
    row_size = 200
    num_rows = int((size_mb * 1024 * 1024) / row_size)
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


# Pre-generate data
pregenerated: dict[str, list[dict[str, str]]] = {}

print(f"Sizes: {SIZES_MB} MB | Iterations: {ITERATIONS}")
print(f"DB latency: {DB_BASE_SEC}s base, log scale "
      f"(1MB={db_latency(1):.1f}s, 10MB={db_latency(10):.1f}s, 50MB={db_latency(50):.1f}s)\n")

print("Generating test data...")
for size in SIZES_MB:
    for i in range(ITERATIONS):
        pregenerated[f"{size}mb_{i}"] = generate_data(size)
    print(f"  {size:>2}MB -> {len(pregenerated[f'{size}mb_0'])} rows")
print()

mgr.clear()

# ── Benchmark ──
for size in SIZES_MB:
    sim_db_time = db_latency(size)

    serialize_times: list[float] = []
    upload_times: list[float] = []
    download_times: list[float] = []
    deserialize_times: list[float] = []
    total_miss_times: list[float] = []
    total_hit_times: list[float] = []
    compressed_sizes: list[int] = []

    for i in range(ITERATIONS):
        data_key = f"{size}mb_{i}"
        data = pregenerated[data_key]
        query = f"SELECT * FROM table_{size}mb_{i}"
        cache_key = make_cache_key(query, (data_key, sim_db_time), {}, "benchmark")

        # ── MISS PATH: simulate DB + serialize + compress + upload + redis write ──
        t_total = time.perf_counter()

        # 1) Simulated DB time (fixed)
        time.sleep(sim_db_time)

        # 2) Serialize + compress
        t0 = time.perf_counter()
        compressed, fmt = serialize(data, "auto", "zstd")
        serialize_times.append((time.perf_counter() - t0) * 1000)
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
        upload_times.append((time.perf_counter() - t0) * 1000)

        total_miss_times.append((time.perf_counter() - t_total) * 1000)

        # ── HIT PATH: redis lookup + S3 download + decompress + deserialize ──
        t_total = time.perf_counter()

        # 1) S3 download
        t0 = time.perf_counter()
        raw = mgr.storage.get(s3_path)
        download_times.append((time.perf_counter() - t0) * 1000)

        # 2) Decompress + deserialize
        t0 = time.perf_counter()
        _ = deserialize(raw, fmt, "zstd")
        deserialize_times.append((time.perf_counter() - t0) * 1000)

        total_hit_times.append((time.perf_counter() - t_total) * 1000)

        # cleanup for next iteration
        mgr.storage.delete(s3_path)
        mgr.redis.delete(mgr._redis_key(cache_key))

    avg_compressed = sum(compressed_sizes) / len(compressed_sizes)

    print(f"{'=' * 70}")
    print(f"  {size}MB Response | Compressed: {avg_compressed / 1024 / 1024:.2f}MB "
          f"({(1 - avg_compressed / (size * 1024 * 1024)) * 100:.0f}% reduction)")
    print(f"{'=' * 70}")
    print()

    # Miss breakdown
    print(f"  CACHE MISS (without cache):")
    print(f"    DB query (simulated)     : {fmt_ms(sim_db_time * 1000):>10}  (fixed)")
    print(f"    Serialize + compress     : {fmt_ms(sum(serialize_times) / ITERATIONS):>10}  "
          f"(p95: {fmt_ms(percentile(serialize_times, 95))})")
    print(f"    S3 upload + Redis write  : {fmt_ms(sum(upload_times) / ITERATIONS):>10}  "
          f"(p95: {fmt_ms(percentile(upload_times, 95))})")
    avg_miss = sum(total_miss_times) / ITERATIONS
    print(f"    ─────────────────────────")
    print(f"    Total                    : {fmt_ms(avg_miss):>10}  "
          f"(p95: {fmt_ms(percentile(total_miss_times, 95))}, "
          f"p99: {fmt_ms(percentile(total_miss_times, 99))})")
    print()

    # Hit breakdown
    print(f"  CACHE HIT (with cache):")
    print(f"    S3 download              : {fmt_ms(sum(download_times) / ITERATIONS):>10}  "
          f"(p95: {fmt_ms(percentile(download_times, 95))})")
    print(f"    Decompress + deserialize : {fmt_ms(sum(deserialize_times) / ITERATIONS):>10}  "
          f"(p95: {fmt_ms(percentile(deserialize_times, 95))})")
    avg_hit = sum(total_hit_times) / ITERATIONS
    print(f"    ─────────────────────────")
    print(f"    Total                    : {fmt_ms(avg_hit):>10}  "
          f"(p95: {fmt_ms(percentile(total_hit_times, 95))}, "
          f"p99: {fmt_ms(percentile(total_hit_times, 99))})")
    print()

    saved_pct = ((avg_miss - avg_hit) / avg_miss) * 100
    print(f"  Saved: {saved_pct:.1f}% | "
          f"Miss: {fmt_ms(avg_miss)} -> Hit: {fmt_ms(avg_hit)}")
    print()

print(f"\nStats: {mgr.stats()}")
mgr.clear()
print("Cache cleared.")
