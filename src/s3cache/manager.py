from __future__ import annotations

import logging
import threading
import time
from typing import Any

import redis

from s3cache.serializer import deserialize, serialize
from s3cache.storage.base import StorageBackend
from s3cache.storage.local import LocalDiskBackend
from s3cache.storage.s3 import S3Backend

logger = logging.getLogger(__name__)


class _Stats:
    """Thread-safe cache statistics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self._hit_latencies: list[float] = []
        self._miss_latencies: list[float] = []

    def record_hit(self, latency_ms: float) -> None:
        with self._lock:
            self.hits += 1
            self._hit_latencies.append(latency_ms)

    def record_miss(self, latency_ms: float) -> None:
        with self._lock:
            self.misses += 1
            self._miss_latencies.append(latency_ms)

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            total = self.hits + self.misses
            return {
                "hits": self.hits,
                "misses": self.misses,
                "errors": self.errors,
                "hit_rate": self.hits / total if total > 0 else 0.0,
                "avg_hit_latency_ms": (
                    sum(self._hit_latencies) / len(self._hit_latencies)
                    if self._hit_latencies
                    else 0.0
                ),
                "avg_miss_latency_ms": (
                    sum(self._miss_latencies) / len(self._miss_latencies)
                    if self._miss_latencies
                    else 0.0
                ),
            }


class CacheManager:
    """Singleton cache manager. Call initialize() once at startup."""

    _instance: CacheManager | None = None
    _lock = threading.Lock()

    def __init__(
        self,
        redis_client: redis.Redis,  # type: ignore[type-arg]
        storage: StorageBackend,
        redis_key_prefix: str,
        default_ttl: int,
        serialization_format: str,
        compression: str,
        max_cache_entries: int,
        stampede_lock_ttl: int,
    ) -> None:
        self.redis = redis_client
        self.storage = storage
        self.redis_key_prefix = redis_key_prefix
        self.default_ttl = default_ttl
        self.serialization_format = serialization_format
        self.compression = compression
        self.max_cache_entries = max_cache_entries
        self.stampede_lock_ttl = stampede_lock_ttl
        self._sync_mode = False
        self._stats = _Stats()

    @classmethod
    def initialize(
        cls,
        *,
        s3_bucket: str = "",
        s3_prefix: str = "query-cache",
        redis_url: str = "redis://localhost:6379/0",
        redis_key_prefix: str = "qc:",
        default_ttl: int = 3600,
        serialization_format: str = "auto",
        compression: str = "zstd",
        max_cache_entries: int = 10000,
        stampede_lock_ttl: int = 30,
        storage_backend: str = "s3",
        local_path: str = "/tmp/query_cache",
        aws_region: str = "us-east-1",
        # For testing: allow injecting dependencies
        _redis_client: redis.Redis | None = None,  # type: ignore[type-arg]
        _storage: StorageBackend | None = None,
    ) -> CacheManager:
        with cls._lock:
            if _redis_client is not None:
                redis_client = _redis_client
            else:
                redis_client = redis.Redis.from_url(redis_url)

            if _storage is not None:
                storage = _storage
            elif storage_backend == "local":
                storage = LocalDiskBackend(local_path)
            else:
                storage = S3Backend(bucket=s3_bucket, prefix=s3_prefix, region=aws_region)

            cls._instance = cls(
                redis_client=redis_client,
                storage=storage,
                redis_key_prefix=redis_key_prefix,
                default_ttl=default_ttl,
                serialization_format=serialization_format,
                compression=compression,
                max_cache_entries=max_cache_entries,
                stampede_lock_ttl=stampede_lock_ttl,
            )
            return cls._instance

    @classmethod
    def get(cls) -> CacheManager:
        if cls._instance is None:
            raise RuntimeError(
                "CacheManager not initialized. Call CacheManager.initialize() first."
            )
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton. Primarily for testing."""
        with cls._lock:
            cls._instance = None

    @property
    def stats_tracker(self) -> _Stats:
        return self._stats

    def _redis_key(self, query_hash: str) -> str:
        return f"{self.redis_key_prefix}{query_hash}"

    def _lock_key(self, query_hash: str) -> str:
        return f"{self.redis_key_prefix}lock:{query_hash}"

    def get_cached(self, query_hash: str) -> tuple[bool, Any]:
        """Check Redis metadata and fetch from storage. Returns (hit, result)."""
        t0 = time.monotonic()
        try:
            rkey = self._redis_key(query_hash)
            meta = self.redis.hgetall(rkey)
            if not meta:
                return False, None

            raw_s3 = meta.get(b"s3_path", meta.get("s3_path", b""))
            s3_path = raw_s3.decode() if isinstance(raw_s3, bytes) else str(raw_s3)
            raw_fmt = meta.get(b"format", meta.get("format", b"pickle"))
            fmt = raw_fmt.decode() if isinstance(raw_fmt, bytes) else str(raw_fmt)
            raw_comp = meta.get(b"compression", meta.get("compression", b"zstd"))
            compression = raw_comp.decode() if isinstance(raw_comp, bytes) else str(raw_comp)

            if not s3_path:
                return False, None

            data = self.storage.get(s3_path)
            if data is None:
                # Orphaned Redis key — clean it up
                self.redis.delete(rkey)
                return False, None

            result = deserialize(data, fmt, compression)
            latency = (time.monotonic() - t0) * 1000
            self._stats.record_hit(latency)
            return True, result

        except Exception:
            logger.warning("Cache read failed", exc_info=True)
            self._stats.record_error()
            return False, None

    def persist_async(
        self,
        query_hash: str,
        result: Any,
        ttl: int,
        namespace: str | None,
        fmt: str | None = None,
    ) -> None:
        """Persist a result to cache in a background daemon thread."""
        if self._sync_mode:
            self._persist(query_hash, result, ttl, namespace, fmt)
            return
        t = threading.Thread(
            target=self._persist,
            args=(query_hash, result, ttl, namespace, fmt),
            daemon=True,
        )
        t.start()

    def _persist(
        self,
        query_hash: str,
        result: Any,
        ttl: int,
        namespace: str | None,
        fmt: str | None,
    ) -> None:
        try:
            actual_fmt = fmt or self.serialization_format
            data, used_fmt = serialize(result, actual_fmt, self.compression)
            s3_path = f"{query_hash}.{used_fmt}"

            self.storage.put(s3_path, data)

            rkey = self._redis_key(query_hash)
            mapping: dict[str | bytes, bytes | float | int | str] = {
                "s3_path": s3_path,
                "created_at": str(int(time.time())),
                "ttl": str(ttl),
                "size_bytes": str(len(data)),
                "format": used_fmt,
                "compression": self.compression,
            }
            if namespace:
                mapping["namespace"] = namespace

            self.redis.hset(rkey, mapping=mapping)
            self.redis.expire(rkey, ttl)

            self._evict_if_needed()

        except Exception:
            logger.warning("Cache persist failed for %s", query_hash, exc_info=True)
            self._stats.record_error()

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if we exceed max_cache_entries."""
        try:
            pattern = f"{self.redis_key_prefix}*"
            keys: list[bytes] = []
            cursor: int = 0
            while True:
                cursor, batch = self.redis.scan(cursor=cursor, match=pattern, count=500)
                # Filter out lock keys
                keys.extend(
                    k for k in batch if not k.decode().startswith(f"{self.redis_key_prefix}lock:")
                )
                if cursor == 0:
                    break

            if len(keys) <= self.max_cache_entries:
                return

            # Sort by created_at and evict oldest
            entries: list[tuple[bytes, int]] = []
            for k in keys:
                created = self.redis.hget(k, "created_at")
                if created:
                    entries.append((k, int(created)))

            entries.sort(key=lambda x: x[1])
            to_evict = entries[: len(entries) - self.max_cache_entries]

            for k, _ in to_evict:
                s3_path_bytes = self.redis.hget(k, "s3_path")
                if s3_path_bytes:
                    self.storage.delete(s3_path_bytes.decode())
                self.redis.delete(k)

        except Exception:
            logger.warning("Eviction failed", exc_info=True)

    def acquire_stampede_lock(self, query_hash: str) -> bool:
        """Try to acquire a stampede lock. Returns True if acquired."""
        try:
            return bool(
                self.redis.set(
                    self._lock_key(query_hash),
                    "1",
                    nx=True,
                    ex=self.stampede_lock_ttl,
                )
            )
        except Exception:
            logger.warning("Stampede lock acquire failed", exc_info=True)
            return False

    def release_stampede_lock(self, query_hash: str) -> None:
        """Release a stampede lock."""
        try:
            self.redis.delete(self._lock_key(query_hash))
        except Exception:
            logger.warning("Stampede lock release failed", exc_info=True)

    def invalidate(self, query_hash: str) -> None:
        """Invalidate a single cache entry by hash."""
        try:
            rkey = self._redis_key(query_hash)
            s3_path_bytes = self.redis.hget(rkey, "s3_path")
            if s3_path_bytes:
                self.storage.delete(s3_path_bytes.decode())
            self.redis.delete(rkey)
        except Exception:
            logger.warning("Invalidation failed for %s", query_hash, exc_info=True)

    def invalidate_namespace(self, namespace: str) -> None:
        """Invalidate all cache entries in a namespace."""
        try:
            pattern = f"{self.redis_key_prefix}*"
            cursor: int = 0
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match=pattern, count=500)
                for k in keys:
                    ns = self.redis.hget(k, "namespace")
                    if ns and ns.decode() == namespace:
                        s3_path_bytes = self.redis.hget(k, "s3_path")
                        if s3_path_bytes:
                            self.storage.delete(s3_path_bytes.decode())
                        self.redis.delete(k)
                if cursor == 0:
                    break
        except Exception:
            logger.warning("Namespace invalidation failed for %s", namespace, exc_info=True)

    def clear(self) -> None:
        """Clear all cache entries."""
        try:
            pattern = f"{self.redis_key_prefix}*"
            cursor: int = 0
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match=pattern, count=500)
                for k in keys:
                    s3_path_bytes = self.redis.hget(k, "s3_path")
                    if s3_path_bytes:
                        self.storage.delete(s3_path_bytes.decode())
                    self.redis.delete(k)
                if cursor == 0:
                    break
        except Exception:
            logger.warning("Clear failed", exc_info=True)

    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        return self._stats.to_dict()

    def cleanup_orphaned_objects(self) -> int:
        """Delete S3 objects with no matching Redis key. Returns count deleted."""
        deleted = 0
        try:
            all_keys = self.storage.list_keys()
            for s3_key in all_keys:
                # Extract hash from filename (e.g., "abc123.pickle" -> "abc123")
                query_hash = s3_key.rsplit(".", 1)[0] if "." in s3_key else s3_key
                rkey = self._redis_key(query_hash)
                if not self.redis.exists(rkey):
                    self.storage.delete(s3_key)
                    deleted += 1
        except Exception:
            logger.warning("Orphan cleanup failed", exc_info=True)
        return deleted
