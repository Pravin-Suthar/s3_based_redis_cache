from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

from s3cache.decorator import cached
from s3cache.manager import CacheManager


class TestCachedDecorator:
    def test_cache_miss_calls_function(self, cache_manager: CacheManager) -> None:
        call_count = 0

        @cached(ttl=60)
        def run_query(query: str) -> dict[str, int]:
            nonlocal call_count
            call_count += 1
            return {"result": 42}

        result = run_query("SELECT 1")
        assert result == {"result": 42}
        assert call_count == 1

    def test_cache_hit_skips_function(self, cache_manager: CacheManager) -> None:
        call_count = 0

        @cached(ttl=60)
        def run_query(query: str) -> dict[str, int]:
            nonlocal call_count
            call_count += 1
            return {"result": 42}

        # First call — miss (sync persist in test mode)
        run_query("SELECT 1")

        # Second call — hit
        result = run_query("SELECT 1")
        assert result == {"result": 42}
        assert call_count == 1

    def test_different_queries_different_results(self, cache_manager: CacheManager) -> None:
        @cached(ttl=60)
        def run_query(query: str) -> str:
            return f"result_for_{query}"

        r1 = run_query("SELECT 1")
        r2 = run_query("SELECT 2")
        assert r1 != r2

    def test_namespace(self, cache_manager: CacheManager) -> None:
        @cached(ttl=60, namespace="ns1")
        def run_query(query: str) -> str:
            return "result"

        result = run_query("SELECT 1")
        assert result == "result"

    def test_ttl_expiry(self, cache_manager: CacheManager) -> None:
        call_count = 0

        @cached(ttl=1)
        def run_query(query: str) -> dict[str, int]:
            nonlocal call_count
            call_count += 1
            return {"result": call_count}

        run_query("SELECT 1")

        # Should be cached
        run_query("SELECT 1")
        assert call_count == 1

        # Wait for TTL expiry (Redis EXPIRE)
        time.sleep(1.5)

        run_query("SELECT 1")
        assert call_count == 2

    def test_fail_open_redis_down(self, cache_manager: CacheManager) -> None:
        """If Redis fails, function still works."""

        @cached(ttl=60)
        def run_query(query: str) -> str:
            return "fresh_result"

        # Break Redis
        original_hgetall = cache_manager.redis.hgetall
        cache_manager.redis.hgetall = MagicMock(side_effect=Exception("Redis down"))

        result = run_query("SELECT 1")
        assert result == "fresh_result"

        cache_manager.redis.hgetall = original_hgetall

    def test_fail_open_storage_fails(self, cache_manager: CacheManager) -> None:
        """If storage get fails, function still works."""

        @cached(ttl=60)
        def run_query(query: str) -> str:
            return "fresh_result"

        # Persist first
        run_query("SELECT 1")

        # Break storage
        original_get = cache_manager.storage.get
        cache_manager.storage.get = MagicMock(return_value=None)  # type: ignore[assignment]

        result = run_query("SELECT 1")
        assert result == "fresh_result"

        cache_manager.storage.get = original_get  # type: ignore[assignment]

    def test_fail_open_serialization_error(self, cache_manager: CacheManager) -> None:
        """If deserialization fails, function still works."""
        from s3cache import manager as mgr_mod

        @cached(ttl=60)
        def run_query(query: str) -> str:
            return "fresh_result"

        run_query("SELECT 1")

        # Break deserializer in the manager module where it's used
        original_deserialize = mgr_mod.deserialize
        mgr_mod.deserialize = MagicMock(side_effect=Exception("Bad data"))  # type: ignore[attr-defined]

        result = run_query("SELECT 1")
        assert result == "fresh_result"

        mgr_mod.deserialize = original_deserialize  # type: ignore[attr-defined]

    def test_function_exception_propagates(self, cache_manager: CacheManager) -> None:
        @cached(ttl=60)
        def bad_query(query: str) -> str:
            raise ValueError("Query failed")

        import pytest

        with pytest.raises(ValueError, match="Query failed"):
            bad_query("SELECT 1")

    def test_stats_increment(self, cache_manager: CacheManager) -> None:
        @cached(ttl=60)
        def run_query(query: str) -> str:
            return "result"

        run_query("SELECT 1")
        run_query("SELECT 1")  # hit

        stats = cache_manager.stats()
        assert stats["hits"] >= 1
        assert stats["misses"] >= 1

    def test_no_cache_manager_runs_function(self) -> None:
        """Without CacheManager initialized, function runs normally."""
        CacheManager.reset()

        @cached(ttl=60)
        def run_query(query: str) -> str:
            return "uncached"

        result = run_query("SELECT 1")
        assert result == "uncached"


class TestStampede:
    def test_concurrent_threads_single_persist(self, cache_manager: CacheManager) -> None:
        """10 concurrent threads, same cold query: all get results, only 1 persists."""
        # Disable sync mode for this concurrency test
        cache_manager._sync_mode = False
        call_count = 0
        lock = threading.Lock()

        @cached(ttl=60)
        def slow_query(query: str) -> dict[str, int]:
            nonlocal call_count
            with lock:
                call_count += 1
            time.sleep(0.1)  # simulate slow query
            return {"result": 42}

        results: list[dict[str, int]] = []
        errors: list[Exception] = []

        def worker() -> None:
            try:
                r = slow_query("SELECT expensive_thing")
                results.append(r)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0
        assert len(results) == 10
        assert all(r == {"result": 42} for r in results)

        # Wait for any async persist threads
        time.sleep(1.0)

        # All threads executed the function (stampede protection doesn't block)
        # but only one should have persisted to storage
        keys = cache_manager.storage.list_keys()
        assert len(keys) == 1
