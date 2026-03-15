from __future__ import annotations

from s3cache.manager import CacheManager


class TestCacheManager:
    def test_singleton(self, cache_manager: CacheManager) -> None:
        assert CacheManager.get() is cache_manager

    def test_get_without_init_raises(self) -> None:
        CacheManager.reset()
        try:
            import pytest

            with pytest.raises(RuntimeError, match="not initialized"):
                CacheManager.get()
        finally:
            pass

    def test_persist_and_retrieve(self, cache_manager: CacheManager) -> None:
        # Persist synchronously by calling _persist directly
        cache_manager._persist("test_hash", {"result": 42}, 60, None, None)

        hit, result = cache_manager.get_cached("test_hash")
        assert hit is True
        assert result == {"result": 42}

    def test_cache_miss(self, cache_manager: CacheManager) -> None:
        hit, result = cache_manager.get_cached("nonexistent")
        assert hit is False
        assert result is None

    def test_invalidate(self, cache_manager: CacheManager) -> None:
        cache_manager._persist("to_delete", {"data": 1}, 60, None, None)
        hit, _ = cache_manager.get_cached("to_delete")
        assert hit is True

        cache_manager.invalidate("to_delete")
        hit, _ = cache_manager.get_cached("to_delete")
        assert hit is False

    def test_invalidate_namespace(self, cache_manager: CacheManager) -> None:
        cache_manager._persist("ns1_a", {"a": 1}, 60, "analytics", None)
        cache_manager._persist("ns1_b", {"b": 2}, 60, "analytics", None)
        cache_manager._persist("ns2_c", {"c": 3}, 60, "other", None)

        cache_manager.invalidate_namespace("analytics")

        hit_a, _ = cache_manager.get_cached("ns1_a")
        hit_b, _ = cache_manager.get_cached("ns1_b")
        hit_c, _ = cache_manager.get_cached("ns2_c")
        assert hit_a is False
        assert hit_b is False
        assert hit_c is True

    def test_clear(self, cache_manager: CacheManager) -> None:
        cache_manager._persist("h1", {"a": 1}, 60, None, None)
        cache_manager._persist("h2", {"b": 2}, 60, None, None)

        cache_manager.clear()

        hit1, _ = cache_manager.get_cached("h1")
        hit2, _ = cache_manager.get_cached("h2")
        assert hit1 is False
        assert hit2 is False

    def test_stats(self, cache_manager: CacheManager) -> None:
        cache_manager._persist("stat_key", {"x": 1}, 60, None, None)

        # Hit
        cache_manager.get_cached("stat_key")
        # Miss
        cache_manager.get_cached("nonexistent")

        stats = cache_manager.stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 0  # misses tracked in decorator
        assert stats["hit_rate"] == 1.0

    def test_stampede_lock(self, cache_manager: CacheManager) -> None:
        assert cache_manager.acquire_stampede_lock("stamp") is True
        # Second acquire should fail
        assert cache_manager.acquire_stampede_lock("stamp") is False
        # Release and re-acquire
        cache_manager.release_stampede_lock("stamp")
        assert cache_manager.acquire_stampede_lock("stamp") is True
        cache_manager.release_stampede_lock("stamp")

    def test_cleanup_orphaned_objects(self, cache_manager: CacheManager) -> None:
        # Persist a real entry
        cache_manager._persist("real_entry", {"a": 1}, 60, None, None)
        # Put an orphaned object directly in storage
        cache_manager.storage.put("orphan.pickle", b"stale data")

        deleted = cache_manager.cleanup_orphaned_objects()
        assert deleted == 1
        assert cache_manager.storage.get("orphan.pickle") is None
        # Real entry still exists
        hit, _ = cache_manager.get_cached("real_entry")
        assert hit is True

    def test_eviction(self, cache_manager: CacheManager) -> None:
        cache_manager.max_cache_entries = 3

        for i in range(5):
            cache_manager._persist(f"evict_{i}", {"i": i}, 60, None, None)

        # Should have at most 3 entries
        count = 0
        for i in range(5):
            hit, _ = cache_manager.get_cached(f"evict_{i}")
            if hit:
                count += 1
        assert count <= 3
