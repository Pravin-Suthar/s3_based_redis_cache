from __future__ import annotations

from pathlib import Path

import fakeredis
import pytest

from s3cache.manager import CacheManager
from s3cache.storage.local import LocalDiskBackend


@pytest.fixture()
def cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cache"
    d.mkdir()
    return d


@pytest.fixture()
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis()


@pytest.fixture()
def local_storage(cache_dir: Path) -> LocalDiskBackend:
    return LocalDiskBackend(cache_dir)


@pytest.fixture()
def cache_manager(
    fake_redis: fakeredis.FakeRedis, local_storage: LocalDiskBackend
) -> CacheManager:
    mgr = CacheManager.initialize(
        _redis_client=fake_redis,  # type: ignore[arg-type]
        _storage=local_storage,
        default_ttl=60,
        compression="zstd",
        max_cache_entries=100,
        stampede_lock_ttl=5,
    )
    mgr._sync_mode = True
    yield mgr  # type: ignore[misc]
    CacheManager.reset()
