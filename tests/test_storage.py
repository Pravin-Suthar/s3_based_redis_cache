from __future__ import annotations

from pathlib import Path

from s3cache.storage.local import LocalDiskBackend


class TestLocalDiskBackend:
    def test_put_and_get(self, cache_dir: Path) -> None:
        backend = LocalDiskBackend(cache_dir)
        backend.put("test_key", b"hello world")
        assert backend.get("test_key") == b"hello world"

    def test_get_missing_returns_none(self, cache_dir: Path) -> None:
        backend = LocalDiskBackend(cache_dir)
        assert backend.get("nonexistent") is None

    def test_delete(self, cache_dir: Path) -> None:
        backend = LocalDiskBackend(cache_dir)
        backend.put("key", b"data")
        backend.delete("key")
        assert backend.get("key") is None

    def test_delete_missing_no_error(self, cache_dir: Path) -> None:
        backend = LocalDiskBackend(cache_dir)
        backend.delete("nonexistent")  # should not raise

    def test_list_keys(self, cache_dir: Path) -> None:
        backend = LocalDiskBackend(cache_dir)
        backend.put("abc.pickle", b"data1")
        backend.put("abd.parquet", b"data2")
        backend.put("xyz.pickle", b"data3")

        keys = backend.list_keys("ab")
        assert sorted(keys) == ["abc.pickle", "abd.parquet"]

    def test_list_keys_empty(self, cache_dir: Path) -> None:
        backend = LocalDiskBackend(cache_dir)
        assert backend.list_keys() == []

    def test_creates_directory(self, tmp_path: Path) -> None:
        new_dir = tmp_path / "new" / "nested"
        backend = LocalDiskBackend(new_dir)
        backend.put("key", b"data")
        assert backend.get("key") == b"data"

    def test_overwrite(self, cache_dir: Path) -> None:
        backend = LocalDiskBackend(cache_dir)
        backend.put("key", b"v1")
        backend.put("key", b"v2")
        assert backend.get("key") == b"v2"
