from __future__ import annotations

import os
import tempfile
from pathlib import Path

from s3cache.storage.base import StorageBackend


class LocalDiskBackend(StorageBackend):
    def __init__(self, base_dir: str | Path) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self._base_dir / key

    def get(self, key: str) -> bytes | None:
        try:
            return self._path(key).read_bytes()
        except FileNotFoundError:
            return None

    def put(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._base_dir)
        try:
            os.write(fd, data)
            os.close(fd)
            os.replace(tmp, path)
        except BaseException:
            os.close(fd) if not os.get_inheritable(fd) else None  # noqa: E501
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def delete(self, key: str) -> None:
        try:
            self._path(key).unlink()
        except FileNotFoundError:
            pass

    def list_keys(self, prefix: str = "") -> list[str]:
        if not self._base_dir.exists():
            return []
        return [
            f.name
            for f in self._base_dir.iterdir()
            if f.is_file() and f.name.startswith(prefix)
        ]
