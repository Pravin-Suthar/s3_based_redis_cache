from __future__ import annotations

from abc import ABC, abstractmethod


class StorageBackend(ABC):
    @abstractmethod
    def get(self, key: str) -> bytes | None:
        ...

    @abstractmethod
    def put(self, key: str, data: bytes) -> None:
        ...

    @abstractmethod
    def delete(self, key: str) -> None:
        ...

    @abstractmethod
    def list_keys(self, prefix: str = "") -> list[str]:
        ...
