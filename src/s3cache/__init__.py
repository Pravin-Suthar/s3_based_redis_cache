from s3cache.decorator import cached
from s3cache.manager import CacheManager
from s3cache.storage import LocalDiskBackend, S3Backend, StorageBackend

__version__ = "1.0.1"
__all__ = ["cached", "CacheManager", "StorageBackend", "S3Backend", "LocalDiskBackend"]
