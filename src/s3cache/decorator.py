from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar, cast

from s3cache.key import make_cache_key
from s3cache.manager import CacheManager

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def cached(
    ttl: int | None = None,
    namespace: str | None = None,
    format: str | None = None,
) -> Callable[[F], F]:
    """Decorator that caches the result of a query executor function.

    The first positional argument is treated as the query string for hashing.
    Additional args/kwargs are included in the cache key.

    Args:
        ttl: Cache TTL in seconds. Defaults to CacheManager's default_ttl.
        namespace: Optional namespace for grouping cache entries.
        format: Serialization format override ("auto", "parquet", "pickle", "json").
    """

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                mgr = CacheManager.get()
            except RuntimeError:
                # CacheManager not initialized — just run the function
                return fn(*args, **kwargs)

            effective_ttl = ttl if ttl is not None else mgr.default_ttl

            # Build cache key from query (first arg) + remaining args
            if not args:
                return fn(*args, **kwargs)

            query = str(args[0])
            extra_args = args[1:]

            cache_key = make_cache_key(query, extra_args, kwargs, namespace)

            # Try cache read
            t0 = time.monotonic()
            hit, result = mgr.get_cached(cache_key)
            if hit:
                return result

            # Cache miss — execute function
            lock_acquired = mgr.acquire_stampede_lock(cache_key)
            try:
                result = fn(*args, **kwargs)
            except Exception:
                if lock_acquired:
                    mgr.release_stampede_lock(cache_key)
                raise

            latency = (time.monotonic() - t0) * 1000
            mgr.stats_tracker.record_miss(latency)

            # Persist only if we hold the lock
            if lock_acquired:
                try:
                    mgr.persist_async(cache_key, result, effective_ttl, namespace, format)
                except Exception:
                    logger.warning("Failed to start async persist", exc_info=True)
                    mgr.stats_tracker.record_error()
                finally:
                    # Lock released after persist thread starts
                    # The persist thread handles the actual work
                    mgr.release_stampede_lock(cache_key)

            return result

        return cast(F, wrapper)

    return decorator
