from __future__ import annotations

import hashlib
import json
import re
from typing import Any


def normalize_query(query: str) -> str:
    """Normalize a query string for consistent hashing.

    - Strip leading/trailing whitespace
    - Collapse multiple whitespace to single space
    """
    query = query.strip()
    query = re.sub(r"\s+", " ", query)
    return query


def _serialize_arg(arg: Any) -> Any:
    """Convert an argument to a JSON-serializable form."""
    try:
        json.dumps(arg)
        return arg
    except (TypeError, ValueError):
        return repr(arg)


def make_cache_key(
    query: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    namespace: str | None = None,
) -> str:
    """Generate a SHA-256 cache key from query + args + kwargs + namespace."""
    normalized = normalize_query(query)

    key_parts: dict[str, Any] = {
        "query": normalized,
        "args": [_serialize_arg(a) for a in args],
        "kwargs": {k: _serialize_arg(v) for k, v in sorted(kwargs.items())},
    }
    if namespace:
        key_parts["namespace"] = namespace

    key_str = json.dumps(key_parts, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(key_str.encode()).hexdigest()
