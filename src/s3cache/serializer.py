from __future__ import annotations

import io
import json
import logging
import pickle
from typing import Any

logger = logging.getLogger(__name__)


def _has_pandas() -> bool:
    try:
        import pandas  # noqa: F401

        return True
    except ImportError:
        return False


def _compress(data: bytes, method: str) -> bytes:
    if method == "zstd":
        import zstandard

        return zstandard.ZstdCompressor().compress(data)
    elif method == "gzip":
        import gzip

        return gzip.compress(data)
    elif method == "none":
        return data
    else:
        raise ValueError(f"Unknown compression method: {method}")


def _decompress(data: bytes, method: str) -> bytes:
    if method == "zstd":
        import zstandard

        return zstandard.ZstdDecompressor().decompress(data)
    elif method == "gzip":
        import gzip

        return gzip.decompress(data)
    elif method == "none":
        return data
    else:
        raise ValueError(f"Unknown compression method: {method}")


def detect_format(obj: Any, requested_format: str = "auto") -> str:
    """Detect the best serialization format for an object."""
    if requested_format != "auto":
        return requested_format

    if _has_pandas():
        import pandas as pd

        if isinstance(obj, pd.DataFrame):
            return "parquet"

    return "pickle"


def serialize(obj: Any, fmt: str = "auto", compression: str = "zstd") -> tuple[bytes, str]:
    """Serialize an object to bytes. Returns (data, format_used)."""
    actual_format = detect_format(obj, fmt)

    if actual_format == "parquet":
        import pandas as pd

        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"parquet format requires a DataFrame, got {type(obj)}")
        buf = io.BytesIO()
        obj.to_parquet(buf, engine="pyarrow")
        raw = buf.getvalue()
    elif actual_format == "json":
        raw = json.dumps(obj, sort_keys=True).encode()
    elif actual_format == "pickle":
        raw = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        raise ValueError(f"Unknown format: {actual_format}")

    compressed = _compress(raw, compression)
    return compressed, actual_format


def deserialize(data: bytes, fmt: str, compression: str = "zstd") -> Any:
    """Deserialize bytes back to a Python object."""
    raw = _decompress(data, compression)

    if fmt == "parquet":
        import pandas as pd

        buf = io.BytesIO(raw)
        return pd.read_parquet(buf, engine="pyarrow")
    elif fmt == "json":
        return json.loads(raw)
    elif fmt == "pickle":
        return pickle.loads(raw)  # noqa: S301
    else:
        raise ValueError(f"Unknown format: {fmt}")
