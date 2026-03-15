from __future__ import annotations

import pytest

from s3cache.serializer import deserialize, detect_format, serialize


class TestDetectFormat:
    def test_auto_dict_returns_pickle(self) -> None:
        assert detect_format({"a": 1}) == "pickle"

    def test_auto_list_returns_pickle(self) -> None:
        assert detect_format([1, 2, 3]) == "pickle"

    def test_explicit_format_respected(self) -> None:
        assert detect_format({"a": 1}, "json") == "json"

    def test_auto_dataframe_returns_parquet(self) -> None:
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2]})
        assert detect_format(df) == "parquet"


class TestSerializeDeserialize:
    def test_pickle_roundtrip(self) -> None:
        obj = {"key": "value", "nums": [1, 2, 3]}
        data, fmt = serialize(obj, "pickle", "zstd")
        assert fmt == "pickle"
        result = deserialize(data, fmt, "zstd")
        assert result == obj

    def test_json_roundtrip(self) -> None:
        obj = {"key": "value", "nums": [1, 2, 3]}
        data, fmt = serialize(obj, "json", "zstd")
        assert fmt == "json"
        result = deserialize(data, fmt, "zstd")
        assert result == obj

    def test_parquet_roundtrip(self) -> None:
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        data, fmt = serialize(df, "parquet", "zstd")
        assert fmt == "parquet"
        result = deserialize(data, fmt, "zstd")
        pd.testing.assert_frame_equal(result, df)

    def test_auto_format_dataframe(self) -> None:
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({"a": [1]})
        data, fmt = serialize(df, "auto", "zstd")
        assert fmt == "parquet"

    def test_auto_format_dict(self) -> None:
        data, fmt = serialize({"a": 1}, "auto", "zstd")
        assert fmt == "pickle"

    def test_gzip_compression(self) -> None:
        obj = {"test": "data"}
        data, fmt = serialize(obj, "pickle", "gzip")
        result = deserialize(data, fmt, "gzip")
        assert result == obj

    def test_no_compression(self) -> None:
        obj = {"test": "data"}
        data, fmt = serialize(obj, "pickle", "none")
        result = deserialize(data, fmt, "none")
        assert result == obj
