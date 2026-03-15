from __future__ import annotations

from s3cache.key import make_cache_key, normalize_query


class TestNormalizeQuery:
    def test_strips_whitespace(self) -> None:
        assert normalize_query("  SELECT 1  ") == "SELECT 1"

    def test_collapses_whitespace(self) -> None:
        assert normalize_query("SELECT   *   FROM   t") == "SELECT * FROM t"

    def test_preserves_case(self) -> None:
        result = normalize_query("SELECT Name FROM Users")
        assert result == "SELECT Name FROM Users"

    def test_preserves_string_literals(self) -> None:
        result = normalize_query("SELECT * FROM t WHERE name = 'Alice'")
        assert result == "SELECT * FROM t WHERE name = 'Alice'"


class TestMakeCacheKey:
    def test_same_query_same_hash(self) -> None:
        h1 = make_cache_key("SELECT 1", (), {})
        h2 = make_cache_key("SELECT 1", (), {})
        assert h1 == h2

    def test_different_query_different_hash(self) -> None:
        h1 = make_cache_key("SELECT 1", (), {})
        h2 = make_cache_key("SELECT 2", (), {})
        assert h1 != h2

    def test_whitespace_insensitive(self) -> None:
        h1 = make_cache_key("SELECT  *  FROM  t", (), {})
        h2 = make_cache_key("SELECT * FROM t", (), {})
        assert h1 == h2

    def test_case_sensitive(self) -> None:
        h1 = make_cache_key("select * from t", (), {})
        h2 = make_cache_key("SELECT * FROM t", (), {})
        assert h1 != h2

    def test_args_affect_hash(self) -> None:
        h1 = make_cache_key("SELECT 1", (1,), {})
        h2 = make_cache_key("SELECT 1", (2,), {})
        assert h1 != h2

    def test_kwargs_affect_hash(self) -> None:
        h1 = make_cache_key("SELECT 1", (), {"limit": 10})
        h2 = make_cache_key("SELECT 1", (), {"limit": 20})
        assert h1 != h2

    def test_namespace_affects_hash(self) -> None:
        h1 = make_cache_key("SELECT 1", (), {}, namespace="a")
        h2 = make_cache_key("SELECT 1", (), {}, namespace="b")
        assert h1 != h2

    def test_returns_hex_string(self) -> None:
        h = make_cache_key("SELECT 1", (), {})
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex
        int(h, 16)  # must be valid hex

    def test_generic_input(self) -> None:
        """Works with any string, not just SQL."""
        h1 = make_cache_key("get_user_profile", ("user_123",), {})
        h2 = make_cache_key("get_user_profile", ("user_456",), {})
        assert h1 != h2
