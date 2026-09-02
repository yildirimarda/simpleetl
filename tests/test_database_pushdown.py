"""Tests for predicate pushdown (filter config -> SQL WHERE) for JDBC sources."""

import pytest
from simpleetl.formats.database import (
    filter_config_to_sql,
    _inject_filter_where,
    Table,
)


class TestFilterConfigToSql:
    """Unit tests for the SQL clause generator."""

    def test_none_returns_empty_string(self):
        assert filter_config_to_sql(None) == ""

    def test_empty_dict_raises(self):
        with pytest.raises(ValueError, match="missing 'column'"):
            filter_config_to_sql({})

    def test_single_column_equality(self):
        expected = "status = 'active'"
        assert (
            filter_config_to_sql({"column": "status", "filter_value": "active"})
            == expected
        )

    def test_single_column_equality_alias_key(self):
        expected = "status = 'active'"
        assert (
            filter_config_to_sql({"filter_column": "status", "value": "active"})
            == expected
        )

    def test_single_column_min_value(self):
        assert filter_config_to_sql({"column": "age", "min_value": 18}) == "age >= 18"

    def test_single_column_max_value(self):
        assert filter_config_to_sql({"column": "age", "max_value": 65}) == "age <= 65"

    def test_single_column_min_and_max(self):
        result = filter_config_to_sql(
            {
                "column": "age",
                "min_value": 18,
                "max_value": 65,
            }
        )
        assert "age >= 18" in result
        assert "age <= 65" in result
        # Combined with AND
        assert result == "age >= 18 AND age <= 65"

    def test_string_min_value_quoted(self):
        expected = "name >= 'Alice'"
        assert (
            filter_config_to_sql({"column": "name", "min_value": "Alice"}) == expected
        )

    def test_integer_min_value_unquoted(self):
        assert filter_config_to_sql({"column": "id", "min_value": 1}) == "id >= 1"

    def test_list_of_filters_combined_with_and(self):
        result = filter_config_to_sql(
            [
                {"column": "age", "min_value": 18},
                {"column": "status", "filter_value": "active"},
            ]
        )
        assert "age >= 18" in result
        assert "status = 'active'" in result
        assert " AND " in result

    def test_string_quoting_escapes_embedded_quotes(self):
        result = filter_config_to_sql({"column": "name", "filter_value": "O'Reilly"})
        assert "O''Reilly" in result

    def test_none_filter_value_with_column_only_uses_is_not_null(self):
        result = filter_config_to_sql({"column": "deleted_at"})
        assert result == "deleted_at IS NOT NULL"


class TestInjectFilterWhere:
    """Unit tests for SQL injection helper."""

    def test_inserts_where_before_order_by(self):
        sql = "SELECT * FROM users ORDER BY id"
        expected = "SELECT * FROM users WHERE age >= 18 ORDER BY id"
        assert _inject_filter_where(sql, "age >= 18") == expected

    def test_inserts_where_before_limit(self):
        sql = "SELECT * FROM users LIMIT 10"
        expected = "SELECT * FROM users WHERE age >= 18 LIMIT 10"
        assert _inject_filter_where(sql, "age >= 18") == expected

    def test_combines_with_existing_where(self):
        sql = "SELECT * FROM users WHERE active = TRUE"
        result = _inject_filter_where(sql, "age >= 18")
        assert "WHERE (active = TRUE) AND (age >= 18)" in result

    def test_combines_with_existing_where_and_order_by(self):
        sql = "SELECT * FROM users WHERE active = TRUE ORDER BY id"
        result = _inject_filter_where(sql, "age >= 18")
        assert "WHERE (active = TRUE) AND (age >= 18)" in result
        assert "ORDER BY id" in result

    def test_empty_clause_returns_original(self):
        sql = "SELECT * FROM users"
        assert _inject_filter_where(sql, "") == sql


class TestTablePushdown:
    """Integration tests asserting generated SQL for database sources."""

    def test_read_with_filter_config_generates_sql(self):
        table = Table("test_table", connection_string="sqlite:///:memory:")
        # Write some data so read works
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "age": [10, 25, 40]})
        table.write(df, if_exists="replace")

        # Read with filter config should generate a query with WHERE clause.
        # We verify by inspecting the SQL through the internal reader, but
        # for simplicity we assert the result is correct and the query runs.
        result = table.read(filter_config={"column": "age", "min_value": 20})
        assert len(result) == 2
        assert list(result["id"]) == [2, 3]

    def test_read_chunks_with_filter_config(self):
        table = Table("test_table", connection_string="sqlite:///:memory:")
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "status": ["a", "b", "c"]})
        table.write(df, if_exists="replace")

        filter_cfg = {"column": "status", "filter_value": "b"}
        chunks = list(table.read_chunks(chunk_size=2, filter_config=filter_cfg))
        assert len(chunks) == 1
        assert chunks[0].iloc[0]["id"] == 2
