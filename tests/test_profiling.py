"""Tests for the DataProfiler and ProfileReport (Phase 8.3)."""

import json

import pandas as pd
import pytest

from simpleetl.core.profiling import DataProfiler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Alice", None],
            "score": [95.0, 82.0, None, 77.0, 88.0],
            "active": [True, False, True, True, False],
        }
    )


@pytest.fixture
def profiler() -> DataProfiler:
    return DataProfiler(top_n=3)


# ---------------------------------------------------------------------------
# DataProfiler.profile — dataset-level
# ---------------------------------------------------------------------------


class TestDataProfilerDatasetLevel:
    def test_row_count(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        assert report.row_count == 5

    def test_column_count(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        assert report.column_count == 4

    def test_memory_mb_positive(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        assert report.memory_mb > 0

    def test_duplicate_row_count(self, profiler):
        df = pd.DataFrame({"a": [1, 1, 2]})
        report = profiler.profile(df)
        assert report.duplicate_row_count == 1

    def test_no_duplicates(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        assert report.duplicate_row_count == 0

    def test_empty_dataframe(self, profiler):
        df = pd.DataFrame({"x": pd.Series([], dtype="float64")})
        report = profiler.profile(df)
        assert report.row_count == 0
        assert report.column_count == 1
        assert report.duplicate_row_count == 0


# ---------------------------------------------------------------------------
# DataProfiler.profile — column-level
# ---------------------------------------------------------------------------


class TestDataProfilerColumnLevel:
    def test_column_names(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        names = [c.name for c in report.columns]
        assert names == ["id", "name", "score", "active"]

    def test_null_count_numeric(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        score_col = next(c for c in report.columns if c.name == "score")
        assert score_col.null_count == 1

    def test_null_pct_numeric(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        score_col = next(c for c in report.columns if c.name == "score")
        assert abs(score_col.null_pct - 20.0) < 0.01

    def test_null_count_string(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        name_col = next(c for c in report.columns if c.name == "name")
        assert name_col.null_count == 1

    def test_distinct_count(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        name_col = next(c for c in report.columns if c.name == "name")
        # Alice, Bob, Charlie — None not counted
        assert name_col.distinct_count == 3

    def test_numeric_stats(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        score_col = next(c for c in report.columns if c.name == "score")
        assert score_col.mean is not None
        assert abs(score_col.mean - (95 + 82 + 77 + 88) / 4) < 0.01
        assert score_col.min == pytest.approx(77.0)
        assert score_col.max == pytest.approx(95.0)

    def test_no_mean_for_string_column(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        name_col = next(c for c in report.columns if c.name == "name")
        assert name_col.mean is None

    def test_min_max_for_string_column(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        name_col = next(c for c in report.columns if c.name == "name")
        # alphabetical min/max
        assert name_col.min == "Alice"
        assert name_col.max == "Charlie"

    def test_top_values_respected(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        name_col = next(c for c in report.columns if c.name == "name")
        # top_n=3, Alice appears twice
        assert len(name_col.top_values) <= 3
        assert name_col.top_values[0]["value"] == "Alice"
        assert name_col.top_values[0]["count"] == 2

    def test_dtype_recorded(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        id_col = next(c for c in report.columns if c.name == "id")
        assert "int" in id_col.dtype.lower()

    def test_all_null_column(self, profiler):
        df = pd.DataFrame({"x": [None, None, None]})
        report = profiler.profile(df)
        col = report.columns[0]
        assert col.null_count == 3
        assert col.null_pct == 100.0


# ---------------------------------------------------------------------------
# ProfileReport output formats
# ---------------------------------------------------------------------------


class TestProfileReportFormats:
    def test_to_dict_keys(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        d = report.to_dict()
        assert set(d.keys()) >= {
            "row_count",
            "column_count",
            "memory_mb",
            "duplicate_row_count",
            "columns",
        }

    def test_to_dict_columns_list(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        d = report.to_dict()
        assert isinstance(d["columns"], list)
        assert len(d["columns"]) == 4

    def test_to_json_is_valid(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        raw = report.to_json()
        parsed = json.loads(raw)
        assert parsed["row_count"] == 5

    def test_to_markdown_contains_header(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        md = report.to_markdown()
        assert "# Data Profile Report" in md
        assert "Row Count" in md
        assert "score" in md

    def test_to_markdown_has_table(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        md = report.to_markdown()
        # Should contain markdown table separators
        assert "|" in md

    def test_to_html_contains_table(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        html = report.to_html()
        assert "<table" in html
        assert "score" in html

    def test_to_html_valid_structure(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        html = report.to_html()
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_to_dict_roundtrip(self, profiler, sample_df):
        report = profiler.profile(sample_df)
        d = report.to_dict()
        # Ensure we can serialise to JSON without errors
        assert json.loads(json.dumps(d, default=str)) is not None


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestProfilerEdgeCases:
    def test_integer_column_all_unique(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"id": range(100)})
        report = profiler.profile(df)
        col = report.columns[0]
        assert col.distinct_count == 100
        assert abs(col.distinct_pct - 100.0) < 0.01

    def test_float_column_with_inf(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"x": [1.0, 2.0, float("inf"), float("-inf")]})
        # Should not crash
        report = profiler.profile(df)
        assert report.row_count == 4

    def test_single_row_df(self):
        profiler = DataProfiler()
        df = pd.DataFrame({"a": [42], "b": ["hello"]})
        report = profiler.profile(df)
        assert report.row_count == 1
        assert report.duplicate_row_count == 0

    def test_no_columns(self):
        profiler = DataProfiler()
        df = pd.DataFrame()
        report = profiler.profile(df)
        assert report.row_count == 0
        assert report.column_count == 0
        assert report.columns == []
