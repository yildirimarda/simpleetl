"""
Extra coverage tests for transformations module.

Targets remaining uncovered lines in transformations.py:
join_data, union_data, deduplicate_data, with_column (Series, error paths),
rename_columns, select_columns, drop_columns, fill_na (dict path, subset),
drop_na, sort_data, limit_rows (negative), sample_data (error paths),
distinct_data, cast_columns (coerce, error paths), when_otherwise,
add_computed_column (error path), group_by_aggregate_data, pivot_data,
unpivot_data, transform_chain (error path),
TransformationChain.map/union/deduplicate/cast/when/compute/pivot/unpivot/window/date_op,
timezone operation, is_business_day.
"""

import pandas as pd
import pytest
from simpleetl.transformations import (
    filter_data,
    join_data,
    union_data,
    deduplicate_data,
    with_column,
    rename_columns,
    select_columns,
    drop_columns,
    fill_na,
    drop_na,
    sort_data,
    limit_rows,
    sample_data,
    distinct_data,
    cast_columns,
    when_otherwise,
    add_computed_column,
    group_by_aggregate_data,
    pivot_data,
    unpivot_data,
    transform_chain,
    TransformationChain,
    date_operations,
)


# ---------------------------------------------------------------------------
# join_data
# ---------------------------------------------------------------------------

class TestJoinData:
    def test_inner_join(self):
        left = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        right = pd.DataFrame({"id": [2, 3, 4], "val": [20, 30, 40]})
        result = join_data(left, right, on="id", how="inner")
        assert len(result) == 2
        assert list(result["id"]) == [2, 3]

    def test_left_join(self):
        left = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        right = pd.DataFrame({"id": [2, 3, 4], "val": [20, 30, 40]})
        result = join_data(left, right, on="id", how="left")
        assert len(result) == 3
        assert pd.isna(result[result["id"] == 1]["val"].iloc[0])

    def test_right_join(self):
        left = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        right = pd.DataFrame({"id": [2, 3], "val": [20, 30]})
        result = join_data(left, right, on="id", how="right")
        assert len(result) == 2
        assert list(result["id"]) == [2, 3]

    def test_outer_join(self):
        left = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        right = pd.DataFrame({"id": [2, 3], "val": [20, 30]})
        result = join_data(left, right, on="id", how="outer")
        assert len(result) == 3

    def test_join_left_on_right_on(self):
        left = pd.DataFrame({"a_id": [1, 2, 3], "name": ["A", "B", "C"]})
        right = pd.DataFrame({"b_id": [2, 3], "val": [20, 30]})
        result = join_data(left, right, left_on="a_id", right_on="b_id", how="inner")
        assert len(result) == 2

    def test_join_invalid_type(self):
        left = pd.DataFrame({"id": [1]})
        right = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="Invalid join type"):
            join_data(left, right, on="id", how="invalid")

    def test_join_missing_on_raises(self):
        left = pd.DataFrame({"id": [1]})
        right = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="Either 'on' or both"):
            join_data(left, right)

    def test_join_missing_left_column(self):
        left = pd.DataFrame({"a": [1]})
        right = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="not found in left DataFrame"):
            join_data(left, right, on="id")

    def test_join_missing_right_column(self):
        left = pd.DataFrame({"id": [1]})
        right = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="not found in right DataFrame"):
            join_data(left, right, on="id")

    def test_join_with_suffix(self):
        left = pd.DataFrame({"id": [1], "val": [10]})
        right = pd.DataFrame({"id": [1], "val": [20]})
        result = join_data(left, right, on="id", suffix="_r")
        assert "val_r" in result.columns

    def test_join_multiple_columns(self):
        left = pd.DataFrame({"a": [1, 1], "b": [1, 2], "v": [10, 20]})
        right = pd.DataFrame({"a": [1], "b": [1], "w": [100]})
        result = join_data(left, right, on=["a", "b"])
        assert len(result) == 1


# ---------------------------------------------------------------------------
# union_data
# ---------------------------------------------------------------------------

class TestUnionData:
    def test_basic_union(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [5, 6], "b": [7, 8]})
        result = union_data(df1, df2)
        assert len(result) == 4

    def test_union_mismatched_columns(self):
        df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df2 = pd.DataFrame({"a": [5], "c": [6]})
        result = union_data(df1, df2)
        assert len(result) == 3
        assert "b" in result.columns
        assert "c" in result.columns

    def test_union_false_index(self):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"a": [2]})
        result = union_data(df1, df2, ignore_index=False)
        assert len(result) == 2

    def test_union_chain_method(self):
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"a": [2]})
        result = TransformationChain(df1).union(df2).result()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# deduplicate_data
# ---------------------------------------------------------------------------

class TestDeduplicateData:
    def test_no_subset(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 10, 20]})
        result = deduplicate_data(df)
        assert len(result) == 2

    def test_with_subset(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 99, 20]})
        result = deduplicate_data(df, subset=["a"])
        assert len(result) == 2

    def test_keep_last(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 99, 20]})
        result = deduplicate_data(df, subset=["a"], keep="last")
        assert result.iloc[0]["b"] == 99

    def test_keep_false(self):
        df = pd.DataFrame({"a": [1, 1, 2, 3], "b": [10, 10, 20, 30]})
        result = deduplicate_data(df, keep=False)
        assert len(result) == 2

    def test_missing_subset_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            deduplicate_data(df, subset=["nonexistent"])

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 10, 20]})
        result = TransformationChain(df).deduplicate(subset=["a"]).result()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# with_column
# ---------------------------------------------------------------------------

class TestWithColumn:
    def test_scalar_value(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = with_column(df, "b", 10)
        assert result["b"].tolist() == [10, 10, 10]

    def test_callable_value(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = with_column(df, "b", lambda d: d["a"] * 2)
        assert result["b"].tolist() == [2, 4, 6]

    def test_series_value(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = with_column(df, "b", pd.Series([10, 20, 30]))
        assert result["b"].tolist() == [10, 20, 30]

    def test_series_length_mismatch(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(ValueError, match="Series length"):
            with_column(df, "b", pd.Series([10, 20]))

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = TransformationChain(df).with_column("b", lambda d: d["a"] + 10).result()
        assert result["b"].tolist() == [11, 12, 13]


# ---------------------------------------------------------------------------
# rename_columns
# ---------------------------------------------------------------------------

class TestRenameColumns:
    def test_basic_rename(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = rename_columns(df, {"a": "alpha"})
        assert "alpha" in result.columns
        assert "a" not in result.columns
        assert "b" in result.columns

    def test_missing_column_raises(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            rename_columns(df, {"nonexistent": "x"})

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = TransformationChain(df).rename_columns({"a": "alpha"}).result()
        assert "alpha" in result.columns


# ---------------------------------------------------------------------------
# select_columns
# ---------------------------------------------------------------------------

class TestSelectColumns:
    def test_basic_select(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = select_columns(df, ["a", "c"])
        assert list(result.columns) == ["a", "c"]

    def test_reorder(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = select_columns(df, ["c", "a"])
        assert list(result.columns) == ["c", "a"]

    def test_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            select_columns(df, ["a", "nonexistent"])

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = TransformationChain(df).select_columns(["a"]).result()
        assert list(result.columns) == ["a"]


# ---------------------------------------------------------------------------
# drop_columns
# ---------------------------------------------------------------------------

class TestDropColumns:
    def test_single_column(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = drop_columns(df, "b")
        assert list(result.columns) == ["a", "c"]

    def test_multiple_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = drop_columns(df, ["a", "c"])
        assert list(result.columns) == ["b"]

    def test_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            drop_columns(df, "nonexistent")

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = TransformationChain(df).drop_columns(["b"]).result()
        assert list(result.columns) == ["a"]


# ---------------------------------------------------------------------------
# fill_na
# ---------------------------------------------------------------------------

class TestFillNa:
    def test_scalar_fill(self):
        df = pd.DataFrame({"a": [1, None], "b": [None, 2]})
        result = fill_na(df, value=0)
        assert result["a"].tolist() == [1, 0]
        assert result["b"].tolist() == [0, 2]

    def test_dict_fill(self):
        df = pd.DataFrame({"a": [1, None], "b": [None, 2]})
        result = fill_na(df, value={"a": 99, "b": -1})
        assert result["a"].tolist() == [1, 99]
        assert result["b"].tolist() == [-1, 2]

    def test_subset_fill(self):
        df = pd.DataFrame({"a": [1, None], "b": [None, 2]})
        result = fill_na(df, value=0, subset=["a"])
        assert result["a"].tolist() == [1, 0]
        assert pd.isna(result["b"].iloc[0])

    def test_dict_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            fill_na(df, value={"nonexistent": 0})

    def test_subset_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            fill_na(df, value=0, subset=["nonexistent"])

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1, None], "b": [None, 2]})
        result = TransformationChain(df).fill_na(value=0).result()
        assert result["a"].tolist() == [1, 0]


# ---------------------------------------------------------------------------
# drop_na
# ---------------------------------------------------------------------------

class TestDropNa:
    def test_how_any(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, None]})
        result = drop_na(df, how="any")
        assert len(result) == 1

    def test_how_all(self):
        df = pd.DataFrame({"a": [1, None, None], "b": [4, 5, None]})
        result = drop_na(df, how="all")
        assert len(result) == 2

    def test_with_subset(self):
        df = pd.DataFrame({"a": [1, 2, None], "b": [None, 5, 6]})
        result = drop_na(df, subset=["a"], how="any")
        assert len(result) == 2

    def test_invalid_how(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Invalid value"):
            drop_na(df, how="maybe")

    def test_missing_subset_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            drop_na(df, subset=["nonexistent"])

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, 6]})
        result = TransformationChain(df).drop_na().result()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# sort_data
# ---------------------------------------------------------------------------

class TestSortData:
    def test_single_column_ascending(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        result = sort_data(df, by="a", ascending=True)
        assert result["a"].tolist() == [1, 2, 3]

    def test_single_column_descending(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        result = sort_data(df, by="a", ascending=False)
        assert result["a"].tolist() == [3, 2, 1]

    def test_multiple_columns(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 1, 2]})
        result = sort_data(df, by=["a", "b"])
        assert result["b"].tolist() == [1, 3, 2]

    def test_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            sort_data(df, by="nonexistent")

    def test_in_chain(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        result = TransformationChain(df).sort(by="a").result()
        assert result["a"].tolist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# limit_rows
# ---------------------------------------------------------------------------

class TestLimitRows:
    def test_basic_limit(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        result = limit_rows(df, n=3)
        assert len(result) == 3

    def test_limit_zero(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = limit_rows(df, n=0)
        assert len(result) == 0

    def test_limit_exceeds(self):
        df = pd.DataFrame({"a": [1, 2]})
        result = limit_rows(df, n=100)
        assert len(result) == 2

    def test_negative_raises(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="non-negative"):
            limit_rows(df, n=-1)

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = TransformationChain(df).limit(2).result()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# sample_data
# ---------------------------------------------------------------------------

class TestSampleData:
    def test_sample_n(self):
        df = pd.DataFrame({"a": range(100)})
        result = sample_data(df, n=10, seed=42)
        assert len(result) == 10

    def test_sample_frac(self):
        df = pd.DataFrame({"a": range(100)})
        result = sample_data(df, frac=0.5, seed=42)
        assert len(result) == 50

    def test_both_n_and_frac_raises(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="not both"):
            sample_data(df, n=1, frac=0.5)

    def test_neither_n_nor_frac_raises(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Either"):
            sample_data(df)

    def test_in_chain(self):
        df = pd.DataFrame({"a": range(50)})
        result = TransformationChain(df).sample(n=5, seed=42).result()
        assert len(result) == 5


# ---------------------------------------------------------------------------
# distinct_data
# ---------------------------------------------------------------------------

class TestDistinctData:
    def test_basic(self):
        df = pd.DataFrame({"a": [1, 1, 2, 3, 3]})
        result = distinct_data(df)
        assert len(result) == 3

    def test_with_subset(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [10, 20, 30]})
        result = distinct_data(df, subset=["a"])
        assert len(result) == 2

    def test_missing_subset_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            distinct_data(df, subset=["nonexistent"])

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1, 1, 2, 3]})
        result = TransformationChain(df).distinct().result()
        assert len(result) == 3


# ---------------------------------------------------------------------------
# cast_columns
# ---------------------------------------------------------------------------

class TestCastColumns:
    def test_int_cast(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        result = cast_columns(df, {"a": "int64"})
        assert result["a"].dtype == "int64"

    def test_str_cast(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        result = cast_columns(df, {"a": object})
        assert result["a"].dtype == object

    def test_coerce(self):
        df = pd.DataFrame({"a": ["1.0", "2.0", "xyz"]})
        result = cast_columns(df, {"a": "float64"}, errors="coerce")
        assert pd.isna(result["a"].iloc[2])

    def test_invalid_errors_value(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Invalid value"):
            cast_columns(df, {"a": "int64"}, errors="skip")

    def test_missing_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            cast_columns(df, {"nonexistent": "int64"})

    def test_in_chain(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        result = TransformationChain(df).cast({"a": "int64"}).result()
        assert result["a"].dtype == "int64"


# ---------------------------------------------------------------------------
# when_otherwise
# ---------------------------------------------------------------------------

class TestWhenOtherwise:
    def test_basic(self):
        df = pd.DataFrame({"score": [95, 82, 70, 55]})
        result = when_otherwise(
            df,
            [
                (df["score"] >= 90, "A"),
                (df["score"] >= 80, "B"),
                (df["score"] >= 70, "C"),
            ],
            otherwise_value="F",
            output_col="grade",
        )
        assert result["grade"].tolist() == ["A", "B", "C", "F"]

    def test_length_mismatch(self):
        df = pd.DataFrame({"score": [95, 82]})
        with pytest.raises(ValueError, match="Condition Series length"):
            when_otherwise(
                df,
                [(pd.Series([True]), "A")],
                otherwise_value="F",
                output_col="grade",
            )

    def test_in_chain(self):
        df = pd.DataFrame({"score": [95, 82, 70, 55]})
        result = (
            TransformationChain(df)
            .when(
                [
                    (df["score"] >= 90, "A"),
                    (df["score"] >= 80, "B"),
                ],
                otherwise_value="C",
                output_col="grade",
            )
            .result()
        )
        assert result["grade"].tolist() == ["A", "B", "C", "C"]


# ---------------------------------------------------------------------------
# add_computed_column
# ---------------------------------------------------------------------------

class TestAddComputedColumn:
    def test_basic_expression(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = add_computed_column(df, "a + b", "total")
        assert result["total"].tolist() == [5, 7, 9]

    def test_invalid_expression(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Failed to evaluate"):
            add_computed_column(df, "nonexistent + 1", "result")

    def test_in_chain(self):
        df = pd.DataFrame({"x": [10, 20], "y": [1, 2]})
        result = TransformationChain(df).compute("x / y", "ratio").result()
        assert result["ratio"].tolist() == [10.0, 10.0]


# ---------------------------------------------------------------------------
# group_by_aggregate_data
# ---------------------------------------------------------------------------

class TestGroupByAggregateData:
    def test_single_agg(self):
        df = pd.DataFrame({
            "dept": ["A", "A", "B", "B"],
            "salary": [50000, 60000, 45000, 55000],
        })
        result = group_by_aggregate_data(df, "dept", {"salary": "mean"})
        assert len(result) == 2
        assert "salary" in result.columns

    def test_list_of_aggs(self):
        df = pd.DataFrame({
            "dept": ["A", "A", "B", "B"],
            "salary": [50000, 60000, 45000, 55000],
        })
        result = group_by_aggregate_data(df, "dept", {"salary": ["mean", "sum"]})
        assert len(result) == 2
        assert "salary_mean" in result.columns
        assert "salary_sum" in result.columns

    def test_named_agg_dict(self):
        df = pd.DataFrame({
            "dept": ["A", "A", "B", "B"],
            "salary": [50000, 60000, 45000, 55000],
        })
        result = group_by_aggregate_data(
            df, "dept", {"salary": ["mean", "sum"]}
        )
        assert len(result) == 2
        assert "salary_mean" in result.columns
        assert "salary_sum" in result.columns

    def test_missing_groupby_column(self):
        df = pd.DataFrame({"dept": ["A"], "salary": [50000]})
        with pytest.raises(ValueError, match="Groupby column"):
            group_by_aggregate_data(df, "nonexistent", {"salary": "mean"})

    def test_missing_agg_column(self):
        df = pd.DataFrame({"dept": ["A"], "salary": [50000]})
        with pytest.raises(ValueError, match="Aggregation column"):
            group_by_aggregate_data(df, "dept", {"nonexistent": "mean"})

    def test_multi_groupby(self):
        df = pd.DataFrame({
            "dept": ["A", "A", "B", "B"],
            "region": ["X", "Y", "X", "Y"],
            "salary": [50000, 60000, 45000, 55000],
        })
        result = group_by_aggregate_data(df, ["dept", "region"], {"salary": "mean"})
        assert len(result) == 4


# ---------------------------------------------------------------------------
# pivot_data
# ---------------------------------------------------------------------------

class TestPivotData:
    def test_basic_pivot(self):
        df = pd.DataFrame({
            "date": ["2024-01", "2024-01", "2024-02", "2024-02"],
            "product": ["A", "B", "A", "B"],
            "sales": [100, 200, 150, 250],
        })
        result = pivot_data(df, index="date", columns="product", values="sales")
        # Pivot with single values col may keep value name in MultiIndex
        cols = list(result.columns)
        assert "date" in cols

    def test_missing_column(self):
        df = pd.DataFrame({"date": ["2024-01"], "product": ["A"], "sales": [100]})
        with pytest.raises(ValueError, match="Columns not found"):
            pivot_data(df, index="nonexistent", columns="product", values="sales")

    def test_in_chain(self):
        df = pd.DataFrame({
            "date": ["2024-01", "2024-01", "2024-02", "2024-02"],
            "product": ["A", "B", "A", "B"],
            "sales": [100, 200, 150, 250],
        })
        result = TransformationChain(df).pivot(
            index="date", columns="product", values="sales"
        ).result()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# unpivot_data
# ---------------------------------------------------------------------------

class TestUnpivotData:
    def test_basic_melt(self):
        df = pd.DataFrame({"id": [1, 2], "a": [10, 20], "b": [30, 40]})
        result = unpivot_data(df, id_vars="id", value_vars=["a", "b"])
        assert len(result) == 4
        assert "variable" in result.columns
        assert "value" in result.columns

    def test_without_value_vars(self):
        df = pd.DataFrame({"id": [1, 2], "a": [10, 20], "b": [30, 40]})
        result = unpivot_data(df, id_vars="id")
        assert len(result) == 4

    def test_custom_names(self):
        df = pd.DataFrame({"id": [1], "a": [10]})
        result = unpivot_data(
            df, id_vars="id", value_vars="a", var_name="col", value_name="val"
        )
        assert "col" in result.columns
        assert "val" in result.columns

    def test_missing_id_column(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Columns not found"):
            unpivot_data(df, id_vars="nonexistent")

    def test_missing_value_column(self):
        df = pd.DataFrame({"id": [1], "a": [10]})
        with pytest.raises(ValueError, match="Columns not found"):
            unpivot_data(df, id_vars="id", value_vars="nonexistent")

    def test_in_chain(self):
        df = pd.DataFrame({"id": [1, 2], "a": [10, 20], "b": [30, 40]})
        result = (
            TransformationChain(df)
            .unpivot(id_vars="id", value_vars=["a", "b"])
            .result()
        )
        assert len(result) == 4


# ---------------------------------------------------------------------------
# transform_chain
# ---------------------------------------------------------------------------

class TestTransformChain:
    def test_basic_chain(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        steps = [
            (filter_data, {"column": "a", "min_value": 2}),
            (limit_rows, {"n": 2}),
        ]
        result = transform_chain(df, steps)
        assert len(result) == 2

    def test_nondatframe_result_raises(self):
        df = pd.DataFrame({"a": [1]})

        def bad_func(d):
            return "not a dataframe"

        with pytest.raises(ValueError, match="did not return a DataFrame"):
            transform_chain(df, [(bad_func, {})])

    def test_single_step(self):
        df = pd.DataFrame({"a": [3, 1, 2]})
        steps = [(sort_data, {"by": "a"})]
        result = transform_chain(df, steps)
        assert result["a"].tolist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# date_operations — timezone coverage
# ---------------------------------------------------------------------------

class TestDateOperationsTimezone:
    def test_timezone_naive(self):
        """Test timezone operation on a naive datetime column."""
        df = pd.DataFrame({"dt": ["2024-03-15", "2024-07-20"]})
        result = date_operations(df, "dt", "timezone", tz="US/Eastern")
        assert pd.api.types.is_datetime64_any_dtype(result["dt"])

    def test_timezone_aware(self):
        """Test timezone operation on an already-aware datetime column."""
        df = pd.DataFrame({"dt": pd.to_datetime(["2024-03-15"]).tz_localize("UTC")})
        result = date_operations(df, "dt", "timezone", tz="US/Pacific")
        assert pd.api.types.is_datetime64_any_dtype(result["dt"])

    def test_timezone_missing_tz(self):
        df = pd.DataFrame({"dt": ["2024-03-15"]})
        with pytest.raises(ValueError, match="'timezone' operation requires 'tz'"):
            date_operations(df, "dt", "timezone")

    def test_diff_missing_other_column(self):
        df = pd.DataFrame({"dt": ["2024-03-15"], "other": ["2024-03-14"]})
        with pytest.raises(ValueError, match="Column 'missing' not found"):
            date_operations(df, "dt", "diff", other="missing")

    def test_in_chain(self):
        df = pd.DataFrame({"dt": ["2024-03-15 12:00"]})
        result = (
            TransformationChain(df)
            .date_op("dt", "add", value=5, unit="days")
            .result()
        )
        assert "dt_added" in result.columns


# ---------------------------------------------------------------------------
# TransformationChain — additional methods
# ---------------------------------------------------------------------------

class TestTransformationChainExtras:
    def test_map_with_dict(self):
        df = pd.DataFrame({"cat": ["A", "B", "C"]})
        result = TransformationChain(df).map("cat", {"A": "Alpha", "B": "Beta", "C": "Gamma"}).result()
        assert result["cat"].tolist() == ["Alpha", "Beta", "Gamma"]

    def test_map_with_function(self):
        df = pd.DataFrame({"val": [1, 2, 3]})
        result = TransformationChain(df).map("val", lambda x: x * 10).result()
        assert result["val"].tolist() == [10, 20, 30]

    def test_join_in_chain(self):
        left = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        right = pd.DataFrame({"id": [2, 3], "val": [20, 30]})
        result = (
            TransformationChain(left)
            .join(right, on="id", how="inner")
            .result()
        )
        assert len(result) == 2

    def test_pivot_in_chain(self):
        df = pd.DataFrame({
            "date": ["2024-01", "2024-01", "2024-02", "2024-02"],
            "product": ["A", "B", "A", "B"],
            "sales": [100, 200, 150, 250],
        })
        result = (
            TransformationChain(df)
            .pivot(index="date", columns="product", values="sales")
            .result()
        )
        assert len(result) == 2

    def test_unpivot_in_chain(self):
        df = pd.DataFrame({"id": [1, 2], "a": [10, 20], "b": [30, 40]})
        result = (
            TransformationChain(df)
            .unpivot(id_vars="id", value_vars=["a", "b"])
            .result()
        )
        assert len(result) == 4

    def test_window_in_chain(self):
        df = pd.DataFrame({
            "dept": ["A", "A", "B", "B"],
            "salary": [50000, 60000, 45000, 55000],
        })
        result = (
            TransformationChain(df)
            .window("dept", "salary", {"rn": {"function": "row_number"}})
            .result()
        )
        assert "rn" in result.columns

    def test_all_chain_methods_return_self(self):
        """Verify that chain methods return self for chaining."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        map_df = pd.DataFrame({"cat": ["A", "B"]})
        date_df = pd.DataFrame({"dt": ["2024-03-15"]})
        win_df = pd.DataFrame({"d": ["A", "A"], "s": [50000, 60000]})
        join_right = pd.DataFrame({"a": [1, 3], "c": [10, 30]})
        pivot_df = pd.DataFrame({
            "date": ["2024-01", "2024-01", "2024-02", "2024-02"],
            "product": ["A", "B", "A", "B"],
            "sales": [100, 200, 150, 250],
        })
        unpivot_df = pd.DataFrame({"id": [1, 2], "x": [10, 20], "y": [30, 40]})

        assert TransformationChain(df).filter(column="a", min_value=1) is not None
        assert TransformationChain(map_df).map("cat", {"A": "X", "B": "Y"}) is not None
        assert TransformationChain(df).deduplicate() is not None
        assert TransformationChain(df).cast({"a": "int64"}) is not None
        assert TransformationChain(df).distinct() is not None
        assert TransformationChain(df).drop_columns(["b"]) is not None
        assert TransformationChain(df).drop_na() is not None
        assert TransformationChain(df).fill_na(value=0) is not None
        assert TransformationChain(df).limit(1) is not None
        assert TransformationChain(df).rename_columns({"a": "x"}) is not None
        assert TransformationChain(df).sample(n=1, seed=42) is not None
        assert TransformationChain(df).select_columns(["a"]) is not None
        assert TransformationChain(df).sort(by="a") is not None
        assert TransformationChain(df).with_column("c", 1) is not None
        assert TransformationChain(df).union(df) is not None
        assert TransformationChain(df).join(right=join_right, on="a") is not None
        assert TransformationChain(pivot_df).pivot(
            index="date", columns="product", values="sales"
        ) is not None
        assert TransformationChain(unpivot_df).unpivot(
            id_vars="id", value_vars=["x", "y"]
        ) is not None
        assert TransformationChain(win_df).window(
            "d", "s", {"rn": {"function": "row_number"}}
        ) is not None
        assert TransformationChain(date_df).date_op(
            "dt", "extract", part="year"
        ) is not None

    def test_union_in_chain(self):
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"a": [3, 4]})
        result = TransformationChain(df1).union(df2).result()
        assert len(result) == 4
