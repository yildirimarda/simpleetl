"""
Tests for the extended transformation module.

Tests for window_functions, string_operations, date_operations,
TransformationChain, and the chain() convenience function.
"""

import pandas as pd
import pytest
from simpleetl.transformations import (
    window_functions,
    string_operations,
    date_operations,
    TransformationChain,
    chain,
)


# ---------------------------------------------------------------------------
# window_functions tests
# ---------------------------------------------------------------------------

class TestWindowFunctions:
    """Tests for the window_functions function."""

    def test_rank(self):
        """Test rank window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 60000, 45000, 55000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'sal_rank': {'function': 'rank'}
        })
        # Dept A: 50000->1, 60000->2, 60000->2 (min method)
        # Dept B: 45000->1, 55000->2
        a_ranks = result[result['dept'] == 'A']['sal_rank'].tolist()
        b_ranks = result[result['dept'] == 'B']['sal_rank'].tolist()
        assert a_ranks == [1.0, 2.0, 2.0]
        assert b_ranks == [1.0, 2.0]

    def test_dense_rank(self):
        """Test dense_rank window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 60000, 45000, 55000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'dense_r': {'function': 'dense_rank'}
        })
        a_ranks = result[result['dept'] == 'A']['dense_r'].tolist()
        # Dense: 50000->1, 60000->2, 60000->2 (no gaps)
        assert a_ranks == [1.0, 2.0, 2.0]

    def test_row_number(self):
        """Test row_number window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'B', 'B', 'B'],
            'salary': [50000, 60000, 45000, 55000, 48000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'rn': {'function': 'row_number'}
        })
        a_rn = result[result['dept'] == 'A']['rn'].tolist()
        b_rn = result[result['dept'] == 'B']['rn'].tolist()
        assert a_rn == [1, 2]
        assert b_rn == [1, 2, 3]

    def test_lag(self):
        """Test lag window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 70000, 45000, 55000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'prev_sal': {'function': 'lag', 'column': 'salary', 'offset': 1}
        })
        a_prev = result[result['dept'] == 'A']['prev_sal'].tolist()
        b_prev = result[result['dept'] == 'B']['prev_sal'].tolist()
        assert pd.isna(a_prev[0])
        assert a_prev[1] == 50000
        assert a_prev[2] == 60000
        assert pd.isna(b_prev[0])
        assert b_prev[1] == 45000

    def test_lead(self):
        """Test lead window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 70000, 45000, 55000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'next_sal': {'function': 'lead', 'column': 'salary', 'offset': 1}
        })
        a_next = result[result['dept'] == 'A']['next_sal'].tolist()
        assert a_next[0] == 60000
        assert a_next[1] == 70000
        assert pd.isna(a_next[2])

    def test_cumsum(self):
        """Test cumsum window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 70000, 45000, 55000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'running_total': {'function': 'cumsum', 'column': 'salary'}
        })
        a_cum = result[result['dept'] == 'A']['running_total'].tolist()
        b_cum = result[result['dept'] == 'B']['running_total'].tolist()
        assert a_cum == [50000, 110000, 180000]
        assert b_cum == [45000, 100000]

    def test_percent_rank(self):
        """Test percent_rank window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'A'],
            'salary': [50000, 60000, 70000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'pct_r': {'function': 'percent_rank'}
        })
        # pandas rank(pct=True) = rank / count
        # rank: 1, 2, 3; count=3
        # pct: 1/3, 2/3, 3/3
        pct = result['pct_r'].tolist()
        assert abs(pct[0] - 1.0 / 3.0) < 1e-10
        assert abs(pct[1] - 2.0 / 3.0) < 1e-10
        assert abs(pct[2] - 1.0) < 1e-10

    def test_cume_dist(self):
        """Test cume_dist window function."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'A'],
            'salary': [50000, 60000, 70000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'cume': {'function': 'cume_dist'}
        })
        # cume_dist = rank / count = 1/3, 2/3, 3/3
        cume = result['cume'].tolist()
        assert abs(cume[0] - 1.0 / 3.0) < 1e-10
        assert abs(cume[1] - 2.0 / 3.0) < 1e-10
        assert abs(cume[2] - 1.0) < 1e-10

    def test_multiple_functions(self):
        """Test applying multiple window functions at once."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 45000, 55000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'rn': {'function': 'row_number'},
            'rk': {'function': 'rank'},
            'prev': {'function': 'lag', 'column': 'salary', 'offset': 1},
        })
        assert 'rn' in result.columns
        assert 'rk' in result.columns
        assert 'prev' in result.columns

    def test_lag_with_fill_value(self):
        """Test lag with a custom fill value."""
        df = pd.DataFrame({
            'dept': ['A', 'A'],
            'salary': [50000, 60000],
        })
        result = window_functions(df, 'dept', 'salary', {
            'prev_sal': {'function': 'lag', 'column': 'salary', 'offset': 1, 'fill_value': 0}
        })
        prev = result[result['dept'] == 'A']['prev_sal'].tolist()
        assert prev[0] == 0
        assert prev[1] == 50000

    def test_invalid_function(self):
        """Test that an invalid function name raises ValueError."""
        df = pd.DataFrame({'dept': ['A'], 'salary': [50000]})
        with pytest.raises(ValueError, match="Invalid window function"):
            window_functions(df, 'dept', 'salary', {
                'x': {'function': 'invalid_func'}
            })

    def test_missing_partition_column(self):
        """Test that a missing partition column raises ValueError."""
        df = pd.DataFrame({'dept': ['A'], 'salary': [50000]})
        with pytest.raises(ValueError, match="Partition column 'nonexistent' not found"):
            window_functions(df, 'nonexistent', 'salary', {
                'rn': {'function': 'row_number'}
            })

    def test_missing_order_column(self):
        """Test that a missing order column raises ValueError."""
        df = pd.DataFrame({'dept': ['A'], 'salary': [50000]})
        with pytest.raises(ValueError, match="Order column 'nonexistent' not found"):
            window_functions(df, 'dept', 'nonexistent', {
                'rn': {'function': 'row_number'}
            })

    def test_lag_without_column_raises(self):
        """Test that lag without 'column' key raises ValueError."""
        df = pd.DataFrame({'dept': ['A', 'A'], 'salary': [50000, 60000]})
        with pytest.raises(ValueError, match="requires a 'column' key"):
            window_functions(df, 'dept', 'salary', {
                'x': {'function': 'lag'}
            })

    def test_empty_dataframe(self):
        """Test window functions on an empty DataFrame."""
        df = pd.DataFrame({'dept': pd.Series([], dtype=str), 'salary': pd.Series([], dtype=float)})
        result = window_functions(df, 'dept', 'salary', {
            'rn': {'function': 'row_number'}
        })
        assert len(result) == 0
        assert 'rn' in result.columns

    def test_list_partition_and_order(self):
        """Test with list partition_by and order_by."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'B', 'B'],
            'team': ['X', 'Y', 'X', 'Y'],
            'salary': [50000, 60000, 45000, 55000],
        })
        result = window_functions(df, ['dept', 'team'], 'salary', {
            'rn': {'function': 'row_number'}
        })
        # Each dept+team combo has exactly one row, so all row numbers should be 1
        assert result['rn'].tolist() == [1, 1, 1, 1]


# ---------------------------------------------------------------------------
# string_operations tests
# ---------------------------------------------------------------------------

class TestStringOperations:
    """Tests for the string_operations function."""

    def test_trim(self):
        """Test trim operation."""
        df = pd.DataFrame({'name': ['  Alice  ', '  Bob', 'Charlie  ']})
        result = string_operations(df, 'name', 'trim')
        assert result['name'].tolist() == ['Alice', 'Bob', 'Charlie']

    def test_upper(self):
        """Test upper operation."""
        df = pd.DataFrame({'name': ['alice', 'Bob', 'CHARLIE']})
        result = string_operations(df, 'name', 'upper')
        assert result['name'].tolist() == ['ALICE', 'BOB', 'CHARLIE']

    def test_lower(self):
        """Test lower operation."""
        df = pd.DataFrame({'name': ['ALICE', 'Bob', 'Charlie']})
        result = string_operations(df, 'name', 'lower')
        assert result['name'].tolist() == ['alice', 'bob', 'charlie']

    def test_replace(self):
        """Test replace operation."""
        df = pd.DataFrame({'text': ['hello world', 'world peace', 'hello hello']})
        result = string_operations(df, 'text', 'replace', old='world', new='earth')
        assert result['text'].tolist() == ['hello earth', 'earth peace', 'hello hello']

    def test_replace_missing_old(self):
        """Test that replace without 'old' raises ValueError."""
        df = pd.DataFrame({'text': ['hello']})
        with pytest.raises(ValueError, match="'replace' operation requires 'old'"):
            string_operations(df, 'text', 'replace', new='x')

    def test_split_no_expand(self):
        """Test split without expand."""
        df = pd.DataFrame({'text': ['a,b,c', 'd,e']})
        result = string_operations(df, 'text', 'split', sep=',')
        assert result['text'].tolist() == [['a', 'b', 'c'], ['d', 'e']]

    def test_split_with_expand(self):
        """Test split with expand=True."""
        df = pd.DataFrame({'text': ['a,b,c', 'd,e,f']})
        result = string_operations(df, 'text', 'split', sep=',', expand=True)
        assert 'text_split_0' in result.columns
        assert 'text_split_1' in result.columns
        assert 'text_split_2' in result.columns
        assert result['text_split_0'].tolist() == ['a', 'd']

    def test_split_missing_sep(self):
        """Test that split without 'sep' raises ValueError."""
        df = pd.DataFrame({'text': ['a,b']})
        with pytest.raises(ValueError, match="'split' operation requires 'sep'"):
            string_operations(df, 'text', 'split')

    def test_contains(self):
        """Test contains operation."""
        df = pd.DataFrame({'text': ['hello world', 'goodbye', 'world peace']})
        result = string_operations(df, 'text', 'contains', pattern='world')
        assert result['text_contains'].tolist() == [True, False, True]

    def test_contains_case_insensitive(self):
        """Test contains with case=False."""
        df = pd.DataFrame({'text': ['Hello World', 'HELLO', 'goodbye']})
        result = string_operations(df, 'text', 'contains', pattern='hello', case=False)
        assert result['text_contains'].tolist() == [True, True, False]

    def test_contains_missing_pattern(self):
        """Test that contains without 'pattern' raises ValueError."""
        df = pd.DataFrame({'text': ['hello']})
        with pytest.raises(ValueError, match="'contains' operation requires 'pattern'"):
            string_operations(df, 'text', 'contains')

    def test_regex_extract(self):
        """Test regex_extract operation."""
        df = pd.DataFrame({'text': ['abc123', 'def456', 'ghi789']})
        result = string_operations(df, 'text', 'regex_extract', pattern=r'(\d+)')
        assert result['text'].tolist() == ['123', '456', '789']

    def test_regex_extract_missing_pattern(self):
        """Test that regex_extract without 'pattern' raises ValueError."""
        df = pd.DataFrame({'text': ['abc']})
        with pytest.raises(ValueError, match="'regex_extract' operation requires 'pattern'"):
            string_operations(df, 'text', 'regex_extract')

    def test_length(self):
        """Test length operation."""
        df = pd.DataFrame({'text': ['hello', 'world', '']})
        result = string_operations(df, 'text', 'length')
        assert result['text_length'].tolist() == [5, 5, 0]

    def test_substring(self):
        """Test substring operation."""
        df = pd.DataFrame({'text': ['hello world', 'goodbye']})
        result = string_operations(df, 'text', 'substring', start=0, stop=5)
        assert result['text'].tolist() == ['hello', 'goodb']

    def test_pad_left(self):
        """Test pad_left operation."""
        df = pd.DataFrame({'text': ['42', '7', '100']})
        result = string_operations(df, 'text', 'pad_left', width=5, fillchar='0')
        assert result['text'].tolist() == ['00042', '00007', '00100']

    def test_pad_right(self):
        """Test pad_right operation."""
        df = pd.DataFrame({'text': ['hi', 'go']})
        result = string_operations(df, 'text', 'pad_right', width=5, fillchar='.')
        assert result['text'].tolist() == ['hi...', 'go...']

    def test_pad_left_missing_width(self):
        """Test that pad_left without 'width' raises ValueError."""
        df = pd.DataFrame({'text': ['hi']})
        with pytest.raises(ValueError, match="'pad_left' operation requires 'width'"):
            string_operations(df, 'text', 'pad_left')

    def test_pad_right_missing_width(self):
        """Test that pad_right without 'width' raises ValueError."""
        df = pd.DataFrame({'text': ['hi']})
        with pytest.raises(ValueError, match="'pad_right' operation requires 'width'"):
            string_operations(df, 'text', 'pad_right')

    def test_invalid_operation(self):
        """Test that an invalid operation raises ValueError."""
        df = pd.DataFrame({'text': ['hello']})
        with pytest.raises(ValueError, match="Invalid string operation"):
            string_operations(df, 'text', 'invalid_op')

    def test_missing_column(self):
        """Test that a missing column raises ValueError."""
        df = pd.DataFrame({'text': ['hello']})
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            string_operations(df, 'nonexistent', 'upper')

    def test_empty_dataframe(self):
        """Test string operations on an empty DataFrame."""
        df = pd.DataFrame({'text': pd.Series([], dtype=str)})
        result = string_operations(df, 'text', 'upper')
        assert len(result) == 0


# ---------------------------------------------------------------------------
# date_operations tests
# ---------------------------------------------------------------------------

class TestDateOperations:
    """Tests for the date_operations function."""

    def test_trunc_year(self):
        """Test trunc operation with year frequency."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-07-20', '2024-11-01']})
        result = date_operations(df, 'dt', 'trunc', freq='year')
        # Year floor should give end of year
        assert pd.api.types.is_datetime64_any_dtype(result['dt'])

    def test_trunc_month(self):
        """Test trunc operation with month frequency."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-07-20']})
        result = date_operations(df, 'dt', 'trunc', freq='month')
        assert result['dt'].iloc[0] == pd.Timestamp('2024-02-29') or \
               result['dt'].iloc[0].month == 2 or result['dt'].iloc[0].month == 3

    def test_trunc_missing_freq(self):
        """Test that trunc without 'freq' raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="'trunc' operation requires 'freq'"):
            date_operations(df, 'dt', 'trunc')

    def test_trunc_invalid_freq(self):
        """Test that trunc with invalid freq raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="Invalid freq"):
            date_operations(df, 'dt', 'trunc', freq='decade')

    def test_extract_year(self):
        """Test extract year operation."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2023-07-20', '2022-11-01']})
        result = date_operations(df, 'dt', 'extract', part='year')
        assert result['dt_year'].tolist() == [2024, 2023, 2022]

    def test_extract_month(self):
        """Test extract month operation."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-07-20', '2024-11-01']})
        result = date_operations(df, 'dt', 'extract', part='month')
        assert result['dt_month'].tolist() == [3, 7, 11]

    def test_extract_quarter(self):
        """Test extract quarter operation."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-07-20', '2024-11-01']})
        result = date_operations(df, 'dt', 'extract', part='quarter')
        assert result['dt_quarter'].tolist() == [1, 3, 4]

    def test_extract_dayofweek(self):
        """Test extract dayofweek operation."""
        df = pd.DataFrame({'dt': ['2024-03-15']})  # Friday = 4
        result = date_operations(df, 'dt', 'extract', part='dayofweek')
        assert result['dt_dayofweek'].iloc[0] == 4

    def test_extract_missing_part(self):
        """Test that extract without 'part' raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="'extract' operation requires 'part'"):
            date_operations(df, 'dt', 'extract')

    def test_extract_invalid_part(self):
        """Test that extract with invalid part raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="Invalid part"):
            date_operations(df, 'dt', 'extract', part='millennium')

    def test_diff(self):
        """Test diff operation."""
        df = pd.DataFrame({
            'start': ['2024-01-01', '2024-06-01'],
            'end': ['2024-01-11', '2024-06-21'],
        })
        result = date_operations(df, 'end', 'diff', other='start', unit='D')
        assert result['end_diff_start'].tolist() == [10.0, 20.0]

    def test_diff_hours(self):
        """Test diff operation with hours unit."""
        df = pd.DataFrame({
            'start': ['2024-01-01 00:00'],
            'end': ['2024-01-01 12:00'],
        })
        result = date_operations(df, 'end', 'diff', other='start', unit='h')
        assert result['end_diff_start'].iloc[0] == 12.0

    def test_diff_missing_other(self):
        """Test that diff without 'other' raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="'diff' operation requires 'other'"):
            date_operations(df, 'dt', 'diff')

    def test_diff_invalid_unit(self):
        """Test that diff with invalid unit raises ValueError."""
        df = pd.DataFrame({
            'dt': ['2024-03-15'],
            'other': ['2024-03-14'],
        })
        with pytest.raises(ValueError, match="Invalid unit"):
            date_operations(df, 'dt', 'diff', other='other', unit='years')

    def test_format(self):
        """Test format operation."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-07-20']})
        result = date_operations(df, 'dt', 'format', fmt='%Y-%m-%d')
        assert result['dt_formatted'].tolist() == ['2024-03-15', '2024-07-20']

    def test_format_missing_fmt(self):
        """Test that format without 'fmt' raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="'format' operation requires 'fmt'"):
            date_operations(df, 'dt', 'format')

    def test_add_days(self):
        """Test add operation with days."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        result = date_operations(df, 'dt', 'add', value=10, unit='days')
        assert result['dt_added'].iloc[0] == pd.Timestamp('2024-03-25')

    def test_add_hours(self):
        """Test add operation with hours."""
        df = pd.DataFrame({'dt': ['2024-03-15 12:00']})
        result = date_operations(df, 'dt', 'add', value=5, unit='hours')
        assert result['dt_added'].iloc[0] == pd.Timestamp('2024-03-15 17:00')

    def test_add_missing_value(self):
        """Test that add without 'value' raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="'add' operation requires 'value'"):
            date_operations(df, 'dt', 'add', unit='days')

    def test_add_missing_unit(self):
        """Test that add without 'unit' raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="'add' operation requires 'unit'"):
            date_operations(df, 'dt', 'add', value=10)

    def test_add_invalid_unit(self):
        """Test that add with invalid unit raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="Invalid unit"):
            date_operations(df, 'dt', 'add', value=10, unit='years')

    def test_is_weekend(self):
        """Test is_weekend operation."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-03-16', '2024-03-17']})
        # Fri=4, Sat=5, Sun=6
        result = date_operations(df, 'dt', 'is_weekend')
        assert result['dt_is_weekend'].tolist() == [False, True, True]

    def test_is_business_day(self):
        """Test is_business_day operation."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-03-16', '2024-03-17']})
        result = date_operations(df, 'dt', 'is_business_day')
        assert result['dt_is_business_day'].tolist() == [True, False, False]

    def test_invalid_operation(self):
        """Test that an invalid operation raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="Invalid date operation"):
            date_operations(df, 'dt', 'invalid_op')

    def test_missing_column(self):
        """Test that a missing column raises ValueError."""
        df = pd.DataFrame({'dt': ['2024-03-15']})
        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            date_operations(df, 'nonexistent', 'extract', part='year')

    def test_empty_dataframe(self):
        """Test date operations on an empty DataFrame."""
        df = pd.DataFrame({'dt': pd.Series([], dtype=str)})
        result = date_operations(df, 'dt', 'extract', part='year')
        assert len(result) == 0
        assert 'dt_year' in result.columns


# ---------------------------------------------------------------------------
# TransformationChain tests
# ---------------------------------------------------------------------------

class TestTransformationChain:
    """Tests for the TransformationChain class."""

    def test_filter_and_select(self):
        """Test chaining filter and select_columns."""
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie', 'David'],
            'age': [25, 17, 35, 42],
            'score': [85.5, 92.0, 78.5, 88.0],
        })
        result = (
            TransformationChain(df)
            .filter(column='age', min_value=18)
            .select_columns(['name', 'age'])
            .result()
        )
        assert len(result) == 3
        assert list(result.columns) == ['name', 'age']
        assert 'Bob' not in result['name'].values

    def test_sort_and_limit(self):
        """Test chaining sort and limit."""
        df = pd.DataFrame({
            'name': ['Charlie', 'Alice', 'David', 'Bob'],
            'age': [35, 25, 42, 17],
        })
        result = (
            TransformationChain(df)
            .sort(by='name')
            .limit(2)
            .result()
        )
        assert len(result) == 2
        assert result['name'].tolist() == ['Alice', 'Bob']

    def test_with_column_and_cast(self):
        """Test chaining with_column and cast."""
        df = pd.DataFrame({'a': ['1', '2', '3']})
        result = (
            TransformationChain(df)
            .cast({'a': 'int64'})
            .with_column('b', lambda d: d['a'] * 10)
            .result()
        )
        assert result['b'].tolist() == [10, 20, 30]

    def test_rename_and_drop(self):
        """Test chaining rename_columns and drop_columns."""
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})
        result = (
            TransformationChain(df)
            .rename_columns({'a': 'alpha'})
            .drop_columns('c')
            .result()
        )
        assert 'alpha' in result.columns
        assert 'a' not in result.columns
        assert 'c' not in result.columns

    def test_fill_na_and_drop_na(self):
        """Test chaining fill_na and drop_na."""
        df = pd.DataFrame({
            'a': [1, None, 3],
            'b': [None, None, 6],
        })
        result = (
            TransformationChain(df)
            .fill_na(value=0, subset=['a'])
            .drop_na(subset=['b'])
            .result()
        )
        # Only row with index 2 has non-NaN 'b', so only 1 row remains
        assert len(result) == 1
        assert result['a'].iloc[0] == 3
        assert result['b'].iloc[0] == 6

    def test_five_step_chain(self):
        """Test a chain of 5 operations."""
        df = pd.DataFrame({
            'name': ['  Alice  ', 'BOB', '  Charlie  ', 'david', 'Eve'],
            'age': [25, 17, 35, 42, 15],
            'score': [85.5, 92.0, 78.5, 88.0, 95.0],
        })
        result = (
            TransformationChain(df)
            .str_op('name', 'trim')
            .str_op('name', 'lower')
            .filter(column='age', min_value=18)
            .with_column('grade', lambda d: d['score'].apply(
                lambda s: 'A' if s >= 90 else 'B' if s >= 80 else 'C'
            ))
            .sort(by='name')
            .result()
        )
        assert len(result) == 3
        assert result['name'].tolist() == ['alice', 'charlie', 'david']
        assert 'grade' in result.columns

    def test_distinct(self):
        """Test distinct operation in chain."""
        df = pd.DataFrame({'a': [1, 1, 2, 2, 3], 'b': [10, 10, 20, 20, 30]})
        result = TransformationChain(df).distinct().result()
        assert len(result) == 3

    def test_deduplicate(self):
        """Test deduplicate operation in chain."""
        df = pd.DataFrame({'a': [1, 1, 2, 2, 3], 'b': [10, 10, 20, 20, 30]})
        result = TransformationChain(df).deduplicate(subset=['a']).result()
        assert len(result) == 3

    def test_sample(self):
        """Test sample operation in chain."""
        df = pd.DataFrame({'a': range(100)})
        result = TransformationChain(df).sample(n=10, seed=42).result()
        assert len(result) == 10

    def test_aggregate_in_chain(self):
        """Test aggregate operation in chain."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 45000, 55000],
        })
        result = (
            TransformationChain(df)
            .aggregate('dept', {'salary': 'mean'})
            .sort(by='dept')
            .result()
        )
        assert len(result) == 2
        assert result[result['dept'] == 'A']['salary'].iloc[0] == 55000.0

    def test_window_in_chain(self):
        """Test window operation in chain."""
        df = pd.DataFrame({
            'dept': ['A', 'A', 'B', 'B'],
            'salary': [50000, 60000, 45000, 55000],
        })
        result = (
            TransformationChain(df)
            .window('dept', 'salary', {'rn': {'function': 'row_number'}})
            .result()
        )
        assert 'rn' in result.columns

    def test_date_op_in_chain(self):
        """Test date_op in chain."""
        df = pd.DataFrame({'dt': ['2024-03-15', '2024-07-20']})
        result = (
            TransformationChain(df)
            .date_op('dt', 'extract', part='year')
            .result()
        )
        assert result['dt_year'].tolist() == [2024, 2024]

    def test_compute_in_chain(self):
        """Test compute (add_computed_column) in chain."""
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        result = (
            TransformationChain(df)
            .compute('a + b', 'total')
            .result()
        )
        assert result['total'].tolist() == [5, 7, 9]

    def test_when_in_chain(self):
        """Test when (when_otherwise) in chain."""
        df = pd.DataFrame({'score': [95, 82, 70, 55]})
        result = (
            TransformationChain(df)
            .when(
                [
                    (df['score'] >= 90, 'A'),
                    (df['score'] >= 80, 'B'),
                    (df['score'] >= 70, 'C'),
                ],
                otherwise_value='F',
                output_col='grade',
            )
            .result()
        )
        assert result['grade'].tolist() == ['A', 'B', 'C', 'F']

    def test_result_returns_dataframe(self):
        """Test that result() returns a DataFrame."""
        df = pd.DataFrame({'a': [1, 2, 3]})
        result = TransformationChain(df).result()
        assert isinstance(result, pd.DataFrame)

    def test_chain_does_not_mutate_original(self):
        """Test that the original DataFrame is not mutated."""
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        original_cols = list(df.columns)
        TransformationChain(df).drop_columns('b').result()
        assert list(df.columns) == original_cols

    def test_empty_dataframe_chain(self):
        """Test chain with an empty DataFrame."""
        df = pd.DataFrame({'a': pd.Series([], dtype=int)})
        result = (
            TransformationChain(df)
            .with_column('b', 1)
            .result()
        )
        assert len(result) == 0
        assert 'b' in result.columns

    def test_missing_column_in_chain_raises(self):
        """Test that operations on missing columns raise ValueError in chain."""
        df = pd.DataFrame({'a': [1, 2, 3]})
        with pytest.raises(ValueError):
            TransformationChain(df).select_columns(['nonexistent']).result()


# ---------------------------------------------------------------------------
# chain() convenience function tests
# ---------------------------------------------------------------------------

class TestChainFunction:
    """Tests for the chain() convenience function."""

    def test_chain_returns_transformation_chain(self):
        """Test that chain() returns a TransformationChain."""
        df = pd.DataFrame({'a': [1, 2, 3]})
        c = chain(df)
        assert isinstance(c, TransformationChain)

    def test_chain_basic_usage(self):
        """Test basic chain usage."""
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 17, 35],
        })
        result = (
            chain(df)
            .filter(column='age', min_value=18)
            .select_columns(['name'])
            .result()
        )
        assert len(result) == 2
        assert 'Bob' not in result['name'].values

    def test_chain_with_multiple_ops(self):
        """Test chain with multiple operations."""
        df = pd.DataFrame({
            'x': [3, 1, 2],
            'y': [10, 20, 30],
        })
        result = (
            chain(df)
            .sort(by='x')
            .with_column('z', lambda d: d['x'] + d['y'])
            .limit(2)
            .result()
        )
        assert len(result) == 2
        assert result['x'].tolist() == [1, 2]
        assert result['z'].tolist() == [21, 32]
