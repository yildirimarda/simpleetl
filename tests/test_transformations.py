"""
Tests for the transformation module.
"""

import pandas as pd
import pytest
from simpleetl.transformations import filter_data, map_values, aggregate_data


def test_filter_data():
    """Test the filter_data function."""
    # Create test DataFrame
    df = pd.DataFrame({
        'age': [10, 20, 30, 40, 50],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [85.5, 92.0, 78.5, 88.0, 95.5]
    })

    # Test filtering with min_value only
    filtered = filter_data(df, column='age', min_value=25)
    assert len(filtered) == 3
    assert list(filtered['age']) == [30, 40, 50]

    # Test filtering with max_value only
    filtered = filter_data(df, column='score', max_value=85.0)
    assert len(filtered) == 1
    assert list(filtered['score']) == [78.5]

    # Test filtering with both min and max
    filtered = filter_data(df, column='age', min_value=20, max_value=40)
    assert len(filtered) == 3
    assert list(filtered['age']) == [20, 30, 40]

    # Test filtering with no bounds (should return original)
    filtered = filter_data(df, column='age')
    pd.testing.assert_frame_equal(filtered, df)

    # Test with non-existent column
    with pytest.raises(ValueError, match="Column 'nonexistent' not found in DataFrame"):
        filter_data(df, column='nonexistent', min_value=0)

    # Test filtering with filter_func
    filtered = filter_data(df, filter_func=lambda row: row['age'] > 25)
    assert len(filtered) == 3
    assert list(filtered['age']) == [30, 40, 50]

    # Test filtering with filter_func and column raises ValueError
    with pytest.raises(ValueError, match="Either filter_func or column must be provided"):
        filter_data(df)


def test_map_values():
    """Test the map_values function."""
    # Create test DataFrame
    df = pd.DataFrame({
        'category': ['A', 'B', 'A', 'C', 'B'],
        'value': [1, 2, 3, 4, 5],
        'status': ['active', 'inactive', 'active', 'active', 'inactive']
    })

    # Test mapping with dictionary
    mapping_dict = {'A': 'Alpha', 'B': 'Beta', 'C': 'Gamma'}
    mapped = map_values(df, 'category', mapping_dict)
    assert list(mapped['category']) == ['Alpha', 'Beta', 'Alpha', 'Gamma', 'Beta']

    # Test mapping with function
    mapped = map_values(df, 'value', lambda x: x * 2)
    assert list(mapped['value']) == [2, 4, 6, 8, 10]

    # Test mapping with function that returns strings
    mapped = map_values(df, 'status', lambda x: x.upper())
    assert list(mapped['status']) == ['ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE', 'INACTIVE']

    # Test with non-existent column
    with pytest.raises(ValueError, match="Column 'nonexistent' not found in DataFrame"):
        map_values(df, 'nonexistent', {'a': 'b'})

    # Test with invalid mapping type
    with pytest.raises(TypeError, match="Mapping must be a dictionary or a callable function"):
        map_values(df, 'category', "invalid")


def test_aggregate_data():
    """Test the aggregate_data function."""
    # Create test DataFrame
    df = pd.DataFrame({
        'department': ['Sales', 'Sales', 'HR', 'HR', 'IT', 'IT'],
        'employee': ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'David'],
        'salary': [50000, 60000, 45000, 55000, 70000, 80000],
        'years': [2, 3, 1, 4, 5, 2]
    })

    # Test simple aggregation
    agg_spec = {'salary': 'mean', 'years': 'sum'}
    aggregated = aggregate_data(df, 'department', agg_spec)
    assert len(aggregated) == 3
    # Check that departments are correct
    assert set(aggregated['department']) == {'Sales', 'HR', 'IT'}
    # Check Sales department: mean salary = (50000+60000)/2 = 55000, sum years = 2+3 = 5
    sales_row = aggregated[aggregated['department'] == 'Sales'].iloc[0]
    assert sales_row['salary'] == 55000.0
    assert sales_row['years'] == 5

    # Test multiple groupby columns
    df_with_region = df.copy()
    df_with_region['region'] = ['North', 'North', 'South', 'South', 'East', 'East']
    agg_spec = {'salary': ['mean', 'max'], 'years': 'mean'}
    aggregated = aggregate_data(df_with_region, ['department', 'region'], agg_spec)
    assert len(aggregated) == 3
    # Check column names are flattened
    assert 'salary_mean' in aggregated.columns
    assert 'salary_max' in aggregated.columns
    assert 'years_mean' in aggregated.columns

    # Test with non-existent groupby column
    with pytest.raises(ValueError, match="Groupby column 'nonexistent' not found in DataFrame"):
        aggregate_data(df, 'nonexistent', {'salary': 'mean'})

    # Test with non-existent aggregation column
    with pytest.raises(ValueError, match="Aggregation column 'nonexistent' not found in DataFrame"):
        aggregate_data(df, 'department', {'nonexistent': 'mean'})