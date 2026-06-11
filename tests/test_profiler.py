"""Tests for scripts/quality/profiler.py (DataProfiler)."""

import pandas as pd

from profiler import DataProfiler


def test_profile_dataframe_shape():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    profile = DataProfiler.profile_dataframe(df, "demo")
    assert profile["table_name"] == "demo"
    assert profile["row_count"] == 3
    assert profile["column_count"] == 2
    assert set(profile["columns"].keys()) == {"a", "b"}


def test_profile_counts_duplicates():
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    profile = DataProfiler.profile_dataframe(df, "demo")
    assert profile["duplicate_rows"] == 1


def test_profile_numeric_column_stats():
    series = pd.Series([1.0, 2.0, 3.0, 4.0])
    stats = DataProfiler.profile_numeric_column(series)
    assert stats["count"] == 4
    assert stats["min"] == 1.0
    assert stats["max"] == 4.0
    assert stats["mean"] == 2.5


def test_profile_categorical_top_value():
    series = pd.Series(["a", "a", "b"])
    stats = DataProfiler.profile_categorical_column(series)
    assert stats["top_value"] == "a"
    assert stats["top_frequency"] == 2
    assert stats["unique"] == 2


def test_profile_records_null_percentage():
    df = pd.DataFrame({"a": [1.0, None, 3.0, None]})
    profile = DataProfiler.profile_dataframe(df, "demo")
    assert profile["columns"]["a"]["null_count"] == 2
    assert profile["columns"]["a"]["null_percentage"] == 50.0
