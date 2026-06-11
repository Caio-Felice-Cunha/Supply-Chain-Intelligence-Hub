"""Tests for scripts/etl/transformer.py (DataTransformer).

Covers the cleaning strategies, deduplication, derived columns, and business
rule filtering. Includes a regression test for the pandas 3.0 forward-fill
fix (fillna(method='ffill') was removed in pandas 3.0).
"""

import numpy as np
import pandas as pd

from etl import DataTransformer


def test_clean_nulls_drop(null_logger):
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"a": [1.0, np.nan, 3.0]})
    result = t.clean_nulls(df, strategy="drop")
    assert len(result) == 2


def test_clean_nulls_fill_mean(null_logger):
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"a": [2.0, np.nan, 4.0]})
    result = t.clean_nulls(df, strategy="fill_mean")
    # Mean of [2, 4] is 3, so the gap is filled with 3.
    assert result["a"].tolist() == [2.0, 3.0, 4.0]


def test_clean_nulls_fill_forward_regression(null_logger):
    """Forward fill must work under pandas 3.0 (was fillna(method='ffill'))."""
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"a": [1.0, np.nan, np.nan, 4.0]})
    result = t.clean_nulls(df, strategy="fill_forward")
    assert result["a"].tolist() == [1.0, 1.0, 1.0, 4.0]


def test_clean_nulls_unknown_strategy_returns_original(null_logger):
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"a": [1.0, np.nan]})
    result = t.clean_nulls(df, strategy="does_not_exist")
    assert result.equals(df)


def test_remove_duplicates(null_logger):
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
    result = t.remove_duplicates(df)
    assert len(result) == 2


def test_add_derived_columns_sales_unit_price(null_logger):
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"revenue": [100.0, 50.0], "quantity_sold": [4, 5]})
    result = t.add_derived_columns(df, "sales")
    assert "unit_price" in result.columns
    assert result["unit_price"].tolist() == [25.0, 10.0]


def test_add_derived_columns_inventory_available(null_logger):
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"quantity_on_hand": [10, 20], "quantity_reserved": [3, 5]})
    result = t.add_derived_columns(df, "inventory")
    assert result["quantity_available"].tolist() == [7, 15]


def test_apply_business_rules_filters_invalid_sales(null_logger):
    t = DataTransformer(null_logger)
    df = pd.DataFrame({"quantity_sold": [5, 0, 3], "revenue": [50.0, 0.0, -1.0]})
    result = t.apply_business_rules(df, "sales")
    # Row with quantity_sold 0 and row with negative revenue are dropped.
    assert len(result) == 1
    assert result.iloc[0]["quantity_sold"] == 5
