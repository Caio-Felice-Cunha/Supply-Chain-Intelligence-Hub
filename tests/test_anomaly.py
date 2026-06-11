"""Tests for scripts/quality/anomaly.py (AnomalyDetector)."""

import pandas as pd

from anomaly import AnomalyDetector


def test_detect_outliers_iqr_flags_extreme_value():
    series = pd.Series([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 1000])
    outliers = AnomalyDetector.detect_outliers_iqr(series)
    assert outliers.iloc[-1]  # 1000 is an outlier
    assert outliers.sum() == 1


def test_detect_outliers_zscore():
    series = pd.Series([1, 2, 3, 4, 5, 100])
    outliers = AnomalyDetector.detect_outliers_zscore(series, threshold=2.0)
    assert outliers.sum() >= 1


def test_analyze_table_anomalies_structure():
    df = pd.DataFrame({"revenue": list(range(1, 30)) + [5000]})
    result = AnomalyDetector.analyze_table_anomalies(df, "sales")
    assert result["table_name"] == "sales"
    assert "revenue" in result["outliers_detected"]
    assert result["outliers_detected"]["revenue"]["method"] == "IQR"
    assert result["outliers_detected"]["revenue"]["count"] >= 1


def test_analyze_skips_low_cardinality_columns():
    # nunique <= 10 columns are skipped by design.
    df = pd.DataFrame({"flag": [0, 1, 0, 1, 0, 1]})
    result = AnomalyDetector.analyze_table_anomalies(df, "demo")
    assert result["outliers_detected"] == {}


def test_isolation_forest_returns_predictions():
    df = pd.DataFrame({"x": list(range(50)), "y": list(range(50))})
    preds = AnomalyDetector.detect_outliers_isolation_forest(df, ["x", "y"])
    assert len(preds) == 50
    # IsolationForest labels are -1 (outlier) or 1 (inlier).
    assert set(preds.tolist()).issubset({-1, 1})
