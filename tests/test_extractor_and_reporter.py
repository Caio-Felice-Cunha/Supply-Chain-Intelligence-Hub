"""Tests for the extractor table allowlist and the reporter output guards.

These do not require a live database. The extractor is exercised only up to
the allowlist check (which runs before any connection is touched), and the
reporter writes to a temporary file.
"""

import json
import logging

import pytest

from etl import DataExtractor
from reporter import DataQualityReporter


def _extractor():
    logger = logging.getLogger("test_extractor")
    logger.addHandler(logging.NullHandler())
    # connection is unused because the allowlist check happens first.
    return DataExtractor(connection=None, logger=logger)


def test_extractor_rejects_unknown_table():
    ext = _extractor()
    with pytest.raises(ValueError):
        ext.extract_table("products; DROP TABLE products")


def test_extractor_rejects_injection_attempt():
    ext = _extractor()
    with pytest.raises(ValueError):
        ext.extract_table("sales UNION SELECT * FROM mysql.user")


def test_extractor_allowlist_contains_schema_tables():
    expected = {"suppliers", "products", "warehouses", "inventory",
                "orders", "sales", "price_history"}
    assert expected == set(DataExtractor.ALLOWED_TABLES)


def test_html_report_handles_empty_results(tmp_path):
    """Empty validation results must not raise ZeroDivisionError."""
    out = tmp_path / "report.html"
    DataQualityReporter.generate_html_report([], {}, {}, output_path=str(out))
    content = out.read_text(encoding="utf-8")
    assert "0.0%" in content  # pass rate falls back to zero, no crash


def test_summary_report_pass_rate(tmp_path):
    class _Result:
        def __init__(self, passed):
            self.passed = passed

    results = [_Result(True), _Result(True), _Result(False)]
    report = DataQualityReporter.generate_summary_report(results, {}, {})
    assert report["validation_summary"]["total_rules"] == 3
    assert report["validation_summary"]["passed"] == 2
    assert report["validation_summary"]["pass_rate"] == "66.67%"


def test_export_to_json_is_utf8(tmp_path):
    out = tmp_path / "summary.json"
    DataQualityReporter.export_to_json({"status": "ok", "mark": "✓"}, str(out))
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["status"] == "ok"
