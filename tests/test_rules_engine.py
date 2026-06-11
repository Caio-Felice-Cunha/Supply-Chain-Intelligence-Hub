"""Tests for scripts/quality/rules_engine.py (DataQualityRulesEngine).

Verifies the standard rule set matches the documentation (16 rules), that
uniqueness/validity/consistency/completeness checks fire correctly, and that
the new rules added to close the doc/code gap behave as documented.
"""

import pandas as pd

from rules_engine import DataQualityRulesEngine, ValidationRule


def test_standard_rules_count_matches_doc():
    """Pre-Defined-Data-Quality-Rule.md documents 16 rules across 5 tables."""
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    total = sum(len(rules) for rules in eng.rules.values())
    assert total == 16
    assert len(eng.rules["suppliers"]) == 3
    assert len(eng.rules["products"]) == 4
    assert len(eng.rules["inventory"]) == 3
    assert len(eng.rules["orders"]) == 3
    assert len(eng.rules["sales"]) == 3


def test_uniqueness_rule_detects_duplicates():
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    df = pd.DataFrame({"product_id": [1, 1, 2], "unit_cost": [5.0, 6.0, 7.0],
                       "reorder_level": [1, 2, 3], "product_name": ["a", "b", "c"]})
    results = {r.rule_name: r for r in eng.execute_rules(df, "products")}
    assert results["product_id_unique"].passed is False
    assert results["product_id_unique"].affected_rows == 1


def test_validity_rule_detects_negative_unit_cost():
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    df = pd.DataFrame({"product_id": [1, 2], "unit_cost": [5.0, -3.0],
                       "reorder_level": [1, 2], "product_name": ["a", "b"]})
    results = {r.rule_name: r for r in eng.execute_rules(df, "products")}
    assert results["unit_cost_positive"].passed is False


def test_completeness_rule_detects_null_product_name():
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    df = pd.DataFrame({"product_id": [1, 2], "unit_cost": [5.0, 6.0],
                       "reorder_level": [1, 2], "product_name": ["a", None]})
    results = {r.rule_name: r for r in eng.execute_rules(df, "products")}
    assert results["product_name_not_null"].passed is False


def test_consistency_reserved_not_exceed_onhand():
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    df = pd.DataFrame({"quantity_on_hand": [10, 20], "quantity_reserved": [5, 25]})
    results = {r.rule_name: r for r in eng.execute_rules(df, "inventory")}
    assert results["reserved_not_exceed_onhand"].passed is False
    assert results["reserved_not_exceed_onhand"].affected_rows == 1


def test_delivery_dates_logical_allows_in_transit_nulls():
    """Orders still in transit (null actual date) must not fail the rule."""
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    df = pd.DataFrame({
        "order_quantity": [5, 3, 2],
        "order_cost": [100.0, 50.0, 10.0],
        "expected_delivery_date": ["2026-01-10", "2026-01-20", "2026-01-05"],
        "actual_delivery_date": ["2026-01-12", None, "2026-01-04"],
    })
    results = {r.rule_name: r for r in eng.execute_rules(df, "orders")}
    # Only the third row (actual before expected) is a violation.
    assert results["delivery_dates_logical"].passed is False
    assert results["delivery_dates_logical"].affected_rows == 1


def test_revenue_quantity_consistency_is_not_a_tautology():
    """A zero-revenue sale with positive quantity must fail the check."""
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    df = pd.DataFrame({"quantity_sold": [2, 4], "revenue": [20.0, 0.0]})
    results = {r.rule_name: r for r in eng.execute_rules(df, "sales")}
    assert results["revenue_quantity_consistency"].passed is False


def test_unknown_table_returns_empty():
    eng = DataQualityRulesEngine()
    eng.define_standard_rules()
    assert eng.execute_rules(pd.DataFrame({"x": [1]}), "not_a_table") == []


def test_custom_rule_can_be_added():
    eng = DataQualityRulesEngine()
    eng.add_rule("products", ValidationRule(
        rule_name="reorder_minimum",
        rule_type="validity",
        column="reorder_level",
        condition=lambda df: df["reorder_level"] >= 10,
        severity="WARNING",
        description="Reorder level should be at least 10",
    ))
    df = pd.DataFrame({"reorder_level": [5, 15]})
    results = eng.execute_rules(df, "products")
    assert results[0].rule_name == "reorder_minimum"
    assert results[0].passed is False
