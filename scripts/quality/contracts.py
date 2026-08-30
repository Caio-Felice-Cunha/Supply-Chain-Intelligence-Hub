"""Cross-table contracts for the generated portfolio dataset."""

from __future__ import annotations

from typing import Dict, Iterable, List

import pandas as pd

from scripts.generation import ScaleConfig


PRIMARY_KEYS = {
    "suppliers": "supplier_id",
    "products": "product_id",
    "warehouses": "warehouse_id",
    "inventory": "inventory_id",
    "sales": "sale_id",
    "orders": "order_id",
    "price_history": "price_history_id",
}

FOREIGN_KEYS = {
    "products": {"supplier_id": ("suppliers", "supplier_id")},
    "inventory": {
        "product_id": ("products", "product_id"),
        "warehouse_id": ("warehouses", "warehouse_id"),
    },
    "sales": {
        "product_id": ("products", "product_id"),
        "warehouse_id": ("warehouses", "warehouse_id"),
    },
    "orders": {
        "supplier_id": ("suppliers", "supplier_id"),
        "product_id": ("products", "product_id"),
        "warehouse_id": ("warehouses", "warehouse_id"),
    },
    "price_history": {
        "product_id": ("products", "product_id"),
        "supplier_id": ("suppliers", "supplier_id"),
    },
}

DATE_COLUMNS = {
    "suppliers": ["created_date"],
    "inventory": ["snapshot_date"],
    "sales": ["sale_date"],
    "orders": ["order_date", "expected_delivery_date", "actual_delivery_date"],
    "price_history": ["effective_date"],
}


def _rule(name: str, passed: bool, observed: str, severity: str = "ERROR") -> dict:
    return {"name": name, "passed": bool(passed), "observed": observed, "severity": severity}


def validate_dataset(
    tables: Dict[str, pd.DataFrame],
    config: ScaleConfig,
    processed: Dict[str, pd.DataFrame] | None = None,
) -> List[dict]:
    """Validate counts, uniqueness, FKs, measures, dates, and ETL parity."""

    rules: List[dict] = []
    for table, expected in config.expected_counts.items():
        actual = len(tables[table])
        rules.append(_rule(f"{table}.row_count", actual == expected, f"{actual:,} / {expected:,}"))

    for table, key in PRIMARY_KEYS.items():
        frame = tables[table]
        unique = frame[key].notna().all() and frame[key].is_unique
        rules.append(_rule(f"{table}.{key}.primary_key", unique, f"{frame[key].nunique():,} unique"))

    for table, mappings in FOREIGN_KEYS.items():
        for column, (parent, parent_key) in mappings.items():
            orphaned = int((~tables[table][column].isin(tables[parent][parent_key])).sum())
            rules.append(_rule(f"{table}.{column}.foreign_key", orphaned == 0, f"{orphaned} orphaned"))

    non_negative = {
        "products": ["unit_cost", "base_price", "reorder_level"],
        "warehouses": ["capacity_units", "current_utilization_pct"],
        "inventory": ["quantity_on_hand", "quantity_reserved"],
        "sales": ["quantity_sold", "unit_price", "revenue"],
        "orders": ["order_quantity", "order_cost"],
        "price_history": ["unit_price"],
    }
    for table, columns in non_negative.items():
        invalid = int((tables[table][columns] < 0).sum().sum())
        rules.append(_rule(f"{table}.non_negative_measures", invalid == 0, f"{invalid} invalid cells"))

    expected_revenue = (tables["sales"].quantity_sold * tables["sales"].unit_price).round(2)
    revenue_delta = (tables["sales"].revenue - expected_revenue).abs().max()
    rules.append(_rule("sales.revenue_formula", revenue_delta <= 0.01, f"max delta {revenue_delta:.2f}"))

    min_date = min(pd.to_datetime(tables["sales"].sale_date).min(), pd.to_datetime(tables["orders"].order_date).min())
    max_date = max(pd.to_datetime(tables["sales"].sale_date).max(), pd.to_datetime(tables["price_history"].effective_date).max())
    rules.append(
        _rule(
            "dataset.date_contract",
            min_date.date() == config.start_date and max_date.date() == config.end_date,
            f"{min_date.date()} to {max_date.date()}",
        )
    )

    controlled_anomalies = sum(
        int((tables[name].get("anomaly_type", pd.Series(dtype=str)).fillna("") != "").sum())
        for name in ("sales", "orders", "price_history")
    ) + int(tables["inventory"].controlled_risk.sum())
    rules.append(_rule("dataset.controlled_business_anomalies", controlled_anomalies > 0, f"{controlled_anomalies:,} flagged", "INFO"))

    if processed is not None:
        for table in config.expected_counts:
            source_count, processed_count = len(tables[table]), len(processed[table])
            rules.append(_rule(f"{table}.source_processed_parity", source_count == processed_count, f"{source_count:,} = {processed_count:,}"))

    return rules


def assert_valid(rules: Iterable[dict]) -> None:
    failures = [rule for rule in rules if not rule["passed"] and rule["severity"] == "ERROR"]
    if failures:
        details = "; ".join(f"{rule['name']}: {rule['observed']}" for rule in failures)
        raise ValueError(f"Data contract failed: {details}")
