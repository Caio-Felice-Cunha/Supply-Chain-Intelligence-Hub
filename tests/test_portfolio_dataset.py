import hashlib

import pandas as pd
from pandas.testing import assert_frame_equal

from scripts.analytics import build_dashboard_payload, calculate_filtered_kpis, transform_dataset
from scripts.generation import PORTFOLIO_COUNTS, generate_dataset, get_scale
from scripts.orchestration.publisher import build_dataset_archive
from scripts.quality.contracts import assert_valid, validate_dataset


def test_portfolio_scale_has_exact_release_contract_and_integrity():
    source = generate_dataset("portfolio", 42)
    assert {name: len(frame) for name, frame in source.items()} == PORTFOLIO_COUNTS
    assert sum(len(frame) for frame in source.values()) == 101_786
    processed = transform_dataset(source)
    rules = validate_dataset(source, get_scale("portfolio"), processed)
    assert_valid(rules)
    assert all(rule["passed"] for rule in rules)
    assert pd.to_datetime(source["sales"].sale_date).min().date().isoformat() == "2024-07-01"
    assert pd.to_datetime(source["price_history"].effective_date).max().date().isoformat() == "2026-06-30"


def test_seed_is_repeatable_and_archive_has_stable_hash():
    first = generate_dataset("smoke", 42)
    second = generate_dataset("smoke", 42)
    for table in first:
        assert_frame_equal(first[table], second[table])
    manifest = {"seed": 42, "scale": "smoke", "source_counts": {name: len(frame) for name, frame in first.items()}}
    first_zip, _ = build_dataset_archive(transform_dataset(first), manifest)
    second_zip, _ = build_dataset_archive(transform_dataset(second), manifest)
    assert hashlib.sha256(first_zip).hexdigest() == hashlib.sha256(second_zip).hexdigest()


def test_filtered_kpis_match_source_facts():
    source = transform_dataset(generate_dataset("smoke", 42))
    rules = validate_dataset(source, get_scale("smoke"), source)
    payload = build_dashboard_payload(source, rules, [], "smoke", 42)
    cube = pd.DataFrame(payload["cube"])
    actual = calculate_filtered_kpis(
        cube,
        start_period="2026-02",
        end_period="2026-05",
        warehouse_id=2,
        category="Home",
    )

    products = source["products"][["product_id", "category"]]
    sales = source["sales"].merge(products, on="product_id")
    selected_sales = sales[
        (sales.sale_date.dt.to_period("M").astype(str).between("2026-02", "2026-05"))
        & (sales.warehouse_id == 2)
        & (sales.category == "Home")
    ]
    orders = source["orders"].merge(products, on="product_id")
    selected_orders = orders[
        (orders.order_date.dt.to_period("M").astype(str).between("2026-02", "2026-05"))
        & (orders.warehouse_id == 2)
        & (orders.category == "Home")
    ]
    completed = selected_orders[selected_orders.actual_delivery_date.notna()]
    expected_rate = round(float((completed.delivery_delay_days <= 0).mean() * 100), 2) if len(completed) else 0
    assert actual["revenue"] == round(float(selected_sales.revenue.sum()), 2)
    assert actual["units"] == int(selected_sales.quantity_sold.sum())
    assert actual["on_time_delivery_rate"] == expected_rate


def test_business_anomalies_are_controlled_not_structural():
    source = generate_dataset("portfolio", 42)
    assert (source["sales"].anomaly_type == "DEMAND_SPIKE").sum() == 70
    assert (source["orders"].anomaly_type == "EXTREME_DELAY").sum() == 37
    assert (source["price_history"].anomaly_type == "PRICE_JUMP").sum() == 5
    assert source["inventory"].controlled_risk.sum() == 745
    assert (source["inventory"].quantity_reserved <= source["inventory"].quantity_on_hand).all()
