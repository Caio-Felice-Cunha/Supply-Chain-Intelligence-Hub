"""Transform source tables and build the dashboard's filterable aggregate cube."""

from __future__ import annotations

from typing import Dict, Iterable

import pandas as pd


def transform_dataset(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Apply lossless, observable transformations to all seven source tables."""

    transformed = {name: frame.copy() for name, frame in tables.items()}
    for table, frame in transformed.items():
        for column in [value for value in frame.columns if "date" in value]:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
        transformed[table] = frame.drop_duplicates().reset_index(drop=True)

    numeric_columns = {
        "suppliers": ["reliability_score", "lead_time_days"],
        "products": ["unit_cost", "base_price", "reorder_level"],
        "warehouses": ["capacity_units", "current_utilization_pct"],
        "inventory": ["quantity_on_hand", "quantity_reserved"],
        "sales": ["quantity_sold", "unit_price", "revenue"],
        "orders": ["order_quantity", "order_cost"],
        "price_history": ["unit_price"],
    }
    for table, columns in numeric_columns.items():
        for column in columns:
            transformed[table][column] = pd.to_numeric(transformed[table][column], errors="raise")
    transformed["inventory"]["controlled_risk"] = transformed["inventory"].controlled_risk.astype(bool)
    transformed["sales"]["is_promotion"] = transformed["sales"].is_promotion.astype(bool)

    inventory = transformed["inventory"]
    inventory["quantity_available"] = inventory.quantity_on_hand - inventory.quantity_reserved

    orders = transformed["orders"]
    orders["delivery_delay_days"] = (orders.actual_delivery_date - orders.expected_delivery_date).dt.days
    orders["is_on_time"] = orders.actual_delivery_date.notna() & (orders.delivery_delay_days <= 0)

    sales = transformed["sales"]
    sales["calculated_revenue"] = (sales.quantity_sold * sales.unit_price).round(2)
    sales["revenue_variance"] = (sales.revenue - sales.calculated_revenue).round(2)
    return transformed


def _join_dimensions(frame: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    columns = ["product_id", "category", "reorder_level", "demand_class"]
    if "supplier_id" not in frame.columns:
        columns.append("supplier_id")
    dimensions = products[columns]
    return frame.merge(dimensions, on="product_id", how="left", validate="many_to_one")


def _sales_cube(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    sales = _join_dimensions(tables["sales"], tables["products"])
    sales["period"] = sales.sale_date.dt.to_period("M").astype(str)
    return (
        sales.groupby(["period", "warehouse_id", "category", "supplier_id"], observed=True)
        .agg(revenue=("revenue", "sum"), units=("quantity_sold", "sum"), sales_events=("sale_id", "count"), promotions=("is_promotion", "sum"))
        .reset_index()
    )


def _orders_cube(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    orders = _join_dimensions(tables["orders"], tables["products"])
    orders["period"] = orders.order_date.dt.to_period("M").astype(str)
    orders["completed_order"] = orders.actual_delivery_date.notna().astype(int)
    orders["on_time_order"] = orders.is_on_time.astype(int)
    orders["late_days"] = orders.delivery_delay_days.clip(lower=0).fillna(0)
    return (
        orders.groupby(["period", "warehouse_id", "category", "supplier_id"], observed=True)
        .agg(
            purchase_orders=("order_id", "count"),
            completed_orders=("completed_order", "sum"),
            on_time_orders=("on_time_order", "sum"),
            late_days=("late_days", "sum"),
            ordered_units=("order_quantity", "sum"),
        )
        .reset_index()
    )


def _inventory_cube(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    inventory = _join_dimensions(tables["inventory"], tables["products"])
    inventory["period"] = inventory.snapshot_date.dt.to_period("M").astype(str)
    ratio = inventory.quantity_available / inventory.reorder_level.clip(lower=1)
    inventory["critical_positions"] = (ratio < 0.5).astype(int)
    inventory["low_positions"] = ((ratio >= 0.5) & (ratio < 1)).astype(int)
    inventory["optimal_positions"] = ((ratio >= 1) & (ratio < 3)).astype(int)
    inventory["excess_positions"] = (ratio >= 3).astype(int)
    inventory["risk_sku"] = inventory.apply(
        lambda row: str(int(row.product_id)) if row.quantity_available < row.reorder_level * 0.5 else "", axis=1
    )

    def risk_ids(values: Iterable[str]) -> str:
        return ",".join(sorted({value for value in values if value}, key=int))

    return (
        inventory.groupby(["period", "warehouse_id", "category", "supplier_id"], observed=True)
        .agg(
            on_hand=("quantity_on_hand", "sum"),
            available=("quantity_available", "sum"),
            critical_positions=("critical_positions", "sum"),
            low_positions=("low_positions", "sum"),
            optimal_positions=("optimal_positions", "sum"),
            excess_positions=("excess_positions", "sum"),
            risk_sku_ids=("risk_sku", risk_ids),
        )
        .reset_index()
    )


def build_cube(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    keys = ["period", "warehouse_id", "category", "supplier_id"]
    cube = _sales_cube(tables).merge(_orders_cube(tables), on=keys, how="outer")
    cube = cube.merge(_inventory_cube(tables), on=keys, how="outer")
    numeric = [column for column in cube.columns if column not in keys + ["risk_sku_ids"]]
    cube[numeric] = cube[numeric].fillna(0)
    cube["risk_sku_ids"] = cube.risk_sku_ids.fillna("")
    cube["revenue"] = cube.revenue.round(2)
    cube["late_days"] = cube.late_days.round(2)
    return cube.sort_values(keys).reset_index(drop=True)


def calculate_filtered_kpis(
    cube: pd.DataFrame,
    start_period: str | None = None,
    end_period: str | None = None,
    warehouse_id: int | None = None,
    category: str | None = None,
    supplier_id: int | None = None,
) -> dict:
    filtered = cube
    if start_period:
        filtered = filtered[filtered.period >= start_period]
    if end_period:
        filtered = filtered[filtered.period <= end_period]
    if warehouse_id:
        filtered = filtered[filtered.warehouse_id == warehouse_id]
    if category:
        filtered = filtered[filtered.category == category]
    if supplier_id:
        filtered = filtered[filtered.supplier_id == supplier_id]

    completed = int(filtered.completed_orders.sum())
    on_time = int(filtered.on_time_orders.sum())
    latest_period = filtered.period.max() if len(filtered) else None
    point_in_time = filtered[filtered.period == latest_period] if latest_period else filtered
    risk_ids = set()
    for value in point_in_time.risk_sku_ids:
        risk_ids.update(item for item in str(value).split(",") if item)
    return {
        "revenue": round(float(filtered.revenue.sum()), 2),
        "units": int(filtered.units.sum()),
        "on_time_delivery_rate": round(on_time * 100 / completed, 2) if completed else 0.0,
        "at_risk_skus": len(risk_ids),
        "purchase_orders": int(filtered.purchase_orders.sum()),
        "inventory_health": {
            status: int(point_in_time[f"{status}_positions"].sum())
            for status in ("critical", "low", "optimal", "excess")
        },
    }


def build_dashboard_payload(
    tables: Dict[str, pd.DataFrame],
    quality_rules: list[dict],
    lineage: list[dict],
    scale: str,
    seed: int,
) -> dict:
    cube = build_cube(tables)
    products = tables["products"]
    suppliers = tables["suppliers"]
    warehouses = tables["warehouses"]
    periods = sorted(cube.period.unique().tolist())
    global_kpis = calculate_filtered_kpis(cube)

    anomalies = {
        "demand_spikes": int((tables["sales"].anomaly_type == "DEMAND_SPIKE").sum()),
        "extreme_delays": int((tables["orders"].anomaly_type == "EXTREME_DELAY").sum()),
        "price_jumps": int((tables["price_history"].anomaly_type == "PRICE_JUMP").sum()),
        "controlled_stock_risks": int(tables["inventory"].controlled_risk.sum()),
    }

    supplier_performance = []
    orders = tables["orders"]
    for supplier_id, group in orders.groupby("supplier_id"):
        completed = group[group.actual_delivery_date.notna()]
        on_time = int((completed.delivery_delay_days <= 0).sum())
        supplier = suppliers.loc[suppliers.supplier_id == supplier_id].iloc[0]
        supplier_performance.append(
            {
                "supplier_id": int(supplier_id),
                "supplier_name": supplier.supplier_name,
                "orders": int(len(group)),
                "on_time_rate": round(on_time * 100 / len(completed), 2) if len(completed) else 0,
                "avg_delay_days": round(float(completed.delivery_delay_days.clip(lower=0).mean()), 2),
                "reliability_score": float(supplier.reliability_score),
            }
        )

    return {
        "meta": {
            "scale": scale,
            "seed": seed,
            "source_rows": int(sum(len(frame) for frame in tables.values())),
            "start_period": periods[0],
            "end_period": periods[-1],
            "cube_rows": len(cube),
        },
        "dimensions": {
            "periods": periods,
            "warehouses": warehouses[["warehouse_id", "warehouse_name", "location"]].to_dict("records"),
            "categories": sorted(products.category.unique().tolist()),
            "suppliers": suppliers[["supplier_id", "supplier_name"]].to_dict("records"),
        },
        "summary": global_kpis,
        "cube": cube.to_dict("records"),
        "supplier_performance": supplier_performance,
        "anomalies": anomalies,
        "quality": {
            "rules": quality_rules,
            "passed": sum(1 for rule in quality_rules if rule["passed"]),
            "failed": sum(1 for rule in quality_rules if not rule["passed"]),
        },
        "lineage": lineage,
    }
