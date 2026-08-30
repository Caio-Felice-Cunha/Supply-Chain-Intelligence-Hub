"""Deterministic, structurally valid supply-chain portfolio dataset.

The generator intentionally models business irregularities (promotion spikes,
late deliveries, price jumps, and low stock) without inserting malformed rows.
Seed 42 and the portfolio scale are the public release contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict

import numpy as np
import pandas as pd


PORTFOLIO_COUNTS = {
    "suppliers": 30,
    "products": 150,
    "warehouses": 6,
    "inventory": 21_600,
    "sales": 70_000,
    "orders": 8_000,
    "price_history": 2_000,
}


@dataclass(frozen=True)
class ScaleConfig:
    name: str
    start_date: date
    end_date: date
    suppliers: int
    products: int
    warehouses: int
    inventory_months: int
    sales: int
    orders: int
    price_history: int

    @property
    def expected_counts(self) -> Dict[str, int]:
        return {
            "suppliers": self.suppliers,
            "products": self.products,
            "warehouses": self.warehouses,
            "inventory": self.products * self.warehouses * self.inventory_months,
            "sales": self.sales,
            "orders": self.orders,
            "price_history": self.price_history,
        }


SCALES = {
    "smoke": ScaleConfig(
        name="smoke",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 6, 30),
        suppliers=6,
        products=18,
        warehouses=3,
        inventory_months=6,
        sales=1_000,
        orders=200,
        price_history=90,
    ),
    "portfolio": ScaleConfig(
        name="portfolio",
        start_date=date(2024, 7, 1),
        end_date=date(2026, 6, 30),
        suppliers=30,
        products=150,
        warehouses=6,
        inventory_months=24,
        sales=70_000,
        orders=8_000,
        price_history=2_000,
    ),
}


def get_scale(name: str) -> ScaleConfig:
    try:
        return SCALES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown scale {name!r}; choose from {sorted(SCALES)}") from exc


def _dates_from_offsets(start: pd.Timestamp, offsets: np.ndarray) -> pd.Series:
    return pd.Series(start + pd.to_timedelta(offsets, unit="D"))


def _suppliers(config: ScaleConfig) -> pd.DataFrame:
    countries = ["Canada", "United States", "Mexico", "Germany", "Japan", "Brazil"]
    rows = []
    for supplier_id in range(1, config.suppliers + 1):
        reliability = 72 + ((supplier_id * 11) % 27) + (supplier_id % 3) * 0.25
        rows.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": f"Supplier {supplier_id:02d}",
                "country": countries[(supplier_id - 1) % len(countries)],
                "reliability_score": round(min(reliability, 99.0), 2),
                "lead_time_days": 5 + ((supplier_id * 7) % 24),
                "created_date": pd.Timestamp(config.start_date),
            }
        )
    return pd.DataFrame(rows)


def _products(config: ScaleConfig) -> pd.DataFrame:
    categories = ["Electronics", "Home", "Outdoor", "Office", "Wellness", "Kitchen"]
    rows = []
    for product_id in range(1, config.products + 1):
        percentile = ((product_id * 37) % 100) / 100
        demand_class = "A" if percentile < 0.20 else "B" if percentile < 0.50 else "C"
        unit_cost = round(7.5 + ((product_id * 13) % 185) + (product_id % 7) * 0.37, 2)
        rows.append(
            {
                "product_id": product_id,
                "product_name": f"SKU {product_id:03d}",
                "category": categories[(product_id - 1) % len(categories)],
                "demand_class": demand_class,
                "unit_cost": unit_cost,
                "base_price": round(unit_cost * (1.32 + (product_id % 9) * 0.025), 2),
                "reorder_level": 24 + ((product_id * 17) % 115),
                "supplier_id": 1 + ((product_id * 7 - 1) % config.suppliers),
            }
        )
    return pd.DataFrame(rows)


def _warehouses(config: ScaleConfig) -> pd.DataFrame:
    names = ["Pacific", "Prairie", "Central", "Atlantic", "North", "Metro"]
    locations = ["Vancouver", "Calgary", "Toronto", "Halifax", "Edmonton", "Montreal"]
    rows = []
    for warehouse_id in range(1, config.warehouses + 1):
        rows.append(
            {
                "warehouse_id": warehouse_id,
                "warehouse_name": f"{names[warehouse_id - 1]} DC",
                "location": locations[warehouse_id - 1],
                "capacity_units": 55_000 + warehouse_id * 8_500,
                "current_utilization_pct": round(57 + warehouse_id * 4.3, 2),
            }
        )
    return pd.DataFrame(rows)


def _inventory(config: ScaleConfig, products: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    month_ends = pd.date_range(config.start_date, config.end_date, freq="ME")
    month_ends = month_ends[-config.inventory_months :]
    product_rows = products.set_index("product_id")
    rows = []
    inventory_id = 1
    demand_factor = {"A": 1.55, "B": 1.15, "C": 0.82}
    for month_index, snapshot in enumerate(month_ends):
        seasonal = 1.18 if snapshot.month in (11, 12) else 0.91 if snapshot.month in (1, 2) else 1.0
        for product_id in range(1, config.products + 1):
            product = product_rows.loc[product_id]
            for warehouse_id in range(1, config.warehouses + 1):
                baseline = product.reorder_level * demand_factor[product.demand_class] * seasonal
                wave = 1 + 0.22 * np.sin((month_index + product_id % 6) * np.pi / 6)
                quantity = max(0, int(round(baseline * wave + rng.normal(22, 15))))
                controlled_risk = (product_id * 5 + warehouse_id * 3 + month_index) % 29 == 0
                if controlled_risk:
                    quantity = max(1, int(product.reorder_level * (0.28 + (product_id % 5) * 0.1)))
                reserved = min(quantity, max(0, int(round(quantity * rng.uniform(0.04, 0.27)))))
                rows.append(
                    {
                        "inventory_id": inventory_id,
                        "product_id": product_id,
                        "warehouse_id": warehouse_id,
                        "quantity_on_hand": quantity,
                        "quantity_reserved": reserved,
                        "snapshot_date": snapshot,
                        "controlled_risk": bool(controlled_risk),
                    }
                )
                inventory_id += 1
    return pd.DataFrame(rows)


def _sales(config: ScaleConfig, products: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    day_count = (pd.Timestamp(config.end_date) - pd.Timestamp(config.start_date)).days
    product_ids = products.product_id.to_numpy()
    demand_weight = products.demand_class.map({"A": 5.0, "B": 2.5, "C": 1.0}).to_numpy()
    product_choice = rng.choice(product_ids, config.sales, p=demand_weight / demand_weight.sum())
    offsets = rng.integers(0, day_count + 1, size=config.sales)
    offsets[0], offsets[-1] = 0, day_count
    sale_dates = _dates_from_offsets(pd.Timestamp(config.start_date), offsets)
    warehouse_ids = rng.integers(1, config.warehouses + 1, size=config.sales)
    product_lookup = products.set_index("product_id")
    promotion = rng.random(config.sales) < 0.14
    anomaly = np.arange(1, config.sales + 1) % 997 == 0
    rows = []
    base_quantity = {"A": 5.4, "B": 3.2, "C": 1.8}
    for index in range(config.sales):
        product_id = int(product_choice[index])
        product = product_lookup.loc[product_id]
        month = sale_dates.iloc[index].month
        seasonal = 1.38 if month in (11, 12) else 1.16 if month in (4, 5) else 0.86 if month == 1 else 1.0
        promo_multiplier = 2.1 if promotion[index] else 1.0
        quantity = max(1, int(rng.poisson(base_quantity[product.demand_class] * seasonal * promo_multiplier)))
        if anomaly[index]:
            quantity *= 7
        unit_price = round(product.base_price * (0.87 if promotion[index] else 1.0) * rng.uniform(0.98, 1.03), 2)
        rows.append(
            {
                "sale_id": index + 1,
                "sale_date": sale_dates.iloc[index],
                "product_id": product_id,
                "warehouse_id": int(warehouse_ids[index]),
                "quantity_sold": quantity,
                "unit_price": unit_price,
                "revenue": round(quantity * unit_price, 2),
                "is_promotion": bool(promotion[index]),
                "anomaly_type": "DEMAND_SPIKE" if anomaly[index] else "",
            }
        )
    return pd.DataFrame(rows)


def _orders(
    config: ScaleConfig,
    products: pd.DataFrame,
    suppliers: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    day_count = (pd.Timestamp(config.end_date) - pd.Timestamp(config.start_date)).days
    offsets = rng.integers(0, day_count + 1, size=config.orders)
    offsets[0], offsets[-1] = 0, day_count
    order_dates = _dates_from_offsets(pd.Timestamp(config.start_date), offsets)
    product_ids = rng.integers(1, config.products + 1, size=config.orders)
    product_lookup = products.set_index("product_id")
    supplier_lookup = suppliers.set_index("supplier_id")
    rows = []
    for index in range(config.orders):
        product_id = int(product_ids[index])
        product = product_lookup.loc[product_id]
        supplier_id = int(product.supplier_id)
        supplier = supplier_lookup.loc[supplier_id]
        order_date = order_dates.iloc[index]
        expected = order_date + pd.Timedelta(days=int(supplier.lead_time_days))
        late_probability = max(0.05, (100 - supplier.reliability_score) / 65)
        is_late = rng.random() < late_probability
        delay = int(rng.integers(1, 10)) if is_late else -int(rng.integers(0, 4))
        controlled_anomaly = (index + 1) % 211 == 0
        if controlled_anomaly:
            delay = 21
        predicted_actual = expected + pd.Timedelta(days=delay)
        if predicted_actual > pd.Timestamp(config.end_date):
            actual = pd.NaT
            status = "IN_TRANSIT"
        else:
            actual = predicted_actual
            status = "LATE" if delay > 0 else "DELIVERED"
        quantity = int(rng.integers(40, 620))
        rows.append(
            {
                "order_id": index + 1,
                "order_date": order_date,
                "supplier_id": supplier_id,
                "product_id": product_id,
                "warehouse_id": int(rng.integers(1, config.warehouses + 1)),
                "order_quantity": quantity,
                "order_cost": round(quantity * product.unit_cost * rng.uniform(0.96, 1.04), 2),
                "expected_delivery_date": expected,
                "actual_delivery_date": actual,
                "delivery_status": status,
                "anomaly_type": "EXTREME_DELAY" if controlled_anomaly else "",
            }
        )
    return pd.DataFrame(rows)


def _price_history(config: ScaleConfig, products: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    day_count = (pd.Timestamp(config.end_date) - pd.Timestamp(config.start_date)).days
    product_lookup = products.set_index("product_id")
    rows = []
    for index in range(config.price_history):
        product_id = 1 + (index % config.products)
        product = product_lookup.loc[product_id]
        cycle = index // config.products
        offset = min(day_count, int(round(cycle * day_count / max(1, config.price_history // config.products))))
        if index == config.price_history - 1:
            offset = day_count
        market_shift = 1 + 0.018 * cycle + 0.035 * np.sin((product_id + cycle) / 4)
        controlled_anomaly = (index + 1) % 389 == 0
        if controlled_anomaly:
            market_shift *= 1.28
        rows.append(
            {
                "price_history_id": index + 1,
                "product_id": product_id,
                "supplier_id": int(product.supplier_id),
                "unit_price": round(product.unit_cost * market_shift * rng.uniform(0.985, 1.015), 2),
                "effective_date": pd.Timestamp(config.start_date) + pd.Timedelta(days=offset),
                "anomaly_type": "PRICE_JUMP" if controlled_anomaly else "",
            }
        )
    return pd.DataFrame(rows)


def generate_dataset(scale: str = "portfolio", seed: int = 42) -> Dict[str, pd.DataFrame]:
    """Generate all seven tables with stable row order and primary keys."""

    config = get_scale(scale)
    rng = np.random.default_rng(seed)
    suppliers = _suppliers(config)
    products = _products(config)
    warehouses = _warehouses(config)
    return {
        "suppliers": suppliers,
        "products": products,
        "warehouses": warehouses,
        "inventory": _inventory(config, products, rng),
        "sales": _sales(config, products, rng),
        "orders": _orders(config, products, suppliers, rng),
        "price_history": _price_history(config, products, rng),
    }
