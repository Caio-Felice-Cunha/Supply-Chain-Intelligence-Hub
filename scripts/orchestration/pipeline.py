"""End-to-end portfolio pipeline: generate → MySQL → ETL → publish."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy import text

from scripts.analytics import build_dashboard_payload, transform_dataset
from scripts.etl import DatabaseConnection, ETLConfig
from scripts.generation import generate_dataset, get_scale
from scripts.orchestration.publisher import json_bytes, publish_site
from scripts.quality.contracts import assert_valid, validate_dataset


SOURCE_ORDER = ["suppliers", "warehouses", "products", "inventory", "sales", "orders", "price_history"]
DELETE_ORDER = ["price_history", "sales", "orders", "inventory", "products", "warehouses", "suppliers"]


def _logger() -> logging.Logger:
    logger = logging.getLogger("portfolio_pipeline")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _record_stage(lineage: list[dict], name: str, started: float, rows_in: int, rows_out: int, state: str = "PASS") -> None:
    lineage.append(
        {
            "stage": name,
            "state": state,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    )


def _load_sources(connection: DatabaseConnection, tables: Dict[str, pd.DataFrame]) -> None:
    with connection.engine.begin() as transaction:
        for table in DELETE_ORDER:
            transaction.execute(text(f"DELETE FROM {table}"))
    for table in SOURCE_ORDER:
        tables[table].to_sql(
            table,
            connection.engine,
            if_exists="append",
            index=False,
            chunksize=1_000,
            method="multi",
        )


def _extract_all(connection: DatabaseConnection) -> Dict[str, pd.DataFrame]:
    primary_keys = {
        "suppliers": "supplier_id",
        "warehouses": "warehouse_id",
        "products": "product_id",
        "inventory": "inventory_id",
        "sales": "sale_id",
        "orders": "order_id",
        "price_history": "price_history_id",
    }
    return {
        table: pd.read_sql_query(text(f"SELECT * FROM {table} ORDER BY {primary_keys[table]}"), connection.engine)
        for table in SOURCE_ORDER
    }


def _load_processed(connection: DatabaseConnection, tables: Dict[str, pd.DataFrame]) -> None:
    for table in SOURCE_ORDER:
        tables[table].to_sql(
            f"{table}_processed",
            connection.engine,
            if_exists="replace",
            index=False,
            chunksize=1_000,
            method="multi",
        )


def run_demo_pipeline(
    scale: str = "portfolio",
    seed: int = 42,
    output: str | Path = "site",
    database: str = "mysql",
) -> dict:
    """Run the release pipeline and return a machine-readable execution summary."""

    config = get_scale(scale)
    logger = _logger()
    lineage: list[dict] = []
    started_pipeline = time.perf_counter()

    started = time.perf_counter()
    generated = generate_dataset(scale, seed)
    source_rows = sum(len(frame) for frame in generated.values())
    _record_stage(lineage, "generate", started, 0, source_rows)
    logger.info("Generated %s source rows with seed %s", f"{source_rows:,}", seed)

    connection = None
    if database == "mysql":
        started = time.perf_counter()
        connection = DatabaseConnection(ETLConfig(), logger).__enter__()
        try:
            _load_sources(connection, generated)
            _record_stage(lineage, "mysql_load", started, source_rows, source_rows)
            started = time.perf_counter()
            extracted = _extract_all(connection)
            _record_stage(lineage, "extract", started, source_rows, sum(len(frame) for frame in extracted.values()))
        except Exception:
            connection.__exit__(*__import__("sys").exc_info())
            raise
    else:
        extracted = {name: frame.copy() for name, frame in generated.items()}
        now = time.perf_counter()
        _record_stage(lineage, "mysql_load", now, source_rows, source_rows, "SKIPPED")
        _record_stage(lineage, "extract", now, source_rows, source_rows, "IN_MEMORY")

    started = time.perf_counter()
    processed = transform_dataset(extracted)
    processed_rows = sum(len(frame) for frame in processed.values())
    _record_stage(lineage, "transform", started, source_rows, processed_rows)

    started = time.perf_counter()
    rules = validate_dataset(extracted, config, processed)
    assert_valid(rules)
    _record_stage(lineage, "validate", started, processed_rows, processed_rows)

    started = time.perf_counter()
    if connection is not None:
        _load_processed(connection, processed)
        connection.__exit__(None, None, None)
        state = "PASS"
    else:
        state = "IN_MEMORY"
    _record_stage(lineage, "processed_load", started, processed_rows, processed_rows, state)

    started = time.perf_counter()
    dashboard = build_dashboard_payload(processed, rules, lineage, scale, seed)
    _record_stage(lineage, "aggregate", started, processed_rows, dashboard["meta"]["cube_rows"])
    dashboard["lineage"] = lineage

    manifest = {
        "project": "Supply Chain Intelligence Hub",
        "scale": scale,
        "seed": seed,
        "date_range": [config.start_date.isoformat(), config.end_date.isoformat()],
        "source_counts": {name: len(frame) for name, frame in generated.items()},
        "source_rows": source_rows,
        "structural_integrity": "PASS",
    }
    pipeline_run = {
        "tool": "supply-chain-intelligence-hub",
        "mode": scale,
        "seed": seed,
        "database": database,
        "source_rows": source_rows,
        "processed_rows": processed_rows,
        "stages": lineage,
        "summary": {
            "state": "PASS",
            "quality_rules": len(rules),
            "quality_failures": sum(1 for rule in rules if not rule["passed"]),
            "duration_ms": round((time.perf_counter() - started_pipeline) * 1000, 2),
        },
    }

    started = time.perf_counter()
    published_manifest = publish_site(Path(output), processed, dashboard, manifest, pipeline_run)
    _record_stage(lineage, "publish", started, dashboard["meta"]["cube_rows"], len(published_manifest))
    dashboard["lineage"] = lineage
    pipeline_run["stages"] = lineage
    pipeline_run["summary"]["duration_ms"] = round((time.perf_counter() - started_pipeline) * 1000, 2)
    (Path(output) / "data" / "dashboard.json").write_bytes(json_bytes(dashboard))
    (Path(output) / "data" / "pipeline-run.json").write_bytes(json_bytes(pipeline_run))
    logger.info("Published dashboard, report, samples, and dataset archive to %s", output)
    return pipeline_run | {"manifest": published_manifest, "lineage": lineage}
