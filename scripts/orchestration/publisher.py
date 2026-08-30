"""Publish deterministic data artifacts and a static engineering case."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import zipfile
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd


SCHEMA = {
    "suppliers": "Supplier master data and reliability assumptions.",
    "products": "SKU, category, demand class, economics, reorder point, and owner supplier.",
    "warehouses": "Distribution-centre capacity and location dimensions.",
    "inventory": "Monthly SKU-by-warehouse inventory snapshots.",
    "sales": "Transaction facts with promotions and controlled demand spikes.",
    "orders": "Purchase orders with expected/actual delivery outcomes.",
    "price_history": "Supplier cost changes and controlled price jumps.",
}


def _json_clean(value):
    if isinstance(value, dict):
        return {str(key): _json_clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_clean(item) for item in value]
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return None if pd.isna(value) else pd.Timestamp(value).isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float) and np.isnan(value):
        return None
    if value is pd.NaT:
        return None
    return value


def json_bytes(payload: object) -> bytes:
    return (json.dumps(_json_clean(payload), indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    clean = frame.copy()
    for column in clean.columns:
        if pd.api.types.is_datetime64_any_dtype(clean[column]):
            clean[column] = clean[column].dt.strftime("%Y-%m-%d")
    return clean.to_csv(index=False, lineterminator="\n", float_format="%.2f").encode("utf-8")


def _write_zip_member(archive: zipfile.ZipFile, name: str, data: bytes) -> None:
    info = zipfile.ZipInfo(name, date_time=(2024, 7, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    archive.writestr(info, data)


def build_dataset_archive(tables: Dict[str, pd.DataFrame], manifest: dict) -> tuple[bytes, dict]:
    table_checksums = {}
    csv_payloads = {}
    for name, frame in tables.items():
        payload = _csv_bytes(frame)
        csv_payloads[name] = payload
        table_checksums[name] = hashlib.sha256(payload).hexdigest()

    archive_manifest = {**manifest, "table_sha256": table_checksums, "schema": SCHEMA}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(csv_payloads):
            _write_zip_member(archive, f"data/{name}.csv", csv_payloads[name])
        _write_zip_member(archive, "manifest.json", json_bytes(archive_manifest))
        _write_zip_member(archive, "schema.json", json_bytes({"tables": SCHEMA}))
    return buffer.getvalue(), archive_manifest


def publish_site(
    output: Path,
    tables: Dict[str, pd.DataFrame],
    dashboard: dict,
    manifest: dict,
    pipeline_run: dict,
) -> dict:
    """Copy static assets and write all generated evidence below ``output``."""

    static_root = Path(__file__).with_name("static")
    output.mkdir(parents=True, exist_ok=True)
    shutil.copytree(static_root, output, dirs_exist_ok=True)
    for name in ("data", "downloads"):
        target = output / name
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
    (output / "report").mkdir(parents=True, exist_ok=True)

    archive_bytes, archive_manifest = build_dataset_archive(tables, manifest)
    archive_path = output / "downloads" / "supply-chain-dataset-seed-42.zip"
    archive_path.write_bytes(archive_bytes)
    archive_checksum = hashlib.sha256(archive_bytes).hexdigest()
    (output / "downloads" / "supply-chain-dataset-sha256.txt").write_text(
        f"{archive_checksum}  {archive_path.name}\n", encoding="utf-8"
    )

    samples = {
        name: frame.head(12).where(pd.notna(frame), None).to_dict("records")
        for name, frame in tables.items()
    }
    generated_manifest = {
        **archive_manifest,
        "archive": archive_path.name,
        "archive_sha256": archive_checksum,
    }
    (output / "data" / "dashboard.json").write_bytes(json_bytes(dashboard))
    (output / "data" / "samples.json").write_bytes(json_bytes(samples))
    (output / "data" / "manifest.json").write_bytes(json_bytes(generated_manifest))
    (output / "data" / "pipeline-run.json").write_bytes(json_bytes(pipeline_run))
    (output / "report" / "quality-rules.json").write_bytes(json_bytes(dashboard["quality"]))
    return generated_manifest
