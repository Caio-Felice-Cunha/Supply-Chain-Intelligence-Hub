# Supply Chain Intelligence Hub

**An end-to-end supply chain analytics build: one command spins up a MySQL database and a Python analytics engine, then an ETL pipeline with a data quality framework turns raw operations data into decision-ready analytics.**

<img width="1065" height="702" alt="Supply Chain Intelligence Hub dashboard" src="https://github.com/user-attachments/assets/82815622-f52f-4ad4-b891-4321da712cf5" />

## What this demonstrates

A supply chain runs on questions like: which suppliers are slipping, which inventory is about to run out, what will demand look like next quarter. This project builds the path from raw data to those answers:

- **SQL**: dimensional modeling (star schema), CTEs, window functions, and stored procedures for repeatable analytics
- **Python ETL**: an extract, transform, validate pipeline with logging and error handling
- **Data quality**: a rules engine, statistical profiling, anomaly detection (IQR and Isolation Forest), and a generated HTML quality report

## Quick start

```bash
git clone https://github.com/Caio-Felice-Cunha/Supply-Chain-Intelligence-Hub.git
cd Supply-Chain-Intelligence-Hub
docker compose up
```

This starts two services:

- **MySQL** with the schema and seed data auto-loaded from `sql/`. It is published on host port **3307** (mapped to the container's 3306), so you can connect a SQL client to `localhost:3307`.
- **Jupyter Lab** with the Python analytics stack. Open it at **http://localhost:8889/?token=analytics**.

In Jupyter, run `notebooks/python/Documentation/run_complete_pipeline.ipynb` to execute the ETL and data quality pipeline against the seeded database.

> The MySQL credentials in this repo (`analytics_user` / `analyticspass123`) are local demo defaults for the bundled container, not production secrets. Override them with environment variables (see `.env.example`); the ETL code reads `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, and `DB_NAME`.

## Results

These numbers come from the report and log artifacts committed in this repo. They are real outputs of the pipeline against the seeded database, not estimates.

**Data quality report** (`outputs/reports/supply_chain_quality_report.html`, generated 2026-02-01 23:55:45):

- 2 tables profiled: `products` = 46 rows / 6 columns, `sales` = 166 rows / 6 columns, 0 duplicate rows.
- Validation rules `product_id_unique`, `unit_cost_positive`, and `quantity_sold_positive` all PASS.
- IQR outliers detected: `products.unit_cost` 5 (10.87%), `products.reorder_level` 1 (2.17%), `sales.quantity_sold` 6 (3.61%), `sales.revenue` 6 (3.61%).

**Quality summary** (`outputs/quality_summary.json`): 3 validation rules executed, 3 passed, 0 failed, 100.00% pass rate; 18 total outliers detected across the 2 tables (212 total rows, 12 columns).

**Full pipeline run** (logged 2026-02-01 23:52:44): 5 tables processed, 572 rows extracted, duration 0.30 seconds, all tables processed successfully. The validator flagged one quality warning: the `orders` table's `delivery_delay_days` column had 46.3% nulls against a 5.0% threshold (orders still in transit have no actual delivery date yet).

## A note on the ETL "load" step

The pipeline is currently extract, transform, and validate. The load step (writing cleaned rows back to `*_processed` tables) is implemented in `scripts/etl/loader.py` but left commented out in the orchestrator (`scripts/orquestration/pipeline.py`), so a default run reports 0 rows loaded. Uncomment the LOAD block in `_process_table` to enable it; `DataLoader` already supports batching and pre-replace backups.

## Tests

The analytics modules have an offline test suite (no MySQL or Docker needed). It runs against synthetic DataFrames and covers the transformer, the quality rules engine, the profiler, the anomaly detector, the extractor's table allowlist, and the report writer.

```bash
pip install -r requirements-dev.txt
pytest
```

CI runs the same suite on Python 3.11, 3.12, and 3.13 (see `.github/workflows/tests.yml`).

## Project structure

```text
Supply-Chain-Intelligence-Hub/
│
├── docker/
│   ├── Dockerfile.jupyter        # Python analytics engine
│   └── Dockerfile.mysql          # MySQL with sample data
│
├── data/
│   └── python-insert-data.ipynb  # Optional bulk synthetic-data inserts
│
├── scripts/
│   ├── etl/                      # config, connection, extractor, transformer, loader, validator
│   ├── quality/                  # profiler, anomaly detection, reporter, rules engine
│   └── orquestration/            # pipeline orchestrator
│
├── sql/
│   ├── 1-init.sql                # Database + tables
│   ├── 2-sql-insert-data.sql     # Seed data
│   └── 3-Stored-Procedures.sql   # Quick analytics
│
├── notebooks/
│   └── python/                   # data exploration + Documentation/run_complete_pipeline.ipynb
│
├── outputs/                      # Generated quality report + summary JSON
├── tests/                        # Offline pytest suite
├── requirements.txt              # Python dependencies
├── requirements-dev.txt          # Test dependencies
└── docker-compose.yml            # Orchestrates the services
```

## Roadmap

The following were part of the original plan and are not yet in the repo:

- **R analysis**: ARIMA time series forecasting, hypothesis testing, supplier performance analysis, and EOQ inventory optimization. The Jupyter image already installs an R kernel and packages, so these would slot into `notebooks/`.
- **Power BI**: an interactive dashboard with DAX metrics, KPI tracking, and drill-through.

## License

MIT. See [LICENSE](./LICENSE).
