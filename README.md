# Supply Chain Intelligence Hub

**An end-to-end supply chain analytics build: one command spins up a MySQL database and a Python + R analytics engine, then an ETL pipeline with a data quality framework turns raw operations data into decision-ready analytics.**

<img width="1065" height="702" alt="Supply Chain Intelligence Hub dashboard" src="https://github.com/user-attachments/assets/82815622-f52f-4ad4-b891-4321da712cf5" />

## What this demonstrates

A supply chain runs on questions like: which suppliers are slipping, which inventory is about to run out, what will demand look like next quarter. This project builds the full path from raw data to those answers:

- **SQL**: dimensional modeling (star schema), CTEs, window functions, and stored procedures for repeatable analytics
- **Python**: an ETL pipeline (extract, transform, validate, load) with logging and error handling
- **Data quality**: profiling, statistical anomaly detection (Isolation Forest), and a generated HTML quality report
- **R**: ARIMA time series forecasting, hypothesis testing, supplier performance analysis, and EOQ inventory optimization
- **Power BI**: a 4-page interactive dashboard with DAX metrics, KPI tracking, and drill-through

## Quick start

```bash
git clone https://github.com/Caio-Felice-Cunha/Supply-Chain-Intelligence-Hub.git
cd Supply-Chain-Intelligence-Hub
docker compose up
```

This starts MySQL (schema and seed data auto-load from `sql/`) and a Jupyter service with the full Python and R stack. Open the Jupyter service and run `notebooks/python/run_complete_pipeline.ipynb` to execute the complete ETL and data quality pipeline.

## Project structure

```text
Supply-Chain-Intelligence-Hub/
│
├── docker/
│   ├── Dockerfile.jupyter        # Python + R analytics engine
│   └── Dockerfile.mysql          # MySQL with sample data
│
├── data/
│   └── python-insert-data.ipynb
│
├── scripts/
│   ├── etl/                      # config, connection, extractor, transformer, loader, validator
│   └── quality/                  # profiler, anomaly detection, reporter, rules engine
│
├── sql/
│   ├── 1-init.sql                # Database + tables
│   ├── 2-sql-insert-data.sql     # Seed data
│   └── 3-Stored-Procedures.sql   # Quick analytics
│
├── notebooks/
│   └── python/                   # pipeline runner, analysis notebooks, quality report
│
├── requirements.txt              # Python dependencies
├── environment.yml               # R dependencies
└── docker-compose.yml            # Orchestrates the services
```

## License

MIT. See [LICENSE](./LICENSE).
