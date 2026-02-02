# Supply Chain Intelligence Hub

**A comprehensive data analytics platform demonstrating enterprise-level data engineering and analytics capabilities.**


<img width="1065" height="702" alt="image" src="https://github.com/user-attachments/assets/82815622-f52f-4ad4-b891-4321da712cf5" />



```text
supply-chain-intelligence-hub/
├── README.md
Supply-Chain-Intelligence-Hub
├── docker/
│   ├── Dockerfile.python        # Python analytics engine
│   ├── Dockerfile.r             # R analytics engine
│   └── Dockerfile.database      # PostgreSQL with sample data
├── docker-compose.yml           # Orchestrates all 3
├── .dockerignore
├── data/
│   └── python-insert-data.ipynb
├── scripts/
│   ├── python
│	│	 ├── etl_pipeline.ipynb        	   #  ETL - error handling,data quality validation, and logging.
│   │	 ├── data_quality_framework.ipynb  #  with profiling, anomaly detection, and automated reporting.
│   │	 └── python_analysis.py
│   └── r_analysis.R
│
├── sql/
│   ├── 1-init.sql		 			# Database + tables
│   ├── 2-sql-insert-data.sql 		# Insert main data into the new tables
│   └── 3-Stored-Procedures.sql 	# For quick analytics
│
├── requirements.txt             # Python dependencies
├── environment.yml              # R dependencies
└── README.md

```
## 🎯 Project Overview

This portfolio project showcases a complete **end-to-end data analytics solution** built for supply chain optimization. It demonstrates mastery across:

- **SQL**: Advanced database design, CTEs, window functions, stored procedures
- **Python**: ETL pipelines, data validation, statistical analysis
- **R**: Time series forecasting, statistical analysis, interactive visualizations
- **Power BI**: Executive dashboards, KPI tracking, interactive reports

## 📊 Key Features

### **SQL**
- ✅ Dimensional data modeling (star schema)
- ✅ Complex CTEs and window functions
- ✅ Stored procedures for automated analytics
- ✅ Query optimization and indexing strategy
- ✅ Real-time performance monitoring

### **Python**
- ✅ ETL pipeline with error handling
- ✅ Data quality validation framework
- ✅ Statistical anomaly detection (Isolation Forest)
- ✅ Feature engineering and transformation
- ✅ Logging and monitoring

### **R**
- ✅ ARIMA time series forecasting
- ✅ Statistical hypothesis testing
- ✅ Supplier performance analysis
- ✅ Inventory optimization (EOQ model)
- ✅ Interactive Plotly visualizations

### **Power BI**
- ✅ 4-page interactive dashboard
- ✅ DAX formulas for calculated metrics
- ✅ Real-time KPI tracking
- ✅ Cross-filtering and drill-through
- ✅ Executive summary & detailed analytics

## 🏗️ Architecture
