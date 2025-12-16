# Databricks FRED Data Pipeline

A comprehensive data pipeline for fetching, storing, and transforming Federal Reserve Economic Data (FRED) in Databricks using the medallion architecture.

## Overview

This project creates and maintains FRED economic data in a Databricks lakehouse with:

- **Bronze Layer**: Raw data ingestion using Streaming Tables with Auto Loader (cloud_files)
- **Silver Layer**: Cleaned, typed data using Streaming Tables with constraints
- **Gold Layer**: Business-ready denormalized Materialized View

## Data Sources

### Rate Series
| Series ID | Description |
|-----------|-------------|
| DFF | Federal Funds Effective Rate |
| DTB3 | 3-Month Treasury Bill Rate |
| DGS1 | 1-Year Treasury Rate |
| DGS2 | 2-Year Treasury Rate |
| DGS5 | 5-Year Treasury Rate |
| DGS10 | 10-Year Treasury Rate |
| DGS30 | 30-Year Treasury Rate |
| SOFR | Secured Overnight Financing Rate |
| DPRIME | Bank Prime Loan Rate |
| MORTGAGE30US | 30-Year Fixed Rate Mortgage |
| MORTGAGE15US | 15-Year Fixed Rate Mortgage |
| BAMLC0A0CM | ICE BofA US Corporate Index Yield |
| BAMLH0A0HYM2 | ICE BofA US High Yield Index |
| T10YIE | 10-Year Breakeven Inflation Rate |
| T5YIE | 5-Year Breakeven Inflation Rate |

## Features

- ✅ Automated FRED API data fetching
- ✅ Medallion architecture (Bronze → Silver → Gold)
- ✅ **Streaming Tables** for Bronze and Silver layers with Auto Loader (DLT Python API)
- ✅ **Materialized View** for Gold layer with automatic refresh (DLT Python API)
- ✅ Primary/Foreign key constraints in Silver layer (via DLT expectations)
- ✅ Continuous incremental processing with streaming
- ✅ Data quality validation with `@dlt.expect_all_or_drop` decorators
- ✅ PySpark transformations for type conversions and data cleaning
- ✅ Databricks Secrets integration for API keys
- ✅ Databricks Asset Bundles configuration

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.9+
- FRED API key ([get one free](https://fred.stlouisfed.org/docs/api/api_key.html))

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/dmkern03/FRED.git
```

### 2. Configure FRED API Secret

```bash
databricks secrets create-scope fred-api
databricks secrets put-secret fred-api api-key --string-value "YOUR_API_KEY"
```

### 3. Create Delta Live Tables Pipeline

**Important:** Notebooks 02-04 must be run as part of a Delta Live Tables (DLT) pipeline, not as standalone notebooks.

#### Option A: Using Databricks Asset Bundles (Recommended)
```bash
cd s:\repos\FRED
databricks bundle deploy --target dev
databricks bundle run fred_dlt_pipeline --target dev
```

#### Option B: Using Databricks UI
1. Navigate to "Delta Live Tables" in Databricks
2. Click "Create Pipeline"
3. Add notebooks: `02_Bronze_Setup.py`, `03_Silver_Setup.py`, `04_Gold_Setup.py`
4. Set target catalog: `investments`, schema: `fred`
5. Enable Photon and set cluster to Enhanced Autoscaling (1-2 workers)
6. Create and start the pipeline

**See [DLT Pipeline Setup Guide](docs/dlt_pipeline_setup.md) for detailed instructions.**

### 4. One-Time Setup

Before running the DLT pipeline, run these notebooks once:

| Step | Notebook | Purpose |
|------|----------|---------|
| 1 | `01_API_Setup.py` | Create volumes and configure secrets |
| 2 | `05_Daily_API_Call.py` | Fetch initial data from FRED API |

### 5. Schedule Daily Data Fetching

Schedule only `05_Daily_API_Call.py` as a daily Databricks Workflow to fetch data from FRED API.

**Note:** The DLT pipeline automatically handles all data processing:
- Bronze Streaming Tables auto-ingest from volumes using Auto Loader
- Silver Streaming Tables auto-transform from Bronze
- Gold Materialized View auto-refreshes from Silver
- Notebooks 6-8 are deprecated and no longer needed

## Project Structure

```
FRED/
├── README.md
├── .gitignore
├── requirements.txt
├── databricks.yml              # Databricks Asset Bundle config
│
├── notebooks/
│   ├── 01_API_Setup.py         # One-time: volumes & secrets
│   ├── 02_Bronze_Setup.py      # DLT: Bronze Streaming Tables (Python API)
│   ├── 03_Silver_Setup.py      # DLT: Silver Streaming Tables + expectations (Python API)
│   ├── 04_Gold_Setup.py        # DLT: Gold Materialized View (Python API)
│   ├── 05_Daily_API_Call.py    # Daily: Fetch from FRED API
│   ├── 06_Bronze_Load.py       # Deprecated: Auto-handled by Streaming Tables
│   ├── 07_Silver_Load.py       # Deprecated: Auto-handled by Streaming Tables
│   └── 08_Gold_Load.py         # Deprecated: Auto-handled by Materialized View
│
├── src/
│   ├── __init__.py
│   └── utils/
│       └── api_helpers.py
│
├── sql/
│   ├── setup_all.sql           # Combined setup DDL
│   └── validate_fred.sql       # Validation queries
│
├── config/
│   └── config.yaml
│
├── resources/                  # DAB resource configs
│   ├── jobs.yml               # Job definitions
│   └── pipelines.yml          # DLT pipeline configuration
│
├── environments/               # DAB environment configs
│
├── tests/
│   └── test_fred_pipeline.py
│
└── docs/
    ├── data_dictionary.md
    ├── architecture.md
    └── dlt_pipeline_setup.md  # DLT pipeline setup guide
```

## Data Model

```
┌─────────────────────────────────┐
│  common.reference.dim_calendar  │ (FK reference)
└─────────────────────────────────┘
                ▲
                │ FK: date
                │
┌───────────────┴─────────────────┐     ┌────────────────────────┐
│investments.fred.silver_observations│───▶│  investments.fred.     │
│  FK: series_id                  │     │  silver_metadata       │
└─────────────────────────────────┘     │  PK: series_id         │
                                        └────────────────────────┘
```

## License

Copyright (c) 2025. All rights reserved.
