# Databricks FRED Data Pipeline

A comprehensive data pipeline for fetching, storing, and transforming Federal Reserve Economic Data (FRED) in Databricks using the medallion architecture.

## Overview

This project creates and maintains FRED economic data in a Databricks lakehouse with:

- **Bronze Layer**: Raw data ingestion from FRED API (CSV/JSON → Delta)
- **Silver Layer**: Cleaned, typed, and deduplicated data with constraints
- **Gold Layer**: Business-ready denormalized table with Change Data Feed enabled

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
- ✅ MERGE operations for incremental updates
- ✅ Primary/Foreign key constraints in Silver layer
- ✅ Change Data Feed enabled on Gold table
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

### 3. Run Notebooks in Order

| Step | Notebook | Purpose |
|------|----------|---------|
| 1 | `01_API_Setup.py` | Create volumes and configure secrets |
| 2 | `02_Bronze_Setup.py` | Create Bronze layer tables |
| 3 | `03_Silver_Setup.py` | Create Silver layer tables with constraints |
| 4 | `04_Gold_Setup.py` | Create Gold layer table |
| 5 | `05_Daily_API_Call.py` | Fetch data from FRED API |
| 6 | `06_Bronze_Load.py` | Load data into Bronze tables |
| 7 | `07_Silver_Load.py` | Transform and load Silver tables |
| 8 | `08_Gold_Load.py` | Create denormalized Gold table |

### 4. Schedule Daily Pipeline

Schedule notebooks 5-8 as a daily Databricks Workflow.

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
│   ├── 02_Bronze_Setup.py      # One-time: Bronze tables
│   ├── 03_Silver_Setup.py      # One-time: Silver tables + constraints
│   ├── 04_Gold_Setup.py        # One-time: Gold table
│   ├── 05_Daily_API_Call.py    # Daily: Fetch from FRED API
│   ├── 06_Bronze_Load.py       # Daily: COPY INTO Bronze
│   ├── 07_Silver_Load.py       # Daily: MERGE into Silver
│   └── 08_Gold_Load.py         # Daily: INSERT OVERWRITE Gold
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
├── environments/               # DAB environment configs
│
├── tests/
│   └── test_fred_pipeline.py
│
└── docs/
    ├── data_dictionary.md
    └── architecture.md
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
│  investments.fred.silver_rates  │────▶│  investments.fred.     │
│  FK: series_id                  │     │  silver_metadata       │
└─────────────────────────────────┘     │  PK: series_id         │
                                        └────────────────────────┘
```

## License

Copyright (c) 2025. All rights reserved.
