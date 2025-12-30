# Databricks FRED Data Pipeline

[![CI](https://github.com/dmkern03/FRED/actions/workflows/ci.yml/badge.svg)](https://github.com/dmkern03/FRED/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-orange.svg)](https://databricks.com/)

A production-grade data pipeline for fetching, storing, and transforming Federal Reserve Economic Data (FRED) in Databricks using the medallion architecture and Delta Live Tables.

## Overview

This project creates and maintains FRED economic data in a Databricks lakehouse with:

- **Bronze Layer**: Raw data ingestion using Streaming Tables with Auto Loader
- **Silver Layer**: Cleaned, typed data using Streaming Tables with data quality constraints
- **Gold Layer**: Business-ready denormalized Materialized View

```
FRED API  ──▶  Bronze (Raw)  ──▶  Silver (Clean)  ──▶  Gold (Business)
              Streaming Table    Streaming Table     Materialized View
```

## Features

- Automated FRED API data fetching for 15 economic rate series
- Medallion architecture (Bronze → Silver → Gold)
- Delta Live Tables with Streaming Tables and Materialized Views
- Data quality validation with DLT expectations
- Primary/Foreign key constraints
- Databricks Asset Bundles for infrastructure as code
- Environment-specific configurations (dev/prod)

## Data Sources

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

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.9+
- FRED API key ([get one free](https://fred.stlouisfed.org/docs/api/api_key.html))

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/dmkern03/FRED.git
cd FRED
```

### 2. Install Dependencies

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure FRED API Secret

```bash
databricks secrets create-scope fred-api
databricks secrets put-secret fred-api api-key --string-value "YOUR_API_KEY"
```

### 4. Deploy the Pipeline

#### Option A: Using Databricks Asset Bundles (Recommended)

```bash
databricks bundle deploy --target dev
databricks bundle run fred_dlt_pipeline --target dev
```

#### Option B: Using Databricks UI

1. Navigate to **Delta Live Tables** in Databricks
2. Click **Create Pipeline**
3. Add notebooks: `02_Bronze_Setup.py`, `03_Silver_Setup.py`, `04_Gold_Setup.py`
4. Set target catalog: `investments`, schema: `fred`
5. Create and start the pipeline

See [DLT Pipeline Setup Guide](docs/dlt_pipeline_setup.md) for detailed instructions.

### 5. Initial Data Load

Run these notebooks once before starting the DLT pipeline:

| Step | Notebook | Purpose |
|------|----------|---------|
| 1 | `01_API_Setup.py` | Create volumes and configure secrets |
| 2 | `05_Daily_API_Call.py` | Fetch initial data from FRED API |

### 6. Schedule Daily Updates

Schedule `05_Daily_API_Call.py` as a daily Databricks Workflow. The DLT pipeline automatically processes new data.

## Project Structure

```
FRED/
├── notebooks/                  # Databricks notebooks
│   ├── 01_API_Setup.py        # One-time setup
│   ├── 02_Bronze_Setup.py     # DLT Bronze layer
│   ├── 03_Silver_Setup.py     # DLT Silver layer
│   ├── 04_Gold_Setup.py       # DLT Gold layer
│   └── 05_Daily_API_Call.py   # Daily API fetch
│
├── src/                        # Python source code
│   └── utils/
│       └── api_helpers.py     # FRED API utilities
│
├── tests/                      # Unit tests
├── docs/                       # Documentation
├── config/                     # Pipeline configuration
├── resources/                  # DAB resource definitions
└── environments/               # Environment configs
```

## Documentation

- [Architecture](docs/architecture.md) - System design and data flow
- [DLT Pipeline Setup](docs/dlt_pipeline_setup.md) - Detailed setup instructions
- [Production Deployment](docs/production_deployment.md) - Production deployment guide
- [Data Dictionary](docs/data_dictionary.md) - Schema documentation
- [Constraints Reference](docs/constraints_reference.md) - PK/FK constraints

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
```

### Code Style

This project uses [Ruff](https://github.com/astral-sh/ruff) for linting and formatting.

```bash
ruff check .
ruff format .
```

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Federal Reserve Bank of St. Louis](https://fred.stlouisfed.org/) for the FRED API
- [Databricks](https://databricks.com/) for Delta Live Tables
