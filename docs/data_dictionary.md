# Data Dictionary: FRED Pipeline

## Overview

The FRED data pipeline stores Federal Reserve Economic Data using a medallion architecture in the `investments.fred` schema.

---

## Tables

### Bronze Layer

#### investments.fred.bronze_observations
Raw observations loaded from CSV files.

| Column | Type | Description |
|--------|------|-------------|
| run_timestamp | STRING | Timestamp when data was fetched |
| series_id | STRING | FRED series identifier |
| series_name | STRING | Friendly name |
| date | STRING | Observation date |
| value | DOUBLE | Observation value |

#### investments.fred.bronze_metadata
Raw series metadata loaded from JSON files.

| Column | Type | Description |
|--------|------|-------------|
| run_timestamp | STRING | Timestamp when fetched |
| series_id | STRING | FRED series identifier |
| friendly_name | STRING | User-friendly name |
| title | STRING | Official FRED title |
| frequency | STRING | Data frequency |
| frequency_short | STRING | Frequency abbreviation |
| units | STRING | Unit of measurement |
| units_short | STRING | Units abbreviation |
| seasonal_adjustment | STRING | SA description |
| seasonal_adjustment_short | STRING | SA abbreviation |
| observation_start | STRING | First observation date |
| observation_end | STRING | Last observation date |
| last_updated | STRING | Last FRED update |
| popularity | STRING | Popularity score |
| notes | STRING | Series notes |

---

### Silver Layer

#### investments.fred.silver_metadata
**Primary Key**: `series_id`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| series_id | STRING | No | FRED series identifier |
| friendly_name | STRING | Yes | User-friendly name |
| title | STRING | Yes | Official FRED title |
| frequency | STRING | Yes | Data frequency |
| frequency_short | STRING | Yes | Frequency abbreviation |
| units | STRING | Yes | Unit of measurement |
| units_short | STRING | Yes | Units abbreviation |
| seasonal_adjustment | STRING | Yes | SA description |
| seasonal_adjustment_short | STRING | Yes | SA abbreviation |
| observation_start | DATE | Yes | First observation date |
| observation_end | DATE | Yes | Last observation date |
| last_updated | TIMESTAMP | Yes | Last FRED update |
| popularity | INT | Yes | Popularity score |
| notes | STRING | Yes | Series notes |
| run_timestamp | TIMESTAMP | No | Source fetch timestamp |
| updated_at | TIMESTAMP | No | Silver update timestamp |

#### investments.fred.silver_observations
**Foreign Keys**: `series_id` → `silver_metadata.series_id`, `date` → `dim_calendar.calendar_date`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| series_id | STRING | No | FRED series identifier |
| series_name | STRING | No | Friendly name |
| date | DATE | No | Observation date |
| value | DOUBLE | No | Observation value |
| run_timestamp | TIMESTAMP | No | Source fetch timestamp |
| updated_at | TIMESTAMP | No | Silver update timestamp |

---

### Gold Layer

#### investments.fred.gold_observations
Denormalized table with Change Data Feed enabled.

| Column | Type | Description |
|--------|------|-------------|
| series_id | STRING | FRED series identifier |
| date | DATE | Observation date |
| value | DOUBLE | Observation value |
| title | STRING | Official FRED title |
| friendly_name | STRING | User-friendly name |
| units | STRING | Unit of measurement |
| source | STRING | Always 'FRED' |

---

## Rate Series Reference

| Series ID | Name | Frequency |
|-----------|------|-----------|
| DFF | Federal Funds Effective Rate | Daily |
| DTB3 | 3-Month Treasury Bill | Daily |
| DGS1 | 1-Year Treasury Rate | Daily |
| DGS2 | 2-Year Treasury Rate | Daily |
| DGS5 | 5-Year Treasury Rate | Daily |
| DGS10 | 10-Year Treasury Rate | Daily |
| DGS30 | 30-Year Treasury Rate | Daily |
| SOFR | Secured Overnight Financing Rate | Daily |
| DPRIME | Bank Prime Loan Rate | Daily |
| MORTGAGE30US | 30-Year Fixed Mortgage | Weekly |
| MORTGAGE15US | 15-Year Fixed Mortgage | Weekly |
| BAMLC0A0CM | US Corporate Bond Yield | Daily |
| BAMLH0A0HYM2 | US High Yield Bond Index | Daily |
| T10YIE | 10-Year Breakeven Inflation | Daily |
| T5YIE | 5-Year Breakeven Inflation | Daily |
