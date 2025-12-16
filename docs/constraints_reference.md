# FRED Pipeline Constraints Reference

This document provides a quick reference for all primary key and foreign key constraints in the FRED data pipeline.

## Overview

The FRED pipeline uses Delta Live Tables with Streaming Tables and Materialized Views. Constraints are defined inline during table creation to ensure data quality and referential integrity.

## Constraint Hierarchy

```
┌─────────────────────────────────┐
│  silver_metadata                │
│  PK: series_id (NOT NULL)       │
└─────────────┬───────────────────┘
              │
              │ FK: series_id
              │
┌─────────────▼───────────────────┐     ┌─────────────────────────────────┐
│  silver_observations            │     │  common.reference.dim_calendar  │
│  FK: series_id → silver_metadata│◄────┤  PK: calendar_date (optional)   │
│  FK: date → dim_calendar (opt)  │     └─────────────────────────────────┘
│  NOT NULL: series_id, date, value
└─────────────────────────────────┘
              │
              │ Auto-join
              │
┌─────────────▼───────────────────┐
│  gold_observations              │
│  (Materialized View)            │
│  Denormalized: obs + metadata   │
└─────────────────────────────────┘
```

## Silver Layer Constraints

### silver_metadata

**Primary Key:**
- `series_id` (STRING NOT NULL)

**Purpose:** Unique identifier for each FRED economic series (e.g., "DGS10", "DFF")

**Definition:**
```sql
CREATE OR REPLACE STREAMING TABLE investments.fred.silver_metadata (
    series_id STRING NOT NULL,
    ...
    CONSTRAINT pk_series PRIMARY KEY(series_id)
)
```

---

### silver_observations

**NOT NULL Constraints:**
- `series_id` (STRING NOT NULL)
- `date` (DATE NOT NULL)
- `value` (DOUBLE NOT NULL)

**Foreign Keys:**
1. `series_id` → `silver_metadata.series_id`
   - **Purpose:** Ensures every observation has corresponding metadata
   - **Enforcement:** Applied during streaming ingestion

2. `date` → `common.reference.dim_calendar.calendar_date` (OPTIONAL)
   - **Purpose:** Ensures all dates are valid calendar dates
   - **Status:** Commented out by default
   - **To enable:** Uncomment in notebook 03_Silver_Setup.py

**Definition:**
```sql
CREATE OR REPLACE STREAMING TABLE investments.fred.silver_observations (
    series_id STRING NOT NULL,
    series_name STRING,
    date DATE NOT NULL,
    value DOUBLE NOT NULL,
    run_timestamp TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT fk_series FOREIGN KEY(series_id) REFERENCES investments.fred.silver_metadata(series_id)
)
```

---

## Bronze Layer

The Bronze layer has **no constraints** as it stores raw data:
- `bronze_observations` - Raw observation data
- `bronze_metadata` - Raw metadata

Data quality is enforced at the Silver layer during transformation.

---

## Gold Layer

The Gold layer is a **Materialized View** with no explicit constraints:
- `gold_observations` - Denormalized view joining silver_observations + silver_metadata

The view automatically maintains referential integrity through the JOIN operation.

---

## Constraint Enforcement in Delta Live Tables

### How Constraints Work

1. **Primary Keys:**
   - Defined inline in CREATE STREAMING TABLE
   - Informational for documentation and optimization
   - Not enforced by Delta (no unique constraint check)
   - Application-level enforcement during streaming

2. **Foreign Keys:**
   - Defined inline in CREATE STREAMING TABLE
   - Informational for lineage and documentation
   - Not enforced by Delta (no referential integrity check)
   - Application-level enforcement during streaming

3. **NOT NULL:**
   - Enforced by Delta Lake
   - Write operations fail if NULL values provided
   - Streaming queries filter out NULL values before insert

### Best Practices

1. **Define constraints in DDL** for documentation and tooling support
2. **Filter invalid data** in the streaming query (e.g., `WHERE value IS NOT NULL`)
3. **Use type conversions** to enforce data types (e.g., `CAST(value AS DOUBLE)`)
4. **Monitor data quality metrics** in DLT pipeline UI
5. **Review constraint violations** in event logs

---

## Validation Queries

### Check Constraint Definitions

```sql
-- View metadata table constraints
DESCRIBE EXTENDED investments.fred.silver_metadata;

-- View observations table constraints
DESCRIBE EXTENDED investments.fred.silver_observations;
```

### Verify Referential Integrity

```sql
-- Find observations without metadata (should be empty)
SELECT o.series_id, COUNT(*) as orphaned_records
FROM investments.fred.silver_observations o
LEFT JOIN investments.fred.silver_metadata m ON o.series_id = m.series_id
WHERE m.series_id IS NULL
GROUP BY o.series_id;

-- Find metadata without observations (valid - new series not yet observed)
SELECT m.series_id, m.title
FROM investments.fred.silver_metadata m
LEFT JOIN investments.fred.silver_observations o ON m.series_id = o.series_id
WHERE o.series_id IS NULL;
```

### Check for NULL Violations

```sql
-- Should return 0 - NOT NULL constraints enforced
SELECT COUNT(*) as null_series_id
FROM investments.fred.silver_observations
WHERE series_id IS NULL;

SELECT COUNT(*) as null_dates
FROM investments.fred.silver_observations
WHERE date IS NULL;

SELECT COUNT(*) as null_values
FROM investments.fred.silver_observations
WHERE value IS NULL;
```

---

## Adding the Optional Calendar Foreign Key

If you have a calendar dimension table, you can enable the date foreign key:

1. **Ensure the calendar table exists:**
   ```sql
   SELECT COUNT(*) FROM common.reference.dim_calendar;
   ```

2. **Ensure it has a primary key:**
   ```sql
   ALTER TABLE common.reference.dim_calendar
   ADD CONSTRAINT pk_date PRIMARY KEY(calendar_date);
   ```

3. **Uncomment in Silver Setup:**
   Edit `notebooks/03_Silver_Setup.py` and uncomment:
   ```sql
   ALTER TABLE investments.fred.silver_observations
   ADD CONSTRAINT fk_date
   FOREIGN KEY(date) REFERENCES common.reference.dim_calendar(calendar_date);
   ```

---

## Troubleshooting

### Issue: Constraint violations not showing in logs

**Solution:** In DLT, constraints are informational. Monitor:
- Data quality expectations in pipeline UI
- Event logs for parsing/type conversion errors
- Row counts and data validation queries

### Issue: Foreign key constraint fails to create

**Cause:** Parent table (silver_metadata) doesn't exist yet

**Solution:** Ensure notebooks run in order:
1. `02_Bronze_Setup.py`
2. `03_Silver_Setup.py` (metadata before observations)
3. `04_Gold_Setup.py`

### Issue: NULL values passing through

**Cause:** NOT NULL constraint not enforced at source

**Solution:** Add explicit filtering in streaming query:
```sql
WHERE series_id IS NOT NULL
  AND date IS NOT NULL
  AND value IS NOT NULL
```

---

## References

- [Delta Lake Constraints](https://docs.databricks.com/delta/constraints.html)
- [Delta Live Tables Expectations](https://docs.databricks.com/delta-live-tables/expectations.html)
- [Streaming Table Constraints](https://docs.databricks.com/delta-live-tables/streaming-tables.html)
