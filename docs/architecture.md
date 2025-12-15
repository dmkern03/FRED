# Architecture: FRED Data Pipeline

## Overview

This document describes the architecture for the FRED data pipeline in Databricks.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FRED API                                        │
│                    (Federal Reserve Economic Data)                           │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ REST API
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Databricks Workspace                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐                                                           │
│  │  Workflow    │ ─── Scheduled Daily                                       │
│  └──────┬───────┘                                                           │
│         │                                                                    │
│         ▼                                                                    │
│  ┌──────────────┐     ┌──────────────────────────────────────┐             │
│  │ 05_Daily_    │────▶│  Volumes (Landing Zone)              │             │
│  │  API_Call    │     │  /Volumes/investments/fred/observations/│  CSV     │
│  │              │     │  /Volumes/investments/fred/metadata/ │  JSON       │
│  └──────────────┘     └──────────────────┬───────────────────┘             │
│                                          │                                   │
│                                          │ COPY INTO                         │
│                                          ▼                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        BRONZE LAYER                                   │   │
│  │  ┌─────────────────────┐  ┌─────────────────────┐                   │   │
│  │  │ bronze_observations │  │   bronze_metadata   │                   │   │
│  │  │   (raw strings)     │  │   (raw strings)     │                   │   │
│  │  └─────────────────────┘  └─────────────────────┘                   │   │
│  └──────────────────────────────────┬───────────────────────────────────┘   │
│                                     │                                        │
│                                     │ MERGE (with type conversion)           │
│                                     ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        SILVER LAYER                                   │   │
│  │  ┌─────────────────────┐  ┌─────────────────────┐                   │   │
│  │  │silver_observations  │  │   silver_metadata   │                   │   │
│  │  │   FK: series_id ────┼──│── PK: series_id     │                   │   │
│  │  │   FK: date          │  │                     │                   │   │
│  │  └─────────────────────┘  └─────────────────────┘                   │   │
│  └──────────────────────────────────┬───────────────────────────────────┘   │
│                                     │                                        │
│                                     │ INSERT OVERWRITE                       │
│                                     ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         GOLD LAYER                                    │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │ gold_observations (denormalized, Change Data Feed enabled)     │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

| Step | Notebook | Action |
|------|----------|--------|
| 1 | 05_Daily_API_Call | Fetch from FRED API → CSV/JSON to Volumes |
| 2 | 06_Bronze_Load | COPY INTO Bronze tables |
| 3 | 07_Silver_Load | MERGE into Silver (type conversion, dedup) |
| 4 | 08_Gold_Load | INSERT OVERWRITE Gold (denormalize) |

## Key Design Decisions

### Why MERGE for Silver?
- Deduplication by `series_id` + `date` with max `run_timestamp`
- Only updates rows when values actually change
- Maintains referential integrity with constraints

### Why INSERT OVERWRITE for Gold?
- Simple denormalization (join Silver tables)
- Full refresh ensures consistency
- Change Data Feed tracks changes for downstream

### Constraints
- **PK on silver_metadata**: `series_id`
- **FK on silver_observations**: `series_id` → `silver_metadata`
- **FK on silver_observations**: `date` → `dim_calendar` (optional)

Note: Databricks constraints are informational only (not enforced), but help document the data model.

## Pipeline Schedule

Recommended: Daily at 6 AM Eastern (after markets close, before business day starts)

```
0 6 * * ? America/New_York
```
