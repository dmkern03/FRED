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
│                                          │ Auto Loader (cloud_files)         │
│                                          ▼                                   │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                   BRONZE LAYER (Streaming Tables)                     │   │
│  │  ┌─────────────────────┐  ┌─────────────────────┐                   │   │
│  │  │ bronze_observations │  │   bronze_metadata   │                   │   │
│  │  │  (streaming table)  │  │  (streaming table)  │                   │   │
│  │  └─────────────────────┘  └─────────────────────┘                   │   │
│  └──────────────────────────────────┬───────────────────────────────────┘   │
│                                     │                                        │
│                                     │ STREAM (auto type conversion)          │
│                                     ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                   SILVER LAYER (Streaming Tables)                     │   │
│  │  ┌─────────────────────┐  ┌─────────────────────┐                   │   │
│  │  │silver_observations  │  │   silver_metadata   │                   │   │
│  │  │  (streaming table)  │  │  (streaming table)  │                   │   │
│  │  │   FK: series_id ────┼──│── PK: series_id     │                   │   │
│  │  │   FK: date          │  │                     │                   │   │
│  │  └─────────────────────┘  └─────────────────────┘                   │   │
│  └──────────────────────────────────┬───────────────────────────────────┘   │
│                                     │                                        │
│                                     │ Auto Refresh                           │
│                                     ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    GOLD LAYER (Materialized View)                     │   │
│  │  ┌────────────────────────────────────────────────────────────────┐ │   │
│  │  │ gold_observations (denormalized, auto-refreshed)               │ │   │
│  │  └────────────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

| Step | Action | Description |
|------|--------|-------------|
| 1 | **API Fetch** (05_Daily_API_Call) | Fetch from FRED API → CSV/JSON to Volumes |
| 2 | **Bronze Ingestion** (Automated) | Streaming Tables with Auto Loader continuously ingest from Volumes |
| 3 | **Silver Transformation** (Automated) | Streaming Tables read from Bronze with type conversion and filtering |
| 4 | **Gold Materialization** (Automated) | Materialized View auto-refreshes with denormalized data from Silver |

**Note:** Only step 1 requires scheduling. Steps 2-4 are handled automatically by Databricks.

## Key Design Decisions

### Why Streaming Tables for Bronze?
- **Auto Loader** continuously monitors volumes for new files
- Automatically handles schema evolution and inference
- Idempotent processing with exactly-once semantics
- No manual COPY INTO commands needed

### Why Streaming Tables for Silver?
- Continuous incremental processing from Bronze
- Type conversions and data quality filters applied in streaming
- Maintains referential integrity with constraints
- Automatic deduplication and state management

### Why Materialized View for Gold?
- Automatically refreshes when source Silver tables change
- Simple denormalized view (join Silver tables)
- No manual INSERT OVERWRITE needed
- Optimized query performance for downstream consumers

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
