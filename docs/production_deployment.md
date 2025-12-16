# Production Deployment Guide

This guide explains how to deploy the FRED data pipeline to production using Databricks Asset Bundles (DAB).

## Prerequisites

1. **Databricks CLI** installed and configured
   ```bash
   pip install databricks-cli
   databricks configure
   ```

2. **Production workspace access**
   - Workspace URL: `https://dbc-6c839758-aba3.cloud.databricks.com/`
   - Permissions: CREATE PIPELINE, CREATE JOB, USE CATALOG, CREATE TABLE

3. **FRED API Key** configured in production workspace
   ```bash
   # Create secrets scope in production workspace
   databricks secrets create-scope fred-api --profile prod
   databricks secrets put-secret fred-api api-key --string-value "YOUR_API_KEY" --profile prod
   ```

## Deployment Steps

### 1. Validate Bundle Configuration

Before deploying, validate the bundle for production:

```bash
cd s:\repos\FRED
databricks bundle validate --target prod
```

### 2. Deploy to Production

Deploy all resources (DLT pipeline, jobs) to production:

```bash
databricks bundle deploy --target prod
```

This creates:
- **DLT Pipeline**: `FRED Data Pipeline - DLT` (continuous mode)
- **Scheduled Job**: `05_Daily_API_Call` (runs at 4:56 AM ET daily)

### 3. One-Time Setup (First Deployment)

After deploying, run these notebooks once in the production workspace:

#### Step 1: Create volumes and configure secrets
```bash
# Run notebook 01_API_Setup.py manually in production workspace
# This creates:
# - /Volumes/investments/fred/observations/
# - /Volumes/investments/fred/metadata/
```

#### Step 2: Fetch initial data
```bash
# Trigger the daily API call job manually to populate initial data
databricks bundle run _05_Daily_API_Call --target prod
```

Or run via Databricks UI:
1. Go to Workflows → Jobs
2. Find "05_Daily_API_Call"
3. Click "Run now"

#### Step 3: Start the DLT Pipeline
```bash
# Start the DLT pipeline for the first time
databricks bundle run fred_dlt_pipeline --target prod
```

Or via Databricks UI:
1. Go to Delta Live Tables
2. Find "FRED Data Pipeline - DLT"
3. Click "Start"

### 4. Verify Deployment

Check that all resources are running:

```bash
# List deployed pipelines
databricks pipelines list --target prod

# List deployed jobs
databricks jobs list --target prod

# Check DLT pipeline status
databricks pipelines get --pipeline-name "FRED Data Pipeline - DLT" --target prod
```

## Production Configuration

### Pipeline Settings (Prod)

| Setting | Value | Reason |
|---------|-------|--------|
| **Mode** | Continuous | Real-time processing |
| **Serverless** | Enabled | Auto-scaling, no cluster management |
| **Expectations** | Fail on error | Strict data quality |
| **Notifications** | Enabled | Alert on failures |

### Pipeline Settings (Dev)

| Setting | Value | Reason |
|---------|-------|--------|
| **Mode** | Triggered | Cost savings |
| **Serverless** | Enabled | Fast startup |
| **Expectations** | Drop on error | Lenient for testing |
| **Notifications** | Disabled | No noise in dev |

## Environment-Specific Differences

The bundle automatically applies different settings per environment:

| Resource | Development | Production |
|----------|-------------|------------|
| **Schema** | `investments.fred_dev` | `investments.fred` |
| **Pipeline Mode** | Triggered | Continuous |
| **Data Quality** | Drop invalid rows | Fail on invalid rows |
| **Notifications** | Disabled | Enabled |
| **Auto-delete** | Yes (on `bundle destroy`) | No (protected) |

## Daily Operations

### Production Pipeline

The production pipeline runs continuously:

1. **API Call Job** (scheduled): Fetches data daily at 4:56 AM ET
   - Uploads CSV/JSON files to volumes
   - Triggered automatically via cron schedule

2. **DLT Pipeline** (continuous): Processes data automatically
   - Bronze: Auto-ingests new files via Auto Loader
   - Silver: Transforms and validates data
   - Gold: Refreshes denormalized view

### Manual Triggers

If you need to manually trigger in production:

```bash
# Trigger API call
databricks bundle run _05_Daily_API_Call --target prod

# Trigger DLT pipeline
databricks bundle run fred_dlt_pipeline --target prod
```

## Monitoring

### DLT Pipeline Monitoring

1. **Via UI:**
   - Go to Delta Live Tables → "FRED Data Pipeline - DLT"
   - View pipeline graph, event log, data quality metrics

2. **Via CLI:**
   ```bash
   databricks pipelines get --pipeline-name "FRED Data Pipeline - DLT" --target prod
   ```

### Job Monitoring

1. **Via UI:**
   - Go to Workflows → Jobs → "05_Daily_API_Call"
   - View run history, logs, failures

2. **Via CLI:**
   ```bash
   databricks jobs list-runs --job-name "05_Daily_API_Call" --target prod
   ```

### Data Quality Checks

Run validation queries in production:

```sql
-- Check latest data
SELECT MAX(date) as latest_date, COUNT(*) as row_count
FROM investments.fred.gold_observations;

-- Check for NULL violations (should be 0)
SELECT COUNT(*) as null_violations
FROM investments.fred.silver_observations
WHERE series_id IS NULL OR date IS NULL OR value IS NULL;

-- Check referential integrity (should be 0)
SELECT COUNT(*) as orphaned_records
FROM investments.fred.silver_observations o
LEFT JOIN investments.fred.silver_metadata m ON o.series_id = m.series_id
WHERE m.series_id IS NULL;
```

## Rollback

If you need to rollback a production deployment:

```bash
# Destroy the current production deployment
databricks bundle destroy --target prod

# Deploy previous version
git checkout <previous-commit>
databricks bundle deploy --target prod
```

## Updating Production

To update the production pipeline after code changes:

```bash
# 1. Make changes to notebooks (02, 03, 04)
# 2. Validate changes
databricks bundle validate --target prod

# 3. Deploy updates
databricks bundle deploy --target prod

# 4. DLT pipeline auto-restarts with new code
# No manual restart needed in continuous mode
```

## Cost Optimization

### Development

- Use **triggered mode** (runs on-demand)
- Stop pipeline when not in use
- Use smaller cluster sizes

### Production

- Use **serverless** (auto-scaling, pay-per-use)
- Use **continuous mode** only if real-time processing needed
- Consider **triggered mode** with hourly/daily schedule if near real-time is acceptable

To switch production to triggered mode (cost savings):

Edit `environments/prod.yml`:
```yaml
continuous: false  # Change from true
```

Then redeploy:
```bash
databricks bundle deploy --target prod
```

## Troubleshooting

### Deployment Fails

**Error**: "User does not have permission to create pipelines"

**Solution**: Ensure your user has the following permissions in production:
```bash
# Grant permissions (workspace admin only)
databricks permissions update --object-type pipelines --resource-id <pipeline-id> --principal user:<your-email> --permission CAN_MANAGE
```

### Pipeline Fails to Start

**Error**: "INTERNAL_ERROR: Unexpected error while checking schema permissions"

**Solution**: Grant catalog/schema permissions:
```sql
GRANT USE CATALOG ON CATALOG investments TO `<your-email>`;
GRANT USE SCHEMA ON SCHEMA investments.fred TO `<your-email>`;
GRANT CREATE TABLE ON SCHEMA investments.fred TO `<your-email>`;
```

### No Data Flowing

**Issue**: Pipeline is running but tables are empty

**Solution**:
1. Check API call job ran successfully
2. Verify files exist in volumes:
   ```sql
   LIST '/Volumes/investments/fred/observations/';
   LIST '/Volumes/investments/fred/metadata/';
   ```
3. Check DLT pipeline event log for errors

## Best Practices

1. **Test in Dev First**: Always test changes in dev before deploying to prod
   ```bash
   databricks bundle deploy --target dev
   databricks bundle run fred_dlt_pipeline --target dev
   ```

2. **Use Git Tags**: Tag production releases for easy rollback
   ```bash
   git tag -a v1.0.3 -m "Production release 1.0.3"
   git push origin v1.0.3
   ```

3. **Monitor Costs**: Review Databricks usage dashboards weekly

4. **Schedule Maintenance**: Plan downtime for major changes during off-hours

5. **Backup Data**: Enable Delta table time travel for recovery
   ```sql
   -- Restore to previous version if needed
   RESTORE TABLE investments.fred.silver_observations TO VERSION AS OF 10;
   ```

## References

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [FRED Pipeline Setup Guide](dlt_pipeline_setup.md)
- [Constraints Reference](constraints_reference.md)
