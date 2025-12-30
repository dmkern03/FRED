# Archived Notebooks

This directory contains deprecated notebooks that are no longer actively used.

## Why These Are Archived

With the adoption of Delta Live Tables (DLT), the manual loading notebooks are no longer needed:

| Notebook | Status | Replacement |
|----------|--------|-------------|
| `06_Bronze_Load.py` | Deprecated | Auto-handled by Bronze Streaming Table with Auto Loader |
| `07_Silver_Load.py` | Deprecated | Auto-handled by Silver Streaming Table |
| `08_Gold_Load.py` | Deprecated | Auto-handled by Gold Materialized View |
| `ZZ_Test.ipynb` | Development | Test/experimental notebook |

## Migration Notes

If you're migrating from the manual load approach to DLT:

1. Use `02_Bronze_Setup.py`, `03_Silver_Setup.py`, `04_Gold_Setup.py` in a DLT pipeline
2. Only `05_Daily_API_Call.py` needs to be scheduled as a job
3. DLT handles all data transformation automatically

See the [DLT Pipeline Setup Guide](../../docs/dlt_pipeline_setup.md) for details.
