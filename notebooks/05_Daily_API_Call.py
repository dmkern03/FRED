# Databricks notebook source
"""
FRED API Rate Data Fetcher - Databricks Edition
Pulls rate data for multiple indices and saves to Unity Catalog Volumes.

Author: Dave
Date: December 2024

================================================================================
DATABRICKS SETUP INSTRUCTIONS
================================================================================

1. CREATE UNITY CATALOG VOLUMES AND BRONZE TABLES
   -----------------------------------------------
   Run this SQL in a Databricks notebook or SQL editor:
   
   -- Create schema (if needed)  
   CREATE SCHEMA IF NOT EXISTS investments.fred;
   
   -- Create volume for observation data
   CREATE VOLUME IF NOT EXISTS investments.fred.observations;
   
   -- Create volume for series metadata/dimensional info
   CREATE VOLUME IF NOT EXISTS investments.fred.metadata;
   
   -- Create Bronze layer tables (empty schemas - data loaded by script)
   CREATE TABLE IF NOT EXISTS investments.fred.bronze_observations (
       run_timestamp TIMESTAMP,
       series_id STRING,
       series_name STRING,
       date DATE,
       value DOUBLE,
       ingestion_timestamp TIMESTAMP
   )
   USING DELTA
   COMMENT 'Bronze layer: Raw FRED observations';
   
   CREATE TABLE IF NOT EXISTS investments.fred.bronze_metadata (
       run_timestamp TIMESTAMP,
       series_id STRING,
       friendly_name STRING,
       title STRING,
       frequency STRING,
       frequency_short STRING,
       units STRING,
       units_short STRING,
       seasonal_adjustment STRING,
       seasonal_adjustment_short STRING,
       observation_start DATE,
       observation_end DATE,
       last_updated STRING,
       popularity INT,
       notes STRING,
       ingestion_timestamp TIMESTAMP
   )
   USING DELTA
   COMMENT 'Bronze layer: Raw FRED series metadata/dimensional info';

2. STORE YOUR FRED API KEY AS A DATABRICKS SECRET
   -----------------------------------------------
   Using Databricks CLI:
   
   # Create a secret scope (one-time setup)
   databricks secrets create-scope --scope fred-api
   
   # Store your API key
   databricks secrets put --scope fred-api --key api-key
   
   Or via the Databricks UI:
   - Go to your workspace URL + #secrets/createScope
   - Create scope named "fred-api"
   - Add secret named "api-key" with your FRED API key value
   
   Get your free FRED API key at: https://fred.stlouisfed.org/docs/api/api_key.html

3. CREATE A DATABRICKS NOTEBOOK
   ----------------------------
   - Create a new Python notebook in your workspace
   - Copy this entire script into the notebook
   - Update the CONFIGURATION section below with your catalog/schema/volume names

4. SCHEDULE THE NOTEBOOK AS A JOB
   ------------------------------
   Option A: Via Databricks UI
   - Click "Schedule" button in the notebook toolbar
   - Or go to Workflows > Jobs > Create Job
   - Select your notebook as the task
   - Set schedule (e.g., daily at 6:00 AM ET)
   - Configure cluster (single node is fine for this workload)
   - Set alerts/notifications as desired
   
   Option B: Via Databricks CLI or API
   See example JSON config at the bottom of this script.

5. RUN THE NOTEBOOK
   -----------------
   - Run manually to test, or let the schedule trigger it
   - Check the volumes for output CSV files:
     SELECT * FROM LIST('/Volumes/investments/fred/observations/')
     SELECT * FROM LIST('/Volumes/investments/fred/metadata/')

================================================================================
"""

import requests
import pandas as pd
from datetime import datetime
import time


# =============================================================================
# DATABRICKS CONFIGURATION
# =============================================================================

# Unity Catalog location
CATALOG = "investments"
SCHEMA = "fred"

# Separate volumes for different data types (landing zone)
VOLUME_OBSERVATIONS = "observations"  # For observation data
VOLUME_METADATA = "metadata"          # For series metadata/dimensional info

# Construct volume paths (Databricks Volumes path format)
VOLUME_PATH_OBSERVATIONS = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_OBSERVATIONS}"
VOLUME_PATH_METADATA = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_METADATA}"

# Secret scope and key name for FRED API key
SECRET_SCOPE = "fred-api"
SECRET_KEY = "api-key"

# =============================================================================
# FRED API CONFIGURATION
# =============================================================================

# Base URL for FRED API
FRED_BASE_URL = "https://api.stlouisfed.org/fred"

# Rate series to pull - ADD NEW SERIES HERE
# Format: "SERIES_ID": "Friendly Name"
RATE_SERIES = {
    # Treasury Rates
    "DFF": "Federal Funds Effective Rate",
    "DTB3": "3-Month Treasury Bill Rate",
    "DGS1": "1-Year Treasury Rate",
    "DGS2": "2-Year Treasury Rate",
    "DGS5": "5-Year Treasury Rate",
    "DGS10": "10-Year Treasury Rate",
    "DGS30": "30-Year Treasury Rate",
    
    # LIBOR/SOFR Rates
    "SOFR": "Secured Overnight Financing Rate (SOFR)",
    
    # Prime Rate
    "DPRIME": "Bank Prime Loan Rate",
    
    # Mortgage Rates
    "MORTGAGE30US": "30-Year Fixed Rate Mortgage Average",
    "MORTGAGE15US": "15-Year Fixed Rate Mortgage Average",
    
    # Corporate Bond Rates
    "BAMLC0A0CM": "ICE BofA US Corporate Index Effective Yield",
    "BAMLH0A0HYM2": "ICE BofA US High Yield Index Effective Yield",
    
    # Inflation Expectations
    "T10YIE": "10-Year Breakeven Inflation Rate",
    "T5YIE": "5-Year Breakeven Inflation Rate",
}

# Optional: Filter by date range (set to None for all available data)
OBSERVATION_START = None  # e.g., "2020-01-01"
OBSERVATION_END = None    # e.g., "2024-12-31"


# =============================================================================
# API FUNCTIONS
# =============================================================================

def get_series_info(series_id: str) -> dict:
    """
    Get metadata about a FRED series.
    
    Args:
        series_id: The FRED series identifier
        
    Returns:
        Dictionary with series metadata
    """
    url = f"{FRED_BASE_URL}/series"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    if "seriess" in data and len(data["seriess"]) > 0:
        return data["seriess"][0]
    return {}


def fetch_series_metadata(series_dict: dict) -> pd.DataFrame:
    """
    Fetch dimensional/metadata information for multiple series.
    
    Args:
        series_dict: Dictionary mapping series_id to friendly name
        
    Returns:
        DataFrame with metadata for all series
    """
    all_metadata = []
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\nFetching metadata for {len(series_dict)} series...")
    
    for series_id, friendly_name in series_dict.items():
        print(f"  Fetching metadata for {series_id}...")
        
        try:
            info = get_series_info(series_id)
            
            if info:
                # Clean notes field - remove line breaks that break CSV parsing
                notes = info.get("notes", "")
                if notes:
                    # Replace newlines, carriage returns, and multiple spaces
                    notes = notes.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
                    # Remove multiple consecutive spaces
                    while "  " in notes:
                        notes = notes.replace("  ", " ")
                    notes = notes.strip()
                
                # Extract key dimensional information - ALL fields as strings
                metadata = {
                    "run_timestamp": run_timestamp,
                    "series_id": info.get("id", series_id),
                    "friendly_name": friendly_name,
                    "title": info.get("title", ""),
                    "frequency": info.get("frequency", ""),
                    "frequency_short": info.get("frequency_short", ""),
                    "units": info.get("units", ""),
                    "units_short": info.get("units_short", ""),
                    "seasonal_adjustment": info.get("seasonal_adjustment", ""),
                    "seasonal_adjustment_short": info.get("seasonal_adjustment_short", ""),
                    "observation_start": info.get("observation_start", ""),
                    "observation_end": info.get("observation_end", ""),
                    "last_updated": info.get("last_updated", ""),
                    "popularity": str(info.get("popularity", "")),
                    "notes": notes
                }
                all_metadata.append(metadata)
                print(f"    ✓ Retrieved metadata")
            else:
                print(f"    ⚠ No metadata available")
                
        except requests.exceptions.HTTPError as e:
            print(f"    ✗ Error: {e}")
        
        # Be nice to the API - small delay between requests
        time.sleep(0.2)
    
    if all_metadata:
        return pd.DataFrame(all_metadata)
    return pd.DataFrame()


def get_series_observations(
    series_id: str,
    observation_start: str = None,
    observation_end: str = None,
    frequency: str = None,
    sort_order: str = "asc"
) -> pd.DataFrame:
    """
    Get observations (data values) for a FRED series.
    
    Args:
        series_id: The FRED series identifier
        observation_start: Start date (YYYY-MM-DD)
        observation_end: End date (YYYY-MM-DD)
        frequency: Aggregation frequency ('d', 'w', 'm', 'q', 'a')
        sort_order: 'asc' or 'desc'
        
    Returns:
        DataFrame with date and value columns
    """
    url = f"{FRED_BASE_URL}/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": sort_order
    }
    
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end
    if frequency:
        params["frequency"] = frequency
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    observations = data.get("observations", [])
    
    if not observations:
        return pd.DataFrame(columns=["date", "value"])
    
    df = pd.DataFrame(observations)
    df = df[["date", "value"]]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    
    return df


def fetch_multiple_series(
    series_dict: dict,
    observation_start: str = None,
    observation_end: str = None,
    latest_only: bool = False
) -> pd.DataFrame:
    """
    Fetch data for multiple series and combine into a single DataFrame.
    
    Args:
        series_dict: Dictionary mapping series_id to friendly name
        observation_start: Start date (YYYY-MM-DD)
        observation_end: End date (YYYY-MM-DD)
        latest_only: If True, only return the most recent observation for each series
        
    Returns:
        Combined DataFrame with all series data
    """
    all_data = []
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for series_id, friendly_name in series_dict.items():
        print(f"  Fetching {series_id} ({friendly_name})...")
        
        try:
            df = get_series_observations(
                series_id,
                observation_start=observation_start,
                observation_end=observation_end
            )
            
            if not df.empty:
                df["run_timestamp"] = run_timestamp
                df["series_id"] = series_id
                df["series_name"] = friendly_name
                
                # Reorder columns to put run_timestamp first
                cols = ["run_timestamp", "series_id", "series_name", "date", "value"]
                df = df[cols]
                
                if latest_only:
                    df = df.tail(1)
                
                all_data.append(df)
                print(f"    ✓ Retrieved {len(df)} observations")
            else:
                print(f"    ⚠ No data available")
                
        except requests.exceptions.HTTPError as e:
            print(f"    ✗ Error: {e}")
        
        # Be nice to the API - small delay between requests
        time.sleep(0.2)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


# =============================================================================
# OUTPUT FUNCTIONS
# =============================================================================

def save_to_csv(df: pd.DataFrame, volume_path: str, filename_prefix: str = "fred_observations") -> str:
    """
    Save DataFrame to a timestamped CSV file in Databricks Volume.
    
    Args:
        df: DataFrame to save
        volume_path: Path to the Databricks volume
        filename_prefix: Prefix for the filename
        
    Returns:
        Path to the saved file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"
    filepath = f"{volume_path}/{filename}"
    
    df.to_csv(filepath, index=False)
    print(f"\n✓ Data saved to: {filepath}")
    
    return filepath


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function for Databricks."""
    
    print("=" * 60)
    print("FRED Rate Data Fetcher - Databricks Edition")
    print("=" * 60)
    print(f"\nLanding Zone (CSV Files):")
    print(f"  Observations Volume: {VOLUME_PATH_OBSERVATIONS}")
    print(f"  Metadata Volume:     {VOLUME_PATH_METADATA}")
    print(f"\nRun Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ==========================================================================
    # GET API KEY FROM DATABRICKS SECRETS
    # ==========================================================================
    try:
        # This works in Databricks notebooks
        FRED_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
        print(f"✓ API key retrieved from secret scope: {SECRET_SCOPE}")
    except NameError:
        # Fallback for local testing (dbutils not available)
        import os
        FRED_API_KEY = os.environ.get("FRED_API_KEY", "YOUR_API_KEY_HERE")
        print("⚠ Running outside Databricks - using environment variable")
    
    if not FRED_API_KEY or FRED_API_KEY == "YOUR_API_KEY_HERE":
        print("\n⚠ ERROR: Please set your FRED API key!")
        print("  In Databricks: Store in secrets scope")
        print("  Locally: Set FRED_API_KEY environment variable")
        return
    
    # Make API key available to other functions via global
    globals()['FRED_API_KEY'] = FRED_API_KEY
    
    # ==========================================================================
    # VERIFY VOLUME ACCESS (BOTH VOLUMES)
    # ==========================================================================
    for volume_name, volume_path in [("Observations", VOLUME_PATH_OBSERVATIONS), ("Metadata", VOLUME_PATH_METADATA)]:
        try:
            test_path = f"{volume_path}/.write_test"
            with open(test_path, 'w') as f:
                f.write("test")
            import os
            os.remove(test_path)
            print(f"✓ {volume_name} volume access verified: {volume_path}")
        except Exception as e:
            print(f"\n⚠ ERROR: Cannot write to {volume_name} volume: {volume_path}")
            print(f"  Error: {e}")
            print("  Please verify the volume exists and you have write permissions.")
            return
    
    # ==========================================================================
    # FETCH SERIES METADATA (DIMENSIONAL INFORMATION)
    # ==========================================================================
    metadata_df = fetch_series_metadata(RATE_SERIES)
    
    if not metadata_df.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metadata_filepath = f"{VOLUME_PATH_METADATA}/fred_series_metadata_{timestamp}.json"
        # Use JSON format to properly handle special characters and line breaks
        metadata_df.to_json(metadata_filepath, orient='records', lines=True)
        print(f"\n✓ Metadata saved to: {metadata_filepath}")
        
        # Display metadata summary
        print("\n" + "=" * 60)
        print("SERIES METADATA SUMMARY")
        print("=" * 60)
        summary_cols = ["series_id", "title", "frequency", "units_short", "last_updated"]
        print(metadata_df[summary_cols].to_string(index=False))
    
    # ==========================================================================
    # FETCH OBSERVATIONS DATA
    # ==========================================================================
    print(f"\nFetching {len(RATE_SERIES)} rate series...")
    if OBSERVATION_START:
        print(f"  Date range: {OBSERVATION_START} to {OBSERVATION_END or 'present'}")
    
    # Fetch all series data
    df = fetch_multiple_series(
        RATE_SERIES,
        observation_start=OBSERVATION_START,
        observation_end=OBSERVATION_END
    )
    
    if df.empty:
        print("\n⚠ No data retrieved. Check your API key and series IDs.")
        return
    
    print(f"\nTotal observations retrieved: {len(df)}")
    
    # Save full historical data (long format)
    save_to_csv(df, VOLUME_PATH_OBSERVATIONS, "fred_observations_historical")
    
    print("\n" + "=" * 60)
    print("✓ FRED data fetch completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()


# =============================================================================
# DATABRICKS JOB CONFIGURATION EXAMPLE (JSON)
# =============================================================================
"""
Save this as a JSON file and use with Databricks CLI:
    databricks jobs create --json @fred_job_config.json

Or use the Databricks REST API to create the job programmatically.

{
    "name": "FRED Rate Data Daily Fetch",
    "email_notifications": {
        "on_failure": ["your-email@company.com"],
        "on_success": []
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "fetch_fred_rates",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/your-email@company.com/fred_rates_fetcher",
                "source": "WORKSPACE"
            },
            "job_cluster_key": "fred_cluster",
            "timeout_seconds": 0,
            "email_notifications": {}
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "fred_cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "14.3.x-scala2.12",
                "spark_conf": {},
                "node_type_id": "i3.xlarge",
                "num_workers": 0,
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "enable_elastic_disk": false,
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD"
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "America/New_York",
        "pause_status": "UNPAUSED"
    },
    "queue": {
        "enabled": true
    },
    "format": "MULTI_TASK"
}

SCHEDULE EXAMPLES (Quartz Cron Format):
---------------------------------------
"0 0 6 * * ?"     = Daily at 6:00 AM
"0 30 7 * * ?"    = Daily at 7:30 AM
"0 0 6 ? * MON-FRI" = Weekdays at 6:00 AM
"0 0 */4 * * ?"   = Every 4 hours
"0 0 6 1 * ?"     = Monthly on the 1st at 6:00 AM

TIMEZONE OPTIONS:
-----------------
America/New_York    = Eastern Time
America/Chicago     = Central Time
America/Denver      = Mountain Time
America/Los_Angeles = Pacific Time
UTC                 = Coordinated Universal Time
"""


# =============================================================================
# ALTERNATIVE: DELTA TABLE OUTPUT (Optional Enhancement)
# =============================================================================
"""
If you prefer to save data as Delta tables instead of CSV files,
you can use this function in a Databricks notebook:

def save_to_delta_table(df: pd.DataFrame, table_name: str):
    '''
    Save DataFrame as a Delta table in Unity Catalog.
    '''
    spark_df = spark.createDataFrame(df)
    
    full_table_name = f"investments.fred.{table_name}"
    
    spark_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)
    
    print(f"✓ Data saved to Delta table: {full_table_name}")

# Usage:
# save_to_delta_table(df, "fred_rates_historical")
# save_to_delta_table(metadata_df, "fred_rates_metadata")
"""