# energy_pipeline_incremental.py - Updated version
import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, timedelta
from dagster import (
    asset, AssetExecutionContext, MaterializeResult, ScheduleDefinition,
    define_asset_job, Definitions
)
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "dk-energy-pipeline"
DATASET_ID = "dk_energy"
TABLE_ID = "capacity_per_municipality"
METADATA_TABLE = "pipeline_metadata"

def get_last_run_date(client) -> datetime:
    """Get the last successful run date from metadata table"""
    query = f"""
    SELECT last_processed_date 
    FROM `{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}`
    WHERE pipeline_name = 'capacity_pipeline'
    ORDER BY updated_at DESC
    LIMIT 1
    """
    
    try:
        result = client.query(query).result()
        rows = list(result)
        if rows and rows[0].last_processed_date:
            return rows[0].last_processed_date
    except NotFound:
        pass
    
    # Default: fetch last 90 days
    return datetime.now() - timedelta(days=90)

def update_metadata(client, last_date: datetime, rows_loaded: int):
    """Update the metadata table with latest run info"""
    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE}`
    (pipeline_name, last_run_timestamp, last_processed_date, rows_loaded, updated_at)
    VALUES (
        'capacity_pipeline',
        CURRENT_TIMESTAMP(),
        DATE('{last_date.strftime('%Y-%m-%d')}'),
        {rows_loaded},
        CURRENT_TIMESTAMP()
    )
    """
    client.query(query).result()

@asset
def raw_energy_capacity_incremental(context: AssetExecutionContext):
    """Extract ONLY NEW energy capacity data - Month as STRING"""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Get last run date
    last_run_date = get_last_run_date(client)
    context.log.info(f"Last successful run: {last_run_date}")
    
    # Fetch all data from API
    url = "https://api.energidataservice.dk/dataset/CapacityPerMunicipality"
    params = {"limit": 10000, "offset": 0}
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data["records"])
    
   # Simple string comparison - always works
    last_run_date_str = last_run_date.strftime("%Y-%m-%d")
    df["_Month_date"] = df["Month"].str[:10]
    df_new = df[df["_Month_date"] > last_run_date_str]
    
    # Drop the temp datetime column
    df_new = df_new.drop(columns=["_Month_date"])
    
    context.log.info(f"Total: {len(df)}, New: {len(df_new)}")
    
    # Get max date for metadata
    max_date = datetime.now()  # Default
    if not df_new.empty:
        # Extract max date from Month string
        max_month_str = df_new["Month"].max()
        max_date = pd.to_datetime(max_month_str)
    
    return {
        "dataframe": df_new,
        "new_records_count": len(df_new),
        "max_date": max_date,
        "last_run_date": last_run_date
    }

@asset
def load_to_bigquery_incremental(
    context: AssetExecutionContext, 
    raw_energy_capacity_incremental: dict
):
    """Append new records (Month as STRING)"""
    client = bigquery.Client(project=PROJECT_ID)
    
    df_new = raw_energy_capacity_incremental["dataframe"]
    new_count = raw_energy_capacity_incremental["new_records_count"]
    max_date = raw_energy_capacity_incremental["max_date"]
    
    if df_new.empty:
        context.log.info("No new records to load")
        return MaterializeResult(metadata={"rows_loaded": 0})
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Simple append - no datetime conversion needed
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,  # Let BigQuery handle schema matching
    )
    
    load_job = client.load_table_from_dataframe(
        df_new, table_ref, job_config=job_config
    )
    load_job.result()
    
    # Update metadata
    update_metadata(client, max_date, new_count)
    
    context.log.info(f"✅ Appended {new_count} rows")
    
    return MaterializeResult(
        metadata={
            "rows_loaded": new_count,
            "max_month": df_new["Month"].max() if not df_new.empty else "None",
        }
    )
@asset(deps=[load_to_bigquery_incremental])
def run_dbt_enrichment(context: AssetExecutionContext):
    """Run dbt models including enrichment"""
    import subprocess
    
    # First run the mapping seed
    subprocess.run(
        ["dbt", "seed", "--project-dir", "energy_dbt"],
        capture_output=True,
        text=True,
        check=True
    )
    
    # Then run models
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "energy_dbt", "--select", "enriched_capacity"],
        capture_output=True,
        text=True,
        check=True
    )
    
    context.log.info("Enrichment completed successfully")
    
    return MaterializeResult(
        metadata={
            "dbt_output": result.stdout[-500:],
            "status": "success"
        }
    )

pipeline_job = define_asset_job(
    "energy_pipeline_incremental",
    selection=["raw_energy_capacity_incremental", "load_to_bigquery_incremental"]
)

schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 9 5 * *",
)

defs = Definitions(
    assets=[raw_energy_capacity_incremental, load_to_bigquery_incremental],
    jobs=[pipeline_job],
    schedules=[schedule],
)