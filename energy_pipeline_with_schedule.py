import requests
import pandas as pd
from dagster import asset, Definitions, MaterializeResult, Output, ScheduleDefinition, DefaultScheduleStatus
from google.cloud import bigquery
import tempfile
import os

@asset
def raw_energy_capacity() -> Output[pd.DataFrame]:
    """Extract energy capacity data from Danish Energy Data Service API"""
    
    print("Extracting data from Energi Data Service...")
    
    url = "https://api.energidataservice.dk/dataset/CapacityPerMunicipality"
    params = {"limit": 10000, "sort": "Month"}
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    records = data.get("records", [])
    
    print(f"Retrieved {len(records)} records")
    
    df = pd.DataFrame(records)
    
    if df.empty:
        raise ValueError("No data retrieved from API")
    
    print(f"Date range: {df['Month'].min()} to {df['Month'].max()}")
    print(f"Municipalities: {df['MunicipalityNo'].nunique()}")
    
    # Return the DataFrame directly - Dagster will handle it
    return Output(
        df,
        metadata={
            "record_count": len(df),
            "first_month": df["Month"].min(),
            "last_month": df["Month"].max(),
        }
    )

@asset
def load_to_bigquery(raw_energy_capacity: pd.DataFrame) -> MaterializeResult:
    """Load the energy data to BigQuery"""
    
    df = raw_energy_capacity
    
    # Clean column names (BigQuery prefers lowercase with underscores)
    df.columns = [col.lower() for col in df.columns]
    
    bq_client = bigquery.Client()
    table_id = "dk_energy.capacity_per_municipality"
    
    print(f"Loading {len(df)} rows to BigQuery: {table_id}")
    
    # Create dataset if it doesn't exist
    bq_client.query("CREATE SCHEMA IF NOT EXISTS dk_energy").result()
    
    # Write to BigQuery
    job = bq_client.load_table_from_dataframe(
        df, 
        table_id, 
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
    )
    job.result()
    
    table = bq_client.get_table(table_id)
    
    return MaterializeResult(
        metadata={
            "table": table_id,
            "rows_loaded": table.num_rows,
            "size_mb": round(table.num_bytes / (1024 * 1024), 2)
        }
    )

# Define schedule: Runs on 5th of every month at 9 AM UTC
# (Dataset updates during first week of each month)
energy_schedule = ScheduleDefinition(
    name="energy_monthly_schedule",
    cron_schedule="0 9 5 * *",  # 9 AM on 5th day of every month
    job_name="energy_pipeline_job",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

# Create a job from assets
from dagster import define_asset_job

energy_job = define_asset_job(
    name="energy_pipeline_job",
    selection=["raw_energy_capacity", "load_to_bigquery"],
)

defs = Definitions(
    assets=[raw_energy_capacity, load_to_bigquery],
    jobs=[energy_job],
    schedules=[energy_schedule],
)