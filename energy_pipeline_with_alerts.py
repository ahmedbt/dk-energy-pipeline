import os
import requests
import pandas as pd
from google.cloud import bigquery
from dagster import (
    asset, 
    AssetExecutionContext, 
    MaterializeResult, 
    ScheduleDefinition,
    define_asset_job,
    Definitions,
    Output,
    MetadataValue,run_failure_sensor, RunFailureSensorContext
)
from dagster_slack import slack_resource, make_slack_on_run_failure_sensor
from datetime import datetime


# ==================== CONFIGURATION ====================
PROJECT_ID = "dk-energy-pipeline"  
DATASET_ID = "dk_energy"
TABLE_ID = "capacity_per_municipality"
SLACK_WEBHOOK_URL = os.environ.get("https://hooks.slack.com/services/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")  

# ==================== EXTRACTION ====================
@asset
def raw_energy_capacity(context: AssetExecutionContext):
    """Extract energy capacity data from Energidataservice API"""
    url = "https://api.energidataservice.dk/dataset/CapacityPerMunicipality"
    params = {"limit": 10000, "offset": 0}
    
    context.log.info(f"Fetching data from {url}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("records"):
            raise ValueError("API returned empty records")
        
        df = pd.DataFrame(data["records"])
        
        context.log.info(f"Extracted {len(df)} rows")
        
        # Add metadata for Dagster UI
        yield Output(
            df,
            metadata={
                "row_count": len(df),
                "columns": MetadataValue.md(str(list(df.columns))),
                "preview": MetadataValue.md(str(df.head(3).to_markdown())),
                "api_status": response.status_code,
            }
        )
        
    except Exception as e:
        context.log.error(f"Extraction failed: {str(e)}")
        raise

# ==================== LOAD ====================
@asset
def load_to_bigquery(context: AssetExecutionContext, raw_energy_capacity: pd.DataFrame):
    """Load data to BigQuery (idempotent full refresh)"""
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Configure load job - truncate and reload
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    
    try:
        load_job = client.load_table_from_dataframe(
            raw_energy_capacity, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for completion
        
        table = client.get_table(table_ref)
        
        context.log.info(f"Loaded {table.num_rows} rows to {table_ref}")
        
        return MaterializeResult(
            metadata={
                "row_count": table.num_rows,
                "table": table_ref,
                "load_time": str(load_job.ended - load_job.started),
            }
        )
    except Exception as e:
        context.log.error(f"BigQuery load failed: {str(e)}")
        raise

# ==================== DBT WRAPPER (Optional) ====================
@asset(deps=[load_to_bigquery])
def run_dbt_transform(context: AssetExecutionContext):
    """Run dbt transformations"""
    import subprocess
    
    try:
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "energy_dbt"],
            capture_output=True,
            text=True,
            check=True
        )
        
        context.log.info("dbt run completed successfully")
        
        return MaterializeResult(
            metadata={
                "dbt_output": MetadataValue.md(f"```\n{result.stdout[-500:]}\n```"),
                "status": "success"
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt failed: {e.stderr}")
        raise


# ==================== SCHEDULE ====================
pipeline_job = define_asset_job(
    "energy_pipeline_daily",
    selection=["raw_energy_capacity", "load_to_bigquery", "run_dbt_transform"]
)

# Run on 5th of each month at 9 AM
schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 9 5 * *",
    execution_timezone="UTC",
)

# ==================== DEFINITIONS ====================
defs = Definitions(
    assets=[raw_energy_capacity, load_to_bigquery, run_dbt_transform],
    jobs=[pipeline_job],
    schedules=[schedule],
    sensors=[slack_alert_on_failure],
    resources={
        "slack": slack_resource.configured({"webhook_url": SLACK_WEBHOOK_URL})
    }
)