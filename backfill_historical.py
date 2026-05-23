# backfill_historical.py
from google.cloud import bigquery
import pandas as pd
import requests

PROJECT_ID = "dk-energy-pipeline"

# Fetch ALL historical data
url = "https://api.energidataservice.dk/dataset/CapacityPerMunicipality"
params = {"limit": 100000, "offset": 0}

response = requests.get(url, params=params)
df = pd.DataFrame(response.json()["records"])

# Load all data initially
client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.dk_energy.capacity_per_municipality"

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # First load = truncate
    autodetect=True,
)

load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
load_job.result()

print(f"✅ Historical backfill complete: {len(df)} rows loaded")
print(f"📅 Date range: {df['Month'].min()} to {df['Month'].max()}")