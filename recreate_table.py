# recreate_table.py - Month as STRING (no datetime conversion)
from google.cloud import bigquery
import pandas as pd
import requests

PROJECT_ID = "dk-energy-pipeline"
DATASET_ID = "dk_energy"
TABLE_ID = "capacity_per_municipality"

client = bigquery.Client(project=PROJECT_ID)

# Drop old table
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
client.delete_table(table_ref, not_found_ok=True)
print(f"✅ Deleted old table: {table_ref}")

# Fetch data
url = "https://api.energidataservice.dk/dataset/CapacityPerMunicipality"
params = {"limit": 100000}
response = requests.get(url, params=params)
data = response.json()
df = pd.DataFrame(data["records"])

# Keep Month as STRING (original format from API)
# No datetime conversion at all!

# Convert numeric columns safely
numeric_cols = ["CapacityGe100MW", "CapacityLt100MW", "OffshoreWindCapacity", 
                "OnshoreWindCapacity", "SolarPowerCapacity"]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

int_cols = ["NumberGenerationUnitsGe100MW", "NumberGenerationUnitsLt100MW",
            "NumberOffshoreWindGenerators", "NumberOnshoreWindGenerators", 
            "NumberSolarPanels"]
for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

# Create table with Month as STRING
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Month", "STRING"),  # STRING, not DATETIME
        bigquery.SchemaField("MunicipalityNo", "STRING"),
        bigquery.SchemaField("CapacityGe100MW", "FLOAT"),
        bigquery.SchemaField("CapacityLt100MW", "FLOAT"),
        bigquery.SchemaField("OffshoreWindCapacity", "FLOAT"),
        bigquery.SchemaField("OnshoreWindCapacity", "FLOAT"),
        bigquery.SchemaField("SolarPowerCapacity", "FLOAT"),
        bigquery.SchemaField("NumberGenerationUnitsGe100MW", "INTEGER"),
        bigquery.SchemaField("NumberGenerationUnitsLt100MW", "INTEGER"),
        bigquery.SchemaField("NumberOffshoreWindGenerators", "INTEGER"),
        bigquery.SchemaField("NumberOnshoreWindGenerators", "INTEGER"),
        bigquery.SchemaField("NumberSolarPanels", "INTEGER"),
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
load_job.result()

print(f"✅ Table created with Month as STRING")
print(f"📊 Loaded {len(df)} rows")
print(f"📅 Sample months: {df['Month'].head(3).tolist()}")