# What This Project Proves

| Skill | Evidence | File to see |
|-------|----------|-------------|
| Python API integration | Pagination, error handling, retries | `energy_pipeline_incremental.py` |
| Orchestration | Dagster assets + schedule | `run_simple.py` |
| SQL (dbt) | Window functions, CTEs, joins | `energy_dbt/models/energy/enriched_capacity.sql` |
| Cloud (BigQuery) | Table creation, query optimization | `recreate_table.py` |
| Data quality | dbt tests + Great Expectations | `schema.yml`, `ge_suite_energy.py` |
| Monitoring | Slack alerts on job status | `run_simple.py` (send_slack function) |
| Visualization | Power BI + DAX measures | `dashboard.pbix` |
| Incremental patterns | State management via metadata table | `energy_pipeline_incremental.py` (line 45-60) |