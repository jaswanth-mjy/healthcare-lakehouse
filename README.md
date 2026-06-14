# Healthcare Data Lakehouse

End-to-end data engineering project built on real CMS hospital quality data.

## Architecture: Medallion (Bronze → Silver → Gold)

```
CMS data.gov CSV
      │
      ▼
scripts/ingest_bronze.py    ← Raw JSON + ingestion metadata
      │  bronze/hospitals/ingestion_date=YYYY-MM-DD/
      ▼
tests/ge_hospital_suite.py  ← Data quality checks (8 expectations)
      │
      ▼
dbt/models/staging/         ← stg_hospitals (clean, typed)
      │
      ▼
dbt/models/gold/            ← dim_hospital + fact_hospital_quality
      │
      ▼
dags/hospital_pipeline_dag.py  ← Airflow orchestration (daily)
```

## Tech Stack (100% Free)
- **Python** — ingestion + quality checks
- **dbt-core + dbt-duckdb** — SQL transformations
- **Great Expectations** — data quality
- **Apache Airflow** — orchestration
- **DuckDB** — local SQL engine (no cloud needed)

## Quickstart

```bash
# 1. Install dependencies
pip install pandas dbt-core dbt-duckdb great_expectations apache-airflow

# 2. Run bronze ingestion
python scripts/ingest_bronze.py

# 3. Run data quality checks
python tests/ge_hospital_suite.py

# 4. Run dbt transformations
cd dbt && dbt run && dbt test

# 5. Start Airflow (optional)
airflow db init && airflow webserver -p 8080
```

## Data Source
CMS Hospital General Information: https://data.cms.gov/provider-data/
