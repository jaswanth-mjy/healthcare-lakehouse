# 🏥 Healthcare Data Lakehouse

An end-to-end data engineering project built on real US government hospital data (CMS), implementing a **Medallion Architecture** (Bronze → Silver → Gold) with automated orchestration, data quality checks, and a star schema analytics layer.

## 🏗️ Architecture

```
CMS Data (data.cms.gov)
        │
        ▼
ingest_bronze.py  ──►  bronze/hospitals/ingestion_date=YYYY-MM-DD/
                        (raw JSON, partitioned by date + audit manifest)
        │
        ▼
ge_hospital_suite.py  ──►  8 automated data quality checks
        │
        ▼
dbt stg_hospitals  ──►  silver (clean, typed, null-safe)
        │
        ▼
dbt dim_hospital          ──►  gold (dimension table, surrogate keys)
dbt fact_hospital_quality ──►  gold (quality metrics per snapshot)
        │
        ▼
Apache Airflow DAG  ──►  daily orchestration @ 6 AM
```

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, Pandas |
| Storage | JSON partitioned by ingestion date |
| Data Quality | Custom Great Expectations suite |
| Transformation | dbt-core + dbt-duckdb |
| Warehouse | DuckDB (local, zero cost) |
| Orchestration | Apache Airflow 2.9 |
| Version Control | Git + GitHub |

## 📊 Dataset

Real hospital quality data from **CMS.gov** (Centers for Medicare & Medicaid Services):

- **5,432 hospitals** across all 50 US states + territories
- Mortality, safety, readmission, and patient experience metrics
- Overall star ratings (1–5)

## ✅ Data Quality Checks (8/8 Passing)

| Check | Result |
|---|---|
| facility_id not null | ✅ PASS |
| facility_name not null | ✅ PASS |
| state not null | ✅ PASS |
| facility_id unique | ✅ PASS |
| overall_rating values in 1–5 or null | ✅ PASS |
| _source metadata present | ✅ PASS |
| _ingested_at metadata present | ✅ PASS |
| row count ≥ 1 | ✅ PASS — 5,432 rows |

## 🥇 Gold Layer — Sample Results

| Hospital | State | Rating | Quality Score |
|---|---|---|---|
| NYU Langone Hospitals | NY | ⭐ 5 | 77.8% |
| Mayo Clinic Hospital Rochester | MN | ⭐ 5 | 59.3% |
| Massachusetts General Hospital | MA | ⭐ 5 | 55.6% |
| Cedars-Sinai Medical Center | CA | ⭐ 5 | 51.9% |
| Morristown Medical Center | NJ | ⭐ 5 | 51.9% |

## 🚀 How to Run

```bash
# 1. Clone the repo
git clone https://github.com/jaswanth-mjy/healthcare-lakehouse.git
cd healthcare-lakehouse

# 2. Set up virtual environment (Python 3.11 required)
python3.11 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install pandas dbt-core dbt-duckdb apache-airflow

# 4. Run Bronze ingestion
python scripts/ingest_bronze.py

# 5. Run data quality checks
python tests/ge_hospital_suite.py

# 6. Run dbt transformations
cd healthcare_dbt
../venv/bin/dbt deps
../venv/bin/dbt run
../venv/bin/dbt test

# 7. Run full Airflow pipeline
export AIRFLOW_HOME=$(pwd)/../airflow
../venv/bin/airflow db migrate
../venv/bin/airflow dags test hospital_quality_pipeline $(date +%Y-%m-%d)
```

## 📁 Project Structure

```
healthcare-lakehouse/
├── data/
│   └── raw/
│       └── hospitals.csv          # Source CSV from CMS.gov
├── bronze/
│   └── hospitals/
│       └── ingestion_date=YYYY-MM-DD/
│           ├── hospitals.json     # Raw records + metadata
│           └── manifest.json      # Audit trail
├── scripts/
│   └── ingest_bronze.py           # Bronze ingestion pipeline
├── tests/
│   └── ge_hospital_suite.py       # 8 data quality checks
├── healthcare_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_hospitals.sql  # Silver layer
│   │   │   ├── schema.yml
│   │   │   └── sources.yml
│   │   └── gold/
│   │       ├── dim_hospital.sql         # Dimension table
│   │       └── fact_hospital_quality.sql # Fact table
│   ├── packages.yml               # dbt_utils dependency
│   └── dbt_project.yml
├── airflow/
│   └── dags/
│       └── hospital_pipeline_dag.py  # Airflow DAG
└── README.md
```

## 🧠 Key Design Decisions

- **Bronze never mutates raw data** — only adds `_source`, `_ingested_at`, `_ingestion_date` metadata
- **TRY_CAST everywhere in Silver** — CMS uses "Not Available" instead of NULL; safe casting prevents pipeline failures
- **Surrogate keys in Gold** — `dbt_utils.generate_surrogate_key` on `facility_id` for SCD-ready dimension
- **DuckDB over local Spark** — faster iteration, zero infrastructure, fully SQL-compatible, production-used

## 👤 Author

**Jaswanth** — Data Engineer | Azure Databricks | PySpark | dbt | Airflow

[![GitHub](https://img.shields.io/badge/GitHub-jaswanth--mjy-black?logo=github)](https://github.com/jaswanth-mjy)
