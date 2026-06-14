"""
hospital_pipeline_dag.py
=========================
Full end-to-end pipeline DAG:
  1. Ingest CMS data → Bronze (JSON)
  2. Run dbt → Silver (stg_hospitals)
  3. Run dbt → Gold (dim_hospital, fact_hospital_quality)
  4. Run dbt tests
  5. Alert on failure (logs + optional email)

Schedule: Daily at 6 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

log = logging.getLogger(__name__)

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "jaswanth",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,   # Set True + configure SMTP to get email alerts
    "email_on_retry":   False,
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="hospital_quality_pipeline",
    description="CMS Hospital data: Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # Daily at 6 AM UTC
    catchup=False,
    tags=["healthcare", "cms", "bronze", "silver", "gold"],
) as dag:

    # ── Task 1: Bronze Ingestion ───────────────────────────────────────────────
    def run_bronze_ingestion(**context):
        """Import and run the bronze ingestion script as a Python function."""
        import sys
        sys.path.insert(0, "/home/claude/healthcare-lakehouse")
        from scripts.ingest_bronze import ingest_hospitals

        output_path = ingest_hospitals()
        log.info(f"Bronze ingestion complete: {output_path}")

        # Push output path to XCom so downstream tasks can reference it
        context["ti"].xcom_push(key="bronze_output", value=output_path)

    t1_bronze = PythonOperator(
        task_id="bronze_ingest_hospitals",
        python_callable=run_bronze_ingestion,
    )

    # ── Task 2: dbt Silver (staging models) ───────────────────────────────────
    t2_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="cd /home/claude/healthcare-lakehouse/dbt && dbt run --select staging",
        env={"DBT_PROFILES_DIR": "/home/claude/healthcare-lakehouse/dbt"},
    )

    # ── Task 3: dbt Gold (dimension + fact models) ────────────────────────────
    t3_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /home/claude/healthcare-lakehouse/dbt && dbt run --select gold",
        env={"DBT_PROFILES_DIR": "/home/claude/healthcare-lakehouse/dbt"},
    )

    # ── Task 4: dbt Tests ─────────────────────────────────────────────────────
    t4_test = BashOperator(
        task_id="dbt_test_all",
        bash_command="cd /home/claude/healthcare-lakehouse/dbt && dbt test",
        env={"DBT_PROFILES_DIR": "/home/claude/healthcare-lakehouse/dbt"},
    )

    # ── Task 5: Success log ───────────────────────────────────────────────────
    def log_success(**context):
        run_date = context["ds"]
        log.info(f"Pipeline succeeded for {run_date}")
        print(f"✓ Hospital quality pipeline complete | date={run_date}")

    t5_success = PythonOperator(
        task_id="log_pipeline_success",
        python_callable=log_success,
    )

    # ── Task 6: Failure alert (runs even if upstream fails) ───────────────────
    def alert_failure(**context):
        task_instance = context["task_instance"]
        dag_id        = context["dag"].dag_id
        run_date      = context["ds"]
        failed_task   = context.get("task").task_id

        msg = f"PIPELINE FAILED | dag={dag_id} | task={failed_task} | date={run_date}"
        log.error(msg)
        # Plug in Slack webhook, PagerDuty, or email here
        print(f"ALERT: {msg}")

    t6_alert = PythonOperator(
        task_id="alert_on_failure",
        python_callable=alert_failure,
        trigger_rule=TriggerRule.ONE_FAILED,   # Only runs if something upstream failed
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    #
    #   t1_bronze → t2_silver → t3_gold → t4_test → t5_success
    #                                              ↘ t6_alert (on failure)
    #
    t1_bronze >> t2_silver >> t3_gold >> t4_test >> [t5_success, t6_alert]
