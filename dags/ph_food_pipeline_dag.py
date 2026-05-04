# ============================================
# ph_food_pipeline_dag.py
# Airflow DAG — Philippine Food Price Pipeline
# Schedule: 1st of every month at 6:00 AM
# ============================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "roland_de_la_rosa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ph_food_price_pipeline",
    description="Monthly pipeline: WFP food prices + PSA CPI + BSP exchange rate → Snowflake → dbt",
    schedule="0 6 1 * *",  # 6AM on the 1st of every month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["philippines", "food", "medallion", "snowflake", "dbt"],
) as dag:

    # ── Task 1: Ingest raw data into Bronze ───────────────────────────────
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="cd /usr/local/airflow && python main.py",
    )

    # ── Task 2: Run dbt Silver models ─────────────────────────────────────
    run_silver = BashOperator(
        task_id="run_silver_models",
        bash_command="cd /usr/local/airflow/ph_food_pipeline && dbt run --select silver",
    )

    # ── Task 3: Run dbt Gold models ───────────────────────────────────────
    run_gold = BashOperator(
        task_id="run_gold_models",
        bash_command="cd /usr/local/airflow/ph_food_pipeline && dbt run --select gold",
    )

    # ── Task 4: Run dbt tests ─────────────────────────────────────────────
    run_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /usr/local/airflow/ph_food_pipeline && dbt test",
    )

    # ── Task dependencies (order of execution) ────────────────────────────
    ingest_bronze >> run_silver >> run_gold >> run_tests