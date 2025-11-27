from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# --- Ensure your repo folder is visible inside the container ---
# This matches your docker-compose volume mapping
sys.path.append("/opt/airflow/repos/gcp_martech_prod/extract")

# Import your existing extraction script
from meta_ads_creative_dim import main as meta_ads_main

# --- DAG Configuration ---
default_args = {
    "owner": "Prasanna",
    "depends_on_past": False,
    "email": ["prasanna@euromedicom.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="meta_ads_creative_dim_raw_dag",
    default_args=default_args,
    description="Incrementally extract and load Meta Ads Creative dimensions data into BigQuery",
    schedule_interval="0 6 1-7 * 1", # minute hour day-of-month month day-of-week (once a month)
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["meta_ads", "bigquery", "incremental", "prod"],
) as dag:

    run_incremental_extraction = PythonOperator(
        task_id="extract_and_load_meta_ads_creative_dim_raw_data",
        python_callable=meta_ads_main,
        dag=dag,
    )

    run_incremental_extraction
