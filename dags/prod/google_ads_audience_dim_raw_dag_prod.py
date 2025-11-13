# IMPORTANT: Run this dimension script on a weekly or monthly schedule in Airflow. That is more than enough to keep your audience names up to date.

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# --- Ensure your repo folder is visible inside the container ---
# This matches your docker-compose volume mapping
sys.path.append("/opt/airflow/repos/gcp_martech_prod/extract")

# Import your existing extraction script
from google_ads_audience_dim import main as google_ads_main

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
    dag_id="google_ads_audience_dim_raw_dag",
    default_args=default_args,
    description="Incrementally extract and load Google Ads Audience Dim data into BigQuery",
    schedule_interval=None,  # Manual trigger only in Dev
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["google_ads", "bigquery", "incremental", "dev", "AUDIENCE DIMENSIONS"],
) as dag:

    run_incremental_extraction = PythonOperator(
        task_id="extract_and_load_google_ads_audience_dim_raw_data",
        python_callable=google_ads_main,
        dag=dag,
    )

    run_incremental_extraction
