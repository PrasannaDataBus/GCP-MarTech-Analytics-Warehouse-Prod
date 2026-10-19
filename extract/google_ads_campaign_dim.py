# Process: Extract Google Ads Campaign Dimensions (Schedules & Status)
# Data Points: Campaign ID, Name, Status, Serving Status, Start Date, End Date
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Materialization: Dimension Table (Overwrites per Account)
# MarTech Dictionary: Refer SharePoint file - MarTech Data Dictionary

from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
from datetime import datetime, timezone, date, timedelta
import pandas as pd
from dotenv import load_dotenv
import re
import os
from pathlib import Path

# --- Detect environment ---
# You can set this with PowerShell: $env:ENVIRONMENT = "DEV" (temporary) or setx ENVIRONMENT "DEV" (permanent)
# Verify using: echo $env:ENVIRONMENT


def load_environment():
    """
    Load params.env dynamically:
      - If inside Airflow: /opt/airflow/secrets/params.env
      - Else (local Windows): infer env (Dev/Prod/…) from script path
        '...\\GCP MarTech Analytics Warehouse - <Env>\\...'
        and load '<base>\\GCP MarTech Analytics Warehouse - <Env>\\params.env'
    Returns the detected environment name in UPPERCASE (e.g., 'DEV', 'PROD').
    """
    # Airflow container
    airflow_env = Path("/opt/airflow/secrets/params.env")
    if airflow_env.exists():
        load_dotenv(airflow_env.as_posix(), override=True)
        env = (os.getenv("ENVIRONMENT_NAME") or os.getenv("ENVIRONMENT") or "AIRFLOW").strip().upper()
        print(f"Airflow detected. Loaded: {airflow_env}")
        print(f"Effective ENV: {env}")
        return env

    # Local path-based detection (Windows)
    # Use __file__ if available, else fall back to CWD (helps in REPL/tests)
    script_path = Path(__file__).resolve() if "__file__" in globals() else Path.cwd().resolve()
    script_str = str(script_path)

    # Match the folder pattern: GCP MarTech Analytics Warehouse - <Env>
    m = re.search(r"GCP MarTech Analytics Warehouse - ([A-Za-z]+)", script_str, flags=re.IGNORECASE)
    if not m:
        raise ValueError(
            "Unable to detect environment from path. Expected path segment like "
            "'GCP MarTech Analytics Warehouse - Dev' or '- Prod'. "
            f"Got: {script_str}"
        )

    env = m.group(1).strip().upper()  # e.g., DEV, PROD, UAT, etc.
    base_path = Path(r"C:\Users\prasa\Root")
    folder_name = f"GCP MarTech Analytics Warehouse - {env.title()}"
    env_file = (base_path / folder_name / "params.env")

    if not env_file.exists():
        raise FileNotFoundError(f"Environment file not found: {env_file}")

    load_dotenv(env_file.as_posix(), override=True)

    # if ENVIRONMENT_NAME exists in params.env, ensure it matches
    file_env = (os.getenv("ENVIRONMENT_NAME") or env).strip().upper()
    if file_env != env:
        print(f"Mismatch: path env={env}, file ENVIRONMENT_NAME={file_env}")

    print(f"Local detected. Loaded: {env_file}")
    print(f"Effective ENV: {file_env}")
    return env


# usage
current_env = load_environment()
if __name__ == "__main__":
    print(f"Running in {current_env} environment")

# --- LOAD CONFIG STRINGS (SAFE AT TOP LEVEL) ---
GOOGLE_ADS_CONFIG = os.getenv("GOOGLE_ADS_CONFIG")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Ensure env var is set for BigQuery (Safe)
if CREDENTIALS_PATH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME")
CAMPAIGN_DIM_TABLE_NAME = os.getenv("CAMPAIGN_DIM_TABLE_NAME")

# --- GAQL QUERY (Dimension fields only, no time segments) ---
QUERY_TEMPLATE = """
SELECT
  customer.id,
  campaign.id,
  campaign.name,
  campaign.status,
  campaign.serving_status,
  campaign.start_date_time,
  campaign.end_date_time
FROM campaign
"""


# ---------------------------------------------------------
# HELPER: LAZY LOAD CLIENTS
# ---------------------------------------------------------
def get_clients():
    """
    Initializes clients ONLY when called, prevents Import crashes in Airflow.
    """
    if not GOOGLE_ADS_CONFIG:
        raise ValueError("GOOGLE_ADS_CONFIG environment variable is missing")

    # print(f"Connecting to Google Ads using config: {GOOGLE_ADS_CONFIG}")

    # Network calls happen HERE now
    ads_client = GoogleAdsClient.load_from_storage(GOOGLE_ADS_CONFIG)
    bq_client = bigquery.Client()

    return ads_client, bq_client


# --- FETCH ALL CLIENT ACCOUNTS (for MCC) ---
def get_child_accounts(manager_customer_id: str, ads_client):
    """Fetch all client accounts under a manager (MCC)."""
    service = ads_client.get_service("GoogleAdsService")
    query = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.status
        FROM customer_client
        WHERE customer_client.manager = FALSE
    """
    response = service.search(
        request={"customer_id": manager_customer_id, "query": query}
    )

    accounts = []
    for row in response:
        accounts.append({
            "id": row.customer_client.id,
            "name": row.customer_client.descriptive_name or "Unnamed Account"
        })
    return accounts


def extract_campaign_dimensions(customer_id: str, ads_client):
    """Extracts campaign metadata without time-series constraints."""
    service = ads_client.get_service("GoogleAdsService")
    response = service.search_stream(customer_id=customer_id, query=QUERY_TEMPLATE)

    rows = []
    for batch in response:
        for row in batch.results:
            rows.append({
                "account_id": str(row.customer.id),
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "status": getattr(row.campaign.status, "name", "UNKNOWN"),
                "serving_status": getattr(row.campaign.serving_status, "name", "UNKNOWN"),
                # v23+: 'YYYY-MM-DD HH:MM:SS' in account time zone; keep the date part only
                "start_date": (row.campaign.start_date_time or "")[:10],
                "end_date": (row.campaign.end_date_time or "")[:10],
                "_ingested_at": datetime.now(timezone.utc)
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        # Convert strings to actual date objects. Errors='coerce' handles empty strings cleanly.
        df['start_date'] = pd.to_datetime(df['start_date'], format = '%Y-%m-%d', errors = 'coerce').dt.date
        df['end_date'] = pd.to_datetime(df['end_date'], format = '%Y-%m-%d', errors = 'coerce').dt.date

    return df


# --- BATCH LOAD TO BIGQUERY ---
def load_batch_to_bigquery(df: pd.DataFrame, bq_client):
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{CAMPAIGN_DIM_TABLE_NAME}"

    # 1. Get unique account IDs that were successfully extracted in this run
    account_ids = df['account_id'].unique().tolist()
    formatted_ids = ",".join([f"'{aid}'" for aid in account_ids])

    # 2. Delete existing records for ONLY the successful accounts (One query instead of many)
    delete_query = f"""
        DELETE FROM `{table_id}` WHERE account_id IN ({formatted_ids})
    """
    try:
        bq_client.query(delete_query).result()
        print(f"  -> Successfully cleared existing dimensions for {len(account_ids)} accounts in one batch.")
    except Exception as e:
        print(f"  -> Note: Delete step skipped or encountered an issue (normal on first run). Error: {e}")

    # 3. Append fresh state for all accounts at once
    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_APPEND",
        schema = [
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("serving_status", "STRING"),
            bigquery.SchemaField("start_date", "DATE"),
            bigquery.SchemaField("end_date", "DATE"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ],
        clustering_fields = ["account_id", "campaign_id"]
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()
    print(f"  -> Successfully loaded {len(df)} total campaigns into {CAMPAIGN_DIM_TABLE_NAME}!")


# --- MAIN ---

# --- MAIN ---
def main():
    print("--- Starting Google Ads Campaign Dimension Extraction ---")

    ads_client, bq_client = get_clients()
    manager_id = ads_client.login_customer_id or ads_client.client_customer_id

    try:
        child_accounts = get_child_accounts(manager_id, ads_client)
        print(f"Found {len(child_accounts)} client accounts under manager {manager_id}")
    except Exception as e:
        print(f"Error fetching ad accounts: {e}")
        return

    EXCLUDED_IDS = ['8024672713']
    failed_accounts = []
    master_df_list = []  # NEW: Initialize an empty list to hold all dataframes

    for account in child_accounts:
        customer_id = str(account["id"])
        account_name = account["name"]

        if customer_id in EXCLUDED_IDS:
            print(f"Skipping known Test Account: {account_name} ({customer_id})")
            continue

        print(f"Extracting dimensions for account: {account_name} ({customer_id})")

        try:
            df = extract_campaign_dimensions(customer_id, ads_client)
            if df.empty:
                print(f"  -> No campaigns found.")
                continue

            # Add to our master list instead of loading to BQ immediately
            master_df_list.append(df)
            print(f"  -> Buffered {len(df)} campaigns in memory.")

        except Exception as e:
            error_msg = f"Failed for {account_name} ({customer_id}): {e}"
            print(error_msg)
            failed_accounts.append(error_msg)

    # --- SINGLE BIGQUERY UPLOAD AT THE END ---
    if master_df_list:
        print("\n--- Pushing Batch to BigQuery ---")
        final_df = pd.concat(master_df_list, ignore_index = True)
        load_batch_to_bigquery(final_df, bq_client)
    else:
        print("\n--- No data extracted to load. ---")

    if failed_accounts:
        print("\nCRITICAL: The following accounts failed extraction:")
        for err in failed_accounts:
            print(f" - {err}")
        raise Exception(f"Script completed with errors in {len(failed_accounts)} accounts.")

    print("\n--- Google Campaign Dimensions Load Complete ---")


if __name__ == "__main__":
    main()
