# Process: Extract Raw Data and Injest into a raw schema inside a raw table
# Data Points: Several Google Ads Account via API
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Partitioning: Assigned in this script (By date)
# Clustering: Assigned in this script (By important / relevant columns)
# Incremental Loading: Time Travel window (14 Days)
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
if __name__ == "__main__":
    current_env = load_environment()
    print(f"Running in {current_env} environment")

# --- Set Google credentials dynamically ---
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME")
AUDIENCE_DIM_TABLE_NAME = os.getenv("AUDIENCE_DIM_TABLE_NAME")

# --- AUTHENTICATION ---
ads_config_path = os.getenv("GOOGLE_ADS_CONFIG")
ads_client = GoogleAdsClient.load_from_storage(ads_config_path)
bq_client = bigquery.Client()

# --- GAQL Query for AUDIENCE DIMENSIONS---
QUERY_TEMPLATE = """
SELECT
  ad_group.id,
  ad_group_criterion.criterion_id,
  ad_group_criterion.display_name,
  ad_group_criterion.type
FROM ad_group_criterion
"""


# --- FETCH ALL CLIENT ACCOUNTS (for MCC) ---
def get_child_accounts(manager_customer_id: str):
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
        request = {"customer_id": manager_customer_id, "query": query}
    )

    accounts = []
    for row in response:
        accounts.append({
            "id": row.customer_client.id,
            "name": row.customer_client.descriptive_name or "Unnamed Account"
        })
    return accounts


def extract_audience_dimension(customer_id: str):
    """Extracts AUDIENCE DIMENSION data from Google Ads for a specific account and date range."""
    service = ads_client.get_service("GoogleAdsService")
    query = QUERY_TEMPLATE

    # Use service.search (not search_stream) because this is a small, non-segmented query
    response = service.search(customer_id=customer_id, query=query)

    rows = []

    # --- DEFINE THE AUDIENCE TYPES WE CARE ABOUT ---
    # These are the *string names* of the enums
    AUDIENCE_TYPES = [
        'AFFINITY',
        'IN_MARKET',
        'USER_LIST',
        'CUSTOM_AUDIENCE',
        'CUSTOM_INTENT'
    ]

    for row in response:
        criterion_type_name = getattr(row.ad_group_criterion.type, "name", None)
        if criterion_type_name in AUDIENCE_TYPES:
            rows.append({
                "ad_group_id": str(row.ad_group.id),
                "audience_criterion_id": str(row.ad_group_criterion.criterion_id),
                "audience_name": row.ad_group_criterion.display_name,
                "audience_type": criterion_type_name,
                "_ingested_at": datetime.now(timezone.utc)
            })

    df = pd.DataFrame(rows)

    # Add deduplication - an audience can be in multiple ad groups but has one name
    if not df.empty:
        df = df.drop_duplicates(subset = ["audience_criterion_id"]).reset_index(drop = True)

    return df


# --- LOAD TO BIGQUERY (INCREMENTAL) ---
def load_audience_dim_to_bigquery(df: pd.DataFrame):
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{AUDIENCE_DIM_TABLE_NAME}"

    # Check if the table_id is valid before proceeding
    if AUDIENCE_DIM_TABLE_NAME is None:
        raise ValueError("AUDIENCE_DIM_TABLE_NAME environment variable is not set. Check your .env file.")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", # This overwrites the table completely
                                        schema = [
                                            bigquery.SchemaField("ad_group_id", "STRING"),  # Useful to keep
                                            bigquery.SchemaField("audience_criterion_id", "STRING"),
                                            bigquery.SchemaField("audience_name", "STRING"),
                                            bigquery.SchemaField("audience_type", "STRING"),
                                            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
                                        ],
    )

    # --- Load the DataFrame into BigQuery ---
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id} (Mode: TRUNCATE)")


# --- MAIN (for Audience Dimension Load) ---
def main():
    manager_id = ads_client.login_customer_id or ads_client.client_customer_id
    child_accounts = get_child_accounts(manager_id)
    print(f"Found {len(child_accounts)} client accounts under manager {manager_id}")

    print("\n--- Starting Audience Dimension Load (Full Refresh) ---")

    # This list will hold the DataFrames from all 12 accounts
    all_audience_dfs = []

    for account in child_accounts:
        customer_id = str(account["id"])
        account_name = account["name"]
        print(f"Extracting Audience Dimension for: {account_name} ({customer_id})")

        try:
            # 1. Call your new dimension extraction function
            df_dim = extract_audience_dimension(customer_id)

            if df_dim.empty:
                print(f"No audience dimension data for {account_name}")
                continue

            # 2. Add the data to our master list
            all_audience_dfs.append(df_dim)

        except Exception as e:
            print(f"Failed Audience Dimension for {account_name} ({customer_id}): {e}")

    # --- After looping, combine, deduplicate, and load ---
    if not all_audience_dfs:
        print("No audience dimension data found in any account. Exiting.")
        return

    print("\nCombining and deduplicating audience data from all accounts...")

    # 3. Combine all DataFrames into one
    master_df = pd.concat(all_audience_dfs, ignore_index = True)

    # 4. Deduplicate to get a clean list of unique audiences
    master_df_deduped = master_df.drop_duplicates(subset = ["audience_criterion_id"]).reset_index(drop = True)

    print(f"Found {len(master_df_deduped)} unique audiences to load.")

    try:
        # 5. Call the dimension load function (uses WRITE_TRUNCATE)
        load_audience_dim_to_bigquery(master_df_deduped)
        print("--- Successfully loaded Audience Dimension ---")
    except Exception as e:
        print(f"Failed to load master Audience Dimension table: {e}")


if __name__ == "__main__":
    main()
