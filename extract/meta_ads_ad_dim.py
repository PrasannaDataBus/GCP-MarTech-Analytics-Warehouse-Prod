# Process: Extract Ad Definition (Bridge Table: Ad ID -> Creative ID) and Load into BigQuery
# Data Points: Meta Ads Library via Graph API
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Partitioning: None (Dimension Table)
# Clustering: Not necessary for small dimension tables
# Loading Strategy: Full Refresh (WRITE_TRUNCATE) - Runs Weekly/Monthly
# MarTech Dictionary: Refer SharePoint file - MarTech Data Dictionary

import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery

# --- Meta SDK Imports ---
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.exceptions import FacebookRequestError


# --- ENVIRONMENT LOADING LOGIC ---
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

# --- Set Google credentials dynamically (Safe String) ---
if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME")
META_AD_DIM_TABLE_NAME = os.getenv("META_AD_DIM_TABLE_NAME")

# --- FIELDS TO EXTRACT ---
FIELDS = [
    'id',
    'name',
    'status',
    'creative', # Contains {id: ...}
    'created_time',
    'updated_time'
]


# ---------------------------------------------------------
# HELPER: LAZY LOAD CLIENTS (BQ Only for Meta Script)
# ---------------------------------------------------------
def get_bq_client():
    """Safe BQ Client initialization"""
    return bigquery.Client()


# --- HELPER: Get Ad Accounts ---
def get_ad_accounts():
    """Fetches all Ad Accounts attached to the System User."""
    me = User(fbid = 'me')
    my_accounts = me.get_ad_accounts(fields = ['name', 'account_id', 'currency'])

    accounts_list = []
    for acc in my_accounts:
        accounts_list.append({
            'id': acc['id'],
            'name': acc.get('name', 'Unnamed Account'),
            'currency': acc.get('currency', 'USD')
        })
    return accounts_list


# --- EXTRACTION FUNCTION (Updated with Low Batch Size) ---
def extract_ad_definitions(account_id: str, account_name: str):
    """Extract Ad Dimensions to merge with Performance"""

    print(f"Querying Ad Dimensions for {account_name} ({account_id})...")

    formatted_id = account_id if account_id.startswith('act_') else f"act_{account_id}"
    account = AdAccount(formatted_id)

    data_rows = []

    try:
        # Fetch ADS using the corrected FIELDS list
        ads = account.get_ads(fields = FIELDS, params = {'limit': 500})

        # Iterate through the cursor (pagination is automatic in the SDK loop)
        count = 0
        for ad in ads:
            count += 1
            if count % 500 == 0:
                print(f"  ...fetched {count} ads dimensions")

            creative_data = ad.get('creative', {})

            data_rows.append({
                'ad_id': ad['id'],
                'ad_name': ad.get('name'),
                'account_id': account_id.replace("act_", ""),
                'account_name': account_name,
                'status': ad.get('status'),
                'creative_id': creative_data.get('id'),  # THE KEY LINK
                'created_time': ad.get('created_time'),
                'updated_time': ad.get('updated_time'),
                '_ingested_at': datetime.now()
            })

    except FacebookRequestError as e:
        print(f"Meta API Error for {account_name}: {e.api_error_message()}")
        # Return whatever we managed to grab before the error
        return pd.DataFrame(data_rows)

    df = pd.DataFrame(data_rows)
    return df


# --- LOAD FUNCTION (Full Refresh) ---
def load_to_bigquery(df: pd.DataFrame, bq_client):
    if META_AD_DIM_TABLE_NAME is None:
        raise ValueError("META_AD_DIM_TABLE_NAME is not set in .env")

    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{META_AD_DIM_TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_TRUNCATE",  # Wipes the table clean and reloads
        schema = [
            bigquery.SchemaField("ad_id", "STRING"),
            bigquery.SchemaField("ad_name", "STRING"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("account_name", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("creative_id", "STRING"),  # Bridge Column
            bigquery.SchemaField("created_time", "STRING"),
            bigquery.SchemaField("updated_time", "STRING"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ]
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()
    print(f"  -> Loaded {len(df)} rows into {table_id} (Mode: TRUNCATE)")


# --- MAIN EXECUTION ---
def main():
    # 1. Load Environment & Init BQ
    load_environment()
    bq_client = get_bq_client()  # BQ needed here

    # 2. READ CREDENTIALS (AFTER load_environment has run)
    META_APP_ID = os.getenv("META_APP_ID")
    META_APP_SECRET = os.getenv("META_APP_SECRET")
    META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

    # 3. Check Credentials availability after loading
    if not all([META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN]):
        print("Error: Missing Meta credentials (APP_ID, SECRET, TOKEN). Check params.env.")
        return

    # 4. INITIALIZE META API (This was missing/misplaced in your error)
    try:
        FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN)
        print("Meta API Initialized Successfully")
    except Exception as e:
        print(f"Failed to initialize Meta API: {e}")
        return

    print("--- Starting Meta Ad dimensions Extraction ---")

    # 5. Get Accounts
    try:
        accounts = get_ad_accounts()
        print(f"Found {len(accounts)} Ad Accounts.")
    except Exception as e:
        print(f"Error fetching ad accounts: {e}")
        return

    all_ads_dfs = []

    # 6. Loop and Extract
    for acc in accounts:
        acc_id = acc['id']
        acc_name = acc['name']

        try:
            df = extract_ad_definitions(acc_id, acc_name)
            if not df.empty:
                all_ads_dfs.append(df)
                print(f"     {acc_name}: Found {len(df)} Ad dimensions.")
            else:
                print(f"     {acc_name}: No Ad dimensions found.")
        except Exception as e:
            print(f"     Failed for {acc_name}: {e}")

    # 7. Consolidate and Load
    if all_ads_dfs:
        print("\nConsolidating all Ad dimensions from all accounts...")
        master_df = pd.concat(all_ads_dfs, ignore_index = True)

        # Deduplicate based on ad_id
        master_df_deduped = master_df.drop_duplicates(subset = ['ad_id']).reset_index(drop = True)
        print(f"Total Unique Ad dimensions to Load: {len(master_df_deduped)}")

        try:
            load_to_bigquery(master_df_deduped, bq_client)
            print("--- Meta Ad dimensions Dimension Load Complete ---")
        except Exception as e:
            print(f"Failed to load to BigQuery: {e}")
    else:
        print("No Ad dimensions extracted from any account.")


if __name__ == "__main__":
    main()