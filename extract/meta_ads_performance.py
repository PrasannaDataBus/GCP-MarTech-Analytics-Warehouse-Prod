# Process: Extract Raw Data and Injest into a raw schema inside a raw table
# Data Points: Several Meta Ads Account via API
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Partitioning: Assigned in this script (By date)
# Clustering: Assigned in this script (By important / relevant columns)
# Incremental Loading: Time Travel window (14 Days)
# Reliability Logic: Implemented 30-day Date Chunking to prevent API Timeouts during backfills
# MarTech Dictionary: Refer SharePoint file - MarTech Data Dictionary

import os
import time
import re
from pathlib import Path
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery

# --- Meta SDK Imports ---
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.exceptions import FacebookRequestError


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
META_PERFORMANCE_TABLE_NAME = os.getenv("META_PERFORMANCE_TABLE_NAME")

# --- META CREDENTIALS ---
META_APP_ID = os.getenv("META_APP_ID")
META_APP_SECRET = os.getenv("META_APP_SECRET")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

# --- AUTHENTICATION ---
bq_client = bigquery.Client()

# --- FIELDS TO EXTRACT ---
FIELDS = [
    'date_start',
    'date_stop',
    'account_id',
    'account_name',
    'campaign_id',
    'campaign_name',
    'adset_id',
    'adset_name',
    'ad_id',
    'ad_name',
    'impressions',
    'clicks',
    'spend',
    'ctr',
    'cpc',
    'cpm',
    'reach',
    'frequency',
    'actions', # Conversions List
    'action_values' # Revenue List
]


# --- HELPER: Date Chunks (The Fix for Timeouts) ---
def get_date_chunks(start_date, end_date, chunk_size=30):
    """Splits a date range into smaller chunks to avoid API timeouts."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    current_start = start
    while current_start <= end:
        current_end = current_start + timedelta(days = chunk_size)
        if current_end > end:
            current_end = end
        yield current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")
        current_start = current_end + timedelta(days = 1)


# --- HELPER: Get Ad Accounts ---
def get_ad_accounts():
    """Fetches all Ad Accounts attached to the System User."""
    me = User(fbid = 'me')
    my_accounts = me.get_ad_accounts(fields = ['name', 'account_id', 'currency'])

    accounts_list = []
    for acc in my_accounts:
        accounts_list.append({
            'id': acc['id'],  # format: act_12345678
            'name': acc.get('name', 'Unnamed Account'),
            'currency': acc.get('currency', 'USD')
        })
    return accounts_list


# --- EXTRACTION FUNCTION (Updated with Chunking) ---
def extract_meta_performance_data(account_id: str, account_name: str, start_date: str, end_date: str, currency: str):
    """Extracts performance data for a specific Meta Ad Account using date chunking."""

    # Ensure 'act_' prefix
    formatted_id = account_id if account_id.startswith('act_') else f"act_{account_id}"
    account = AdAccount(formatted_id)

    all_data_rows = []

    # Loop through 30-day chunks
    for chunk_start, chunk_end in get_date_chunks(start_date, end_date, chunk_size = 30):
        print(f"  -> Fetching chunk: {chunk_start} to {chunk_end}...")

        time_range = {'since': chunk_start, 'until': chunk_end}

        params = {
            'time_range': time_range,
            'time_increment': 1,
            'level': 'ad',
            'limit': 500,
        }

        try:
            # Request insights for this specific chunk
            insights = account.get_insights(fields = FIELDS, params = params)

            # --- Processing Loop ---
            for item in insights:
                total_conversions = 0.0
                total_conversion_value = 0.0

                if 'actions' in item:
                    for action in item['actions']:
                        total_conversions += float(action['value'])

                if 'action_values' in item:
                    for val in item['action_values']:
                        total_conversion_value += float(val['value'])

                row = {
                    "date": item['date_start'],
                    "account_id": item['account_id'],
                    "account_name": item['account_name'],
                    "campaign_id": item['campaign_id'],
                    "campaign_name": item['campaign_name'],
                    "adset_id": item['adset_id'],
                    "adset_name": item['adset_name'],
                    "ad_id": item['ad_id'],
                    "ad_name": item['ad_name'],

                    "impressions": int(item.get('impressions', 0)),
                    "clicks": int(item.get('clicks', 0)),
                    "spend": float(item.get('spend', 0.0)),
                    "ctr": float(item.get('ctr', 0.0)),
                    "average_cpc": float(item.get('cpc', 0.0)),
                    "cpm": float(item.get('cpm', 0.0)),
                    "reach": int(item.get('reach', 0)),
                    "frequency": float(item.get('frequency', 0.0)),

                    "conversions": total_conversions,
                    "conversion_value": total_conversion_value,

                    "currency": currency,
                    "_ingested_at": datetime.now()
                }
                all_data_rows.append(row)

            # Sleep briefly to be kind to the API rate limits between chunks
            time.sleep(1)

        except FacebookRequestError as e:
            print(f"Meta API Error for {account_name} (Chunk {chunk_start}): {e.api_error_message()}")
            # We continue to the next chunk instead of failing the whole year
            continue

    df = pd.DataFrame(all_data_rows)
    if not df.empty:
        df['date'] = pd.to_datetime(df['date']).dt.date

    return df


# --- HELPER: Get Last Loaded Date ---
def get_last_loaded_date():
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{META_PERFORMANCE_TABLE_NAME}"
    try:
        query = f"SELECT MAX(date) AS max_date FROM `{table_id}`"
        results = bq_client.query(query).result()
        first = next(results, None)
        if first and first.max_date: return first.max_date
    except Exception:
        return None
    return None


# --- LOAD FUNCTION (Idempotent) ---
def load_to_bigquery(df: pd.DataFrame, start_date: str, end_date: str, account_id: str):
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{META_PERFORMANCE_TABLE_NAME}"

    clean_acc_id = df['account_id'].iloc[0]

    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        AND account_id = '{clean_acc_id}'
    """
    try:
        bq_client.query(delete_query).result()
        print(f"  -> Cleared overlap for {clean_acc_id}")
    except Exception:
        pass

    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_APPEND",
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("account_id", "STRING"),
            bigquery.SchemaField("account_name", "STRING"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("campaign_name", "STRING"),
            bigquery.SchemaField("adset_id", "STRING"),
            bigquery.SchemaField("adset_name", "STRING"),
            bigquery.SchemaField("ad_id", "STRING"),
            bigquery.SchemaField("ad_name", "STRING"),

            bigquery.SchemaField("impressions", "INTEGER"),
            bigquery.SchemaField("clicks", "INTEGER"),
            bigquery.SchemaField("spend", "FLOAT"),
            bigquery.SchemaField("ctr", "FLOAT"),
            bigquery.SchemaField("average_cpc", "FLOAT"),
            bigquery.SchemaField("cpm", "FLOAT"),
            bigquery.SchemaField("reach", "INTEGER"),
            bigquery.SchemaField("frequency", "FLOAT"),

            bigquery.SchemaField("conversions", "FLOAT"),
            bigquery.SchemaField("conversion_value", "FLOAT"),

            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        ],
        time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.DAY,
            field = "date",
        ),
        clustering_fields = ["account_id", "campaign_id", "adset_id"]
    )

    job = bq_client.load_table_from_dataframe(df, table_id, job_config = job_config)
    job.result()
    print(f"  -> Loaded {len(df)} rows into {table_id}")


# --- MAIN EXECUTION ---
def main():
    # 1. Load Environment Variables
    load_environment()

    # 2. Check Credentials availability after loading
    if not all([META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN]):
        print("Error: Missing Meta credentials (APP_ID, SECRET, TOKEN). Check params.env.")
        return

    # 3. INITIALIZE META API (This was missing/misplaced in your error)
    try:
        FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN)
        print("Meta API Initialized Successfully")
    except Exception as e:
        print(f"Failed to initialize Meta API: {e}")
        return

    print("--- Starting Meta Ads Performance Extraction ---")

    # 4. Get Accounts (Now this will work because API is init)
    try:
        accounts = get_ad_accounts()
        print(f"Found {len(accounts)} Ad Accounts.")
    except Exception as e:
        print(f"Error fetching ad accounts: {e}")
        return

    # ==============================================================================
    # AUTOMATIC INCREMENTAL LOAD (DEFAULT)
    # ==============================================================================

    last_date = get_last_loaded_date()
    lookback_days = 14

    if last_date:
        start_date = (last_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        print(f"Incremental Mode: Last loaded {last_date}. Lookback {lookback_days} days.")
    else:
        start_date = "2022-01-01"
        print(f"Full Load Mode: Starting from {start_date}")

    end_date = date.today().strftime("%Y-%m-%d")

    for acc in accounts:
        acc_id = acc['id']
        acc_name = acc['name']
        currency = acc['currency']

        try:
            df = extract_meta_performance_data(acc_id, acc_name, start_date, end_date, currency)
            if not df.empty:
                print(f"Extracted {len(df)} rows for {acc_name}.")
                load_to_bigquery(df, start_date, end_date, acc_id)
            else:
                print(f"No data for {acc_name} in this range.")
        except Exception as e:
            print(f"Failed to process {acc_name}: {e}")

    # ==============================================================================
    # HISTORICAL BACKFILL (2023 - 2025)
    # Note: Do not uncomment the below without understanding that the below logic will
    # Append rows, it is important to provide the years = [] value. For Example: years = [2025]
    # When you provide the years values then the logic will filter out the raw data between {year}-01-01
    # and {year}-12-31. As i have already extracted and loaded the historical backfill data
    # so do not uncomment the below logic as you will "ONCE AGAIN DELETE AND RELOAD" the previously existing records
    # which will result in "DELETION OF EXISTING ROWS" + "RELOAD OF SAME ROWS" + "COST WILL BE INCURRED".
    # I did extracted from the year 2023 as Meta has timerange for historical backfills which is = 37 months
    # Today = 25/11/2025 minus 37 months = 10/2022 - so I skip 3 months in 2022 and I am starting the extraction from 2023
    # ==============================================================================

    # years = [2025]  # Define years to backfill
    #
    # for acc in accounts:
    #     acc_id = acc['id']
    #     acc_name = acc['name']
    #     currency = acc['currency']
    #
    #     print(f"\nProcessing Backfill for: {acc_name} ({acc_id})")
    #
    #     for yr in years:
    #         start_date = f"{yr}-01-01"
    #         end_date = f"{yr}-12-31"
    #
    #         # Cap end date at today if backfilling current year
    #         if int(yr) == date.today().year:
    #             end_date = date.today().strftime("%Y-%m-%d")
    #
    #         print(f"  -> Extracting Year {yr}: {start_date} to {end_date}")
    #
    #         try:
    #             df = extract_meta_performance_data(acc_id, acc_name, start_date, end_date, currency)
    #
    #             if not df.empty:
    #                 # Important: We pass the specific dates to ensure DELETE works correctly for this slice
    #                 load_to_bigquery(df, start_date, end_date, acc_id)
    #             else:
    #                 print(f"     No data for {yr}.")
    #
    #         except Exception as e:
    #             print(f"     Failed for {yr}: {e}")
    #
    print("--- Meta Ads Load Complete ---")


if __name__ == "__main__":
    main()