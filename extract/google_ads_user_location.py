# Process: Extract Raw Data and Injest into a raw schema inside a raw table
# Data Points: Several Google Ads Account via API
# Orchestration: Airflow-Docker-Dev

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
USER_LOCATION_TABLE_NAME = os.getenv("USER_LOCATION_TABLE_NAME")

# --- AUTHENTICATION ---
ads_config_path = os.getenv("GOOGLE_ADS_CONFIG")
ads_client = GoogleAdsClient.load_from_storage(ads_config_path)
bq_client = bigquery.Client()

# --- GAQL Query for USER LOCATION (City/Region/Country)---
QUERY_TEMPLATE = """
SELECT
  segments.date,
  customer.id,
  customer.descriptive_name,
  customer.currency_code,
  campaign.id,
  campaign.name,
  campaign.status,
  campaign.bidding_strategy_type,
  ad_group.id,
  ad_group.name,
  user_location_view.country_criterion_id,
  user_location_view.targeting_location,
  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.average_cpc,
  metrics.cost_micros,
  metrics.conversions,
  metrics.conversions_value,
  metrics.all_conversions,
  metrics.view_through_conversions
FROM user_location_view
WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
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


def extract_user_location_data(customer_id: str, start_date: str, end_date: str):
    """Extracts USER LOCATION performance data from Google Ads for a specific account and date range."""
    service = ads_client.get_service("GoogleAdsService")
    query = QUERY_TEMPLATE.format(start_date=start_date, end_date=end_date)
    response = service.search_stream(customer_id=customer_id, query=query)

    rows = []
    for batch in response:
        for row in batch.results:
            rows.append({
                "date": row.segments.date,
                "account_id": str(row.customer.id),
                "account_name": row.customer.descriptive_name,
                "campaign_id": str(row.campaign.id),
                "campaign_name": row.campaign.name,
                "campaign_status": row.campaign.status.name,
                "ad_group_id": str(row.ad_group.id),
                "ad_group_name": row.ad_group.name,
                "user_geo_criterion_id": str(row.user_location_view.country_criterion_id),
                "is_targeting_location": row.user_location_view.targeting_location,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "ctr": row.metrics.ctr,
                "average_cpc": (
                    float(row.metrics.average_cpc.micros) / 1_000_000
                    if getattr(row.metrics.average_cpc, "micros", None) is not None
                    else None
                ),
                "cost_micros": row.metrics.cost_micros,
                "conversions": row.metrics.conversions,
                "conversions_value": row.metrics.conversions_value,
                "all_conversions": row.metrics.all_conversions,
                "view_through_conversions": row.metrics.view_through_conversions,
                "bidding_strategy_type": getattr(row.campaign.bidding_strategy_type, "name", None),
                "currency": row.customer.currency_code,
                "_ingested_at": datetime.now(timezone.utc)
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        df = df.astype({
            "impressions": "int64",
            "clicks": "int64",
            "cost_micros": "int64",
            "ctr": "float64",
            "average_cpc": "float64",
            "conversions": "float64",
            "conversions_value": "float64",
            "all_conversions": "float64",
            "view_through_conversions": "float64",
    })

    return df


# # --- FIND LAST LOADED DATE ---
def get_last_loaded_date():
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{USER_LOCATION_TABLE_NAME}"
    query = f"SELECT MAX(date) AS last_date FROM `{table_id}`"
    result = list(bq_client.query(query))
    last_date = result[0].last_date if result and result[0].last_date else None
    return last_date


# --- LOAD TO BIGQUERY (INCREMENTAL) ---
def load_to_bigquery(df: pd.DataFrame, start_date: str, end_date: str, account_name: str, account_id: str):
    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{USER_LOCATION_TABLE_NAME}"

    # Delete overlapping date range to ensure no duplicates
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE(date) BETWEEN '{start_date}' AND '{end_date}'
        AND account_id = '{account_id}'
    """
    bq_client.query(delete_query).result()
    print(f"Deleted existing rows for {account_name} ({account_id}) between {start_date} and {end_date}")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows for {account_name} ({account_id}) into {RAW_DATASET_NAME}.{USER_LOCATION_TABLE_NAME}")


# --- MAIN ---
def main():
    manager_id = ads_client.login_customer_id or ads_client.client_customer_id
    child_accounts = get_child_accounts(manager_id)
    print(f"Found {len(child_accounts)} client accounts under manager {manager_id}")

    last_loaded_date = get_last_loaded_date()
    lookback_days = 14  # configurable window for late updates
    if last_loaded_date:
        start_date = (last_loaded_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    else:
        start_date = "2022-01-01"  # fallback for first run

    end_date = date.today().strftime("%Y-%m-%d")

    print(f"Incremental load from {start_date} → {end_date}")

    for account in child_accounts:
        customer_id = str(account["id"])
        account_name = account["name"]
        print(f"\nExtracting for account: {account_name} ({customer_id})")

        try:
            df = extract_user_location_data(customer_id, start_date, end_date)
            if df.empty:
                print(f"No new or updated data for {account_name}")
                continue
            print(f"Extracted {len(df)} rows for {account_name}")
            load_to_bigquery(df, start_date, end_date, account_name, customer_id)
        except Exception as e:
            print(f"Failed for {account_name} ({customer_id}): {e}")


if __name__ == "__main__":
    main()



# Note: Do not uncomment the below without understanding that the below logic will
# Append rows, it is important to provide the years = [] value. For Example: years = [2025]
# When you provide the years values then the logic will filter out the raw data between {year}-01-01
# and {year}-12-31. As i have already extracted and loaded the historical backfill data
# so do not uncomment the below logic as you will overwrite the previously existing same records
# which will result in duplicate rows and cost will be incurred.

# # --- LOAD TO BIGQUERY (HISTORICAL BACKFILL 2022 - 2025)

# def load_to_bigquery(df):
#     # Convert date column safely to datetime.date
#     if "date" in df.columns:
#         df["date"] = pd.to_datetime(df["date"], errors = "coerce").dt.date
#
#     table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{USER_LOCATION_TABLE_NAME}"
#     job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
#
#     # define schema explicitly to ensure BigQuery types match
#     job_config.schema = [
#         bigquery.SchemaField("date", "DATE"),
#     ]
#
#     job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
#     job.result()
#     print(f"Loaded {len(df)} rows into {table_id}")
#
#
# # --- MAIN EXECUTION ---
# def main():
#     manager_id = ads_client.login_customer_id or ads_client.client_customer_id
#     child_accounts = get_child_accounts(manager_id)
#
#     print(f"Found {len(child_accounts)} client accounts under manager {manager_id}")
#
#     for account in child_accounts:
#         customer_id = str(account["id"])
#         account_name = account["name"]
#         print(f"\nExtracting USER LOCATION for account: {account_name} ({customer_id})")
#
#         years = [2025]  # Start with one year test, expand later
#         for yr in years:
#             start_date = f"{yr}-01-01"
#             end_date = f"{yr}-12-31" if yr < date.today().year else str(date.today())
#
#             print(f"Extracting USER LOCATION {start_date} → {end_date}")
#             try:
#                 df = extract_user_location_data(customer_id, start_date, end_date)
#
#                 if df.empty:
#                     print(f"No USER LOCATION data for {yr} in {account_name}")
#                     continue
#
#                 print(f"Extracted USER LOCATION {len(df)} rows for {yr} ({account_name})")
#                 load_to_bigquery(df)
#
#             except Exception as e:
#                 print(f"Failed USER LOCATION for {account_name} ({customer_id}): {e}")
#
#
# if __name__ == "__main__":
#     main()
