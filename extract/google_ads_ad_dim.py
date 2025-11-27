# Process: Extract Raw Data and Injest into a raw schema inside a raw table
# Data Points: Several Google Ads Account via API
# Orchestration: Airflow-Docker-Dev & Airflow-Docker-Prod
# Partitioning: Not needed as the size is small
# Clustering: Not needed as the size is small
# Incremental Loading: Not needed as the size is small / rather run once a week
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
PERFORMANCE_DIM_TABLE_NAME = os.getenv("PERFORMANCE_DIM_TABLE_NAME")

# --- GAQL Query for AD PERFORMANCE DIMENSIONS---
QUERY_TEMPLATE = """
SELECT
  ad.id,
  ad.type,
  ad.display_url,
  ad.final_urls,
  
  # Responsive Search Ad (RSA) assets
  ad.responsive_search_ad.headlines,
  ad.responsive_search_ad.descriptions,
  
  # Responsive Display Ad (RDA) assets
  ad.responsive_display_ad.headlines,
  ad.responsive_display_ad.descriptions,
  ad.responsive_display_ad.long_headline,
  
  # Expanded Text Ad (ETA) assets (legacy, but good for historical)
  ad.expanded_text_ad.headline_part1,
  ad.expanded_text_ad.headline_part2,
  ad.expanded_text_ad.headline_part3,
  ad.expanded_text_ad.description,
  ad.expanded_text_ad.description2

FROM ad
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
        request = {"customer_id": manager_customer_id, "query": query}
    )

    accounts = []
    for row in response:
        accounts.append({
            "id": row.customer_client.id,
            "name": row.customer_client.descriptive_name or "Unnamed Account"
        })
    return accounts


def extract_ad_creative_dim(customer_id: str, ads_client):
    """Extracts Ad Creative dimension data (ID, Type, Assets) for a specific account."""
    service = ads_client.get_service("GoogleAdsService")
    query = QUERY_TEMPLATE

    # Use service.search (not search_stream) because this is a small, non-segmented query
    response = service.search(customer_id=customer_id, query=query)

    rows = []
    for row in response:
        ad_id = str(row.ad.id)
        ad_type = row.ad.type.name

        headlines = []
        descriptions = []
        final_urls = [url for url in row.ad.final_urls]

        if ad_type == 'RESPONSIVE_SEARCH_AD':
            # These are AdTextAsset objects. We need their .text attribute.
            headlines = [h.text for h in row.ad.responsive_search_ad.headlines]
            descriptions = [d.text for d in row.ad.responsive_search_ad.descriptions]

        elif ad_type == 'RESPONSIVE_DISPLAY_AD':
            headlines = [h.text for h in row.ad.responsive_display_ad.headlines]
            descriptions = [d.text for d in row.ad.responsive_display_ad.descriptions]
            if row.ad.responsive_display_ad.long_headline:
                headlines.append(row.ad.responsive_display_ad.long_headline.text)

        elif ad_type == 'EXPANDED_TEXT_AD':
            if row.ad.expanded_text_ad.headline_part1:
                headlines.append(row.ad.expanded_text_ad.headline_part1)
            if row.ad.expanded_text_ad.headline_part2:
                headlines.append(row.ad.expanded_text_ad.headline_part2)
            if row.ad.expanded_text_ad.headline_part3:
                headlines.append(row.ad.expanded_text_ad.headline_part3)

            if row.ad.expanded_text_ad.description:
                descriptions.append(row.ad.expanded_text_ad.description)
            if row.ad.expanded_text_ad.description2:
                descriptions.append(row.ad.expanded_text_ad.description2)

        # Add other ad types (e.g., ImageAd) here if needed

        rows.append({
            "ad_id": ad_id,
            "ad_type": ad_type,
            "final_urls": final_urls,
            "headlines": headlines,
            "descriptions": descriptions,
            "_ingested_at": datetime.now(timezone.utc)
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.drop_duplicates(subset = ["ad_id"]).reset_index(drop = True)

    return df


# --- LOAD TO BIGQUERY
def load_ad_dim_to_bigquery(df: pd.DataFrame, bq_client):
    """Loads the Ad Creative dimension DataFrame to BigQuery (TRUNCATE mode)."""

    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{PERFORMANCE_DIM_TABLE_NAME}"

    # Check if the table_id is valid before proceeding
    if PERFORMANCE_DIM_TABLE_NAME is None:
        raise ValueError("PERFORMANCE_DIM_TABLE_NAME environment variable is not set. Check your .env file.")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", # This overwrites the table completely
                                        schema = [
                                            bigquery.SchemaField("ad_id", "STRING"),
                                            bigquery.SchemaField("ad_type", "STRING"),
                                            bigquery.SchemaField("final_urls", "STRING", mode = "REPEATED"), # ARRAY<STRING>
                                            bigquery.SchemaField("headlines", "STRING", mode = "REPEATED"), # ARRAY<STRING>
                                            bigquery.SchemaField("descriptions", "STRING", mode = "REPEATED"), # ARRAY<STRING>
                                            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
                                        ],
    )

    # --- Load the DataFrame into BigQuery ---
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id} (Mode: TRUNCATE)")


# --- MAIN (for Ad Performance Dimension Load) ---
def main():
    # 1. CALL THE NEW FUNCTION TO CONNECT
    ads_client, bq_client = get_clients()

    manager_id = ads_client.login_customer_id or ads_client.client_customer_id

    # 2. PASS THE CLIENTS AS ARGUMENTS
    child_accounts = get_child_accounts(manager_id, ads_client)
    print(f"Found {len(child_accounts)} client accounts under manager {manager_id}")

    # Define the IDs you want to ignore ---
    EXCLUDED_IDS = ['8024672713']

    print("\n--- Starting Ad Performance Dimension Load (Full Refresh) ---")

    # This list will hold the DataFrames from all 12 accounts
    all_ad_dfs = []

    # Initialize the failure tracking list
    failed_accounts = []

    for account in child_accounts:
        customer_id = str(account["id"])
        account_name = account["name"]

        # --- The Skip Logic ---
        if customer_id in EXCLUDED_IDS:
            print(f"Skipping known Test Account: {account_name} ({customer_id})")
            continue  # <--- Jumps to the next account immediately. No API call, no error.

        print(f"Extracting Ad Performance Dimension for: {account_name} ({customer_id})")

        try:
            # 1. Call your new dimension extraction function
            df_dim = extract_ad_creative_dim(customer_id, ads_client)

            if df_dim.empty:
                print(f"No Ad Performance dimension data for {account_name}")
                continue

            # 2. Add the data to our master list
            all_ad_dfs.append(df_dim)

        except Exception as e:
            # Define error_msg BEFORE using it
            error_msg = f"Failed Ad Performance Dimension for {account_name} ({customer_id}): {e}"
            print(error_msg)

            # Append error to list instead of ignoring it
            failed_accounts.append(error_msg)

    # --- After looping, combine, deduplicate, and load ---

    # CHECK: Do we have ANY data to process?
    if all_ad_dfs:
        print("\nCombining and deduplicating Ad Performance data from all accounts...")

        # 3. Combine all DataFrames into one
        master_df = pd.concat(all_ad_dfs, ignore_index = True)

        # 4. Deduplicate to get a clean list of unique Ad performance dim
        master_df_deduped = master_df.drop_duplicates(subset = ["ad_id"]).reset_index(drop = True)

        print(f"Found {len(master_df_deduped)} unique Ad Performance to load.")

        try:
            # 5. Call the dimension load function (uses WRITE_TRUNCATE)
            load_ad_dim_to_bigquery(master_df_deduped, bq_client)
            print("--- Successfully loaded Ad Performance Dimension ---")
        except Exception as e:
            print(f"Failed to load master Ad Performance Dimension table: {e}")
            raise e  # <--- This forces Airflow to mark the task as FAILED
    else:
        print("No Ad Performance dimension data found in successfully processed accounts.")
        # We do NOT return here, because we still need to check if accounts failed.

    # --- FINAL FAILURE CHECK ---
    # If there were ANY failures during the loop, raise an exception now.
    if failed_accounts:
        print("\nCRITICAL: The following accounts failed extraction:")
        for err in failed_accounts:
            print(f" - {err}")

        # This ensures Airflow marks the task as FAILED so you get the email/alert
        raise Exception(f"Script completed with errors in {len(failed_accounts)} accounts.")


if __name__ == "__main__":
    main()
