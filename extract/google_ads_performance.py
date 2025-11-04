from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
from datetime import datetime, timezone, date
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from params.env
load_dotenv(r"C:\Users\prasa\Root\GCP MarTech Analytics Warehouse\params.env")

# Set Google credentials dynamically
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET_NAME = os.getenv("RAW_DATASET_NAME")
PERFORMANCE_TABLE_NAME = os.getenv("PERFORMANCE_TABLE_NAME")

# --- AUTHENTICATION ---
ads_config_path = os.getenv("GOOGLE_ADS_CONFIG")
ads_client = GoogleAdsClient.load_from_storage(ads_config_path)
bq_client = bigquery.Client()

# --- GAQL QUERY (all fields) ---
QUERY_TEMPLATE = """
SELECT
  segments.date,
  customer.id,
  customer.descriptive_name,
  customer.currency_code,
  campaign.id,
  campaign.name,
  campaign.status,
  campaign.advertising_channel_type,
  campaign.bidding_strategy_type,
  ad_group.id,
  ad_group.name,
  ad_group_ad.ad.id,
  ad_group_ad.ad.name,
  ad_group_ad.ad.type,
  segments.device,
  segments.ad_network_type,
  metrics.impressions,
  metrics.clicks,
  metrics.ctr,
  metrics.average_cpc,
  metrics.cost_micros,
  metrics.conversions,
  metrics.conversions_value,
  metrics.all_conversions,
  metrics.view_through_conversions,
  metrics.engagements
FROM ad_group_ad
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
        request={"customer_id": manager_customer_id, "query": query}
    )

    accounts = []
    for row in response:
        accounts.append({
            "id": row.customer_client.id,
            "name": row.customer_client.descriptive_name or "Unnamed Account"
        })
    return accounts


def extract_ads_data(customer_id: str, start_date: str, end_date: str):
    """Extracts performance data from Google Ads for a specific account and date range."""
    service = ads_client.get_service("GoogleAdsService")
    query = QUERY_TEMPLATE.format(start_date=start_date, end_date=end_date)
    response = service.search_stream(customer_id=customer_id, query=query)

    rows = []
    for batch in response:
        for row in batch.results:
            rows.append({
                "date": row.segments.date,
                "account_id": str(row.customer.id),  # Cast to string
                "account_name": row.customer.descriptive_name,
                "campaign_id": str(row.campaign.id), # Cast to string
                "campaign_name": row.campaign.name,
                "campaign_status": row.campaign.status.name,
                "ad_group_id": str(row.ad_group.id), # Cast to string
                "ad_group_name": row.ad_group.name,
                "ad_id": str(row.ad_group_ad.ad.id) if row.ad_group_ad.ad else None,
                "ad_name": getattr(row.ad_group_ad.ad, "name", None),
                "ad_type": getattr(row.ad_group_ad.ad.type_, "name", None),
                "ad_network_type": getattr(row.segments.ad_network_type, "name", None),
                "device": getattr(row.segments.device, "name", None),
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
                "engagements": row.metrics.engagements,
                "bidding_strategy_type": getattr(row.campaign.bidding_strategy_type, "name", None),
                "currency": row.customer.currency_code,
                "_ingested_at": datetime.now(timezone.utc)
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.astype({
            "impressions": "int64",
            "clicks": "int64",
            "engagements": "int64",
            "cost_micros": "int64",
            "ctr": "float64",
            "average_cpc": "float64",
            "conversions": "float64",
            "conversions_value": "float64",
            "all_conversions": "float64",
            "view_through_conversions": "float64",
    })
    return df


def load_to_bigquery(df: pd.DataFrame):
    # Convert date column safely to datetime.date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors = "coerce").dt.date

    table_id = f"{PROJECT_ID}.{RAW_DATASET_NAME}.{PERFORMANCE_TABLE_NAME}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    # define schema explicitly to ensure BigQuery types match
    job_config.schema = [
        bigquery.SchemaField("date", "DATE"),
    ]

    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")


# --- MAIN EXECUTION ---
def main():
    manager_id = ads_client.login_customer_id or ads_client.client_customer_id
    child_accounts = get_child_accounts(manager_id)

    print(f"Found {len(child_accounts)} client accounts under manager {manager_id}")

    for account in child_accounts:
        customer_id = str(account["id"])
        account_name = account["name"]
        print(f"\nExtracting for account: {account_name} ({customer_id})")

        years = [2025]  # Start with one year test, expand later
        for yr in years:
            start_date = f"{yr}-01-01"
            end_date = f"{yr}-12-31" if yr < date.today().year else str(date.today())

            print(f"Extracting {start_date} → {end_date}")
            try:
                df = extract_ads_data(customer_id, start_date, end_date)

                if df.empty:
                    print(f"No data for {yr} in {account_name}")
                    continue

                print(f"Extracted {len(df)} rows for {yr} ({account_name})")
                load_to_bigquery(df)

            except Exception as e:
                print(f"Failed for {account_name} ({customer_id}): {e}")


if __name__ == "__main__":
    main()
