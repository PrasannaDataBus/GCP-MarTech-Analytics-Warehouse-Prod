# Test 1: BigQuery connection test

# from google.cloud import bigquery
#
# client = bigquery.Client()
# print("Connected to project:", client.project)

#Test 2: Oauth test

# from google.ads.googleads.client import GoogleAdsClient
#
# # Load the configuration file
# client = GoogleAdsClient.load_from_storage("key/google-ads.yaml")
#
#
# def main():
#     ga_service = client.get_service("GoogleAdsService")
#
#     query = """
#         SELECT
#             customer.id,
#             customer.descriptive_name,
#             campaign.id,
#             campaign.name,
#             campaign.status
#         FROM campaign
#         ORDER BY campaign.id
#         LIMIT 10
#     """
#
#     # Get your client ID (the Ads account you want to query)
#     customer_id = client.login_customer_id or client.client_customer_id
#
#     # Execute query
#     response = ga_service.search(customer_id=customer_id, query=query)
#
#     print("\nConnection successful! Sample campaigns:\n")
#     for row in response:
#         print(f"- {row.campaign.name} (Status: {row.campaign.status.name})")
#
#
# if __name__ == "__main__":
#     main()

# Test 3: META ADS connection test

import os
import sys
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount

# --- 1. Load Environment Variables ---
# Try loading from params.env (your project structure) or .env
env_path = "params.env" if os.path.exists("params.env") else ".env"
if os.path.exists(env_path):
    load_dotenv(env_path, override = True)
    print(f"Loaded configuration from {env_path}")
else:
    print("Error: No .env or params.env file found.")
    sys.exit(1)

# --- 2. Get Credentials ---
my_app_id = os.getenv("META_APP_ID")
my_app_secret = os.getenv("META_APP_SECRET")
my_access_token = os.getenv("META_ACCESS_TOKEN")

if not all([my_app_id, my_app_secret, my_access_token]):
    print("Error: Missing Meta credentials in environment file.")
    print("Ensure META_APP_ID, META_APP_SECRET, and META_ACCESS_TOKEN are set.")
    sys.exit(1)


def main():
    print("\n--- Starting Meta API Connection Test ---")

    # --- 3. Initialize API ---
    try:
        FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
        print("API Initialized successfully.")
    except Exception as e:
        print(f"API Initialization Failed: {e}")
        return

    # --- 4. Verify User (The System User) ---
    try:
        # 'me' refers to the System User linked to the token
        me = User(fbid = 'me').api_get(fields = ['name', 'id'])
        print(f"Connected as System User: {me['name']} (ID: {me['id']})")
    except Exception as e:
        print(f"Failed to fetch System User. Check your Access Token. Error: {e}")
        return

    # --- 5. List Ad Accounts ---
    print("\n--- Fetching Ad Accounts ---")
    try:
        # Fetch accounts connected to this System User
        my_accounts = me.get_ad_accounts(fields = ['name', 'account_id', 'currency'])

        if not my_accounts:
            print("No Ad Accounts found. Did you assign assets to the System User in Business Settings?")
            return

        print(f"Found {len(my_accounts)} Ad Accounts:\n")

        # --- 6. List Campaigns for the First Account ---
        # We take the first account to test campaign permissions
        first_account = my_accounts[0]
        print(f"Testing Account: {first_account['name']} ({first_account['account_id']})")

        # Fetch 5 campaigns
        campaigns = first_account.get_campaigns(
            fields = ['name', 'status', 'objective'],
            params = {'limit': 5}
        )

        if campaigns:
            print("Campaigns fetched successfully:")
            for campaign in campaigns:
                print(f"  - {campaign['name']} (Status: {campaign['status']})")
        else:
            print("Connection successful, but no campaigns found in this account.")

    except Exception as e:
        print(f"Error fetching data: {e}")

    print("\n--- Test Complete ---")


if __name__ == "__main__":
    main()