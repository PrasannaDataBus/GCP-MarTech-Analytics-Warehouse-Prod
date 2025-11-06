# Test: BigQuery connection test

# from google.cloud import bigquery
#
# client = bigquery.Client()
# print("Connected to project:", client.project)

#Test 2: Oauth test

from google.ads.googleads.client import GoogleAdsClient

# Load the configuration file
client = GoogleAdsClient.load_from_storage("key/google-ads.yaml")


def main():
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
            customer.id,
            customer.descriptive_name,
            campaign.id,
            campaign.name,
            campaign.status
        FROM campaign
        ORDER BY campaign.id
        LIMIT 10
    """

    # Get your client ID (the Ads account you want to query)
    customer_id = client.login_customer_id or client.client_customer_id

    # Execute query
    response = ga_service.search(customer_id=customer_id, query=query)

    print("\nConnection successful! Sample campaigns:\n")
    for row in response:
        print(f"- {row.campaign.name} (Status: {row.campaign.status.name})")


if __name__ == "__main__":
    main()


