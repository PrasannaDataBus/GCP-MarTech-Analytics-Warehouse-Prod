from google.cloud import bigquery

client = bigquery.Client()
print("Connected to project:", client.project)