from google.cloud import bigquery
import boto3
import pandas as pd
import os

# BigQuery setup
bq_client = bigquery.Client()

# Get credentials from environment variables
aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Query data
query = """
SELECT * FROM `analytics.046_boots_campaign_liveramp_daily_report_table` ORDER BY timestamp ASC
"""
query_results = bq_client.query(query).result()

# Convert to DataFrame (optional)
df = query_results.to_dataframe()

# Alternatively, save directly to a CSV file
df.to_csv("OZ-16511-Boots-Baby-Mar-Aug-25_liveramp_20250410.csv", index=False)

# Upload to S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

s3_client.upload_file(
    'OZ-16511-Boots-Baby-Mar-Aug-25_liveramp_20250410.csv',     
    'com-liveramp-eu-customer-uploads',     
    '1777/1/OZ-16511-Boots-Baby-Mar-Aug-25_liveramp_20250410.csv'
)
print("Upload complete!")