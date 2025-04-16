from google.cloud import bigquery
import boto3
import pandas as pd
import os
import uuid

# BigQuery setup
bq_client = bigquery.Client()

# Get credentials from environment variables
aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Create a temporary table to store results
temp_table_id = f"temp_export_{uuid.uuid4().hex[:8]}"
temp_dataset = "analytics"  # Use a dataset where you have write permissions
temp_table_ref = f"{bq_client.project}.{temp_dataset}.{temp_table_id}"

# Set up the query job with a destination table
job_config = bigquery.QueryJobConfig(
    destination=temp_table_ref,
    write_disposition="WRITE_TRUNCATE"  # Overwrite the table if it exists
)

# Query data
query = """
SELECT * FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk` ORDER BY ts ASC
"""

# Run the query and wait for it to complete
query_job = bq_client.query(query, job_config=job_config)
query_job.result()  # Wait for the query to complete

print(f"Query results saved to temporary table: {temp_table_ref}")

# Now extract the data from the temporary table to GCS
# (or download in chunks if you prefer)
local_filename = "ozone_liveramp_ad_requests_last7_20250416.csv"

# Download in chunks
print("Downloading data in chunks...")
rows_per_chunk = 100000  # Adjust based on your memory constraints
offset = 0
chunk_counter = 0
write_mode = 'w'  # First chunk overwrites, subsequent chunks append

query_paged = f"SELECT * FROM `{temp_table_ref}`"

while True:
    chunk_query = f"{query_paged} LIMIT {rows_per_chunk} OFFSET {offset}"
    chunk_df = bq_client.query(chunk_query).result().to_dataframe()
    
    if chunk_df.empty:
        break  # No more data
    
    # Write to CSV (header only for first chunk)
    chunk_df.to_csv(local_filename, mode=write_mode, index=False, header=(write_mode == 'w'))
    
    print(f"Processed chunk {chunk_counter + 1} ({len(chunk_df)} rows)")
    offset += rows_per_chunk
    chunk_counter += 1
    write_mode = 'a'  # Switch to append mode after first chunk

# Clean up the temporary table
bq_client.delete_table(temp_table_ref, not_found_ok=True)
print(f"Temporary table {temp_table_ref} deleted")

# Upload to S3
print("Uploading to S3...")
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

s3_client.upload_file(
    local_filename,     
    'com-liveramp-eu-customer-uploads',     
    '1777/2/ozone_liveramp_ad_requests_last7_20250416.csv'
)
print("Upload complete!")