from google.cloud import bigquery
import boto3
import pandas as pd
import io
import os
import math

# BigQuery setup
bq_client = bigquery.Client()

# Get credentials from environment variables
aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Set up destination details
bucket_name = 'com-liveramp-eu-customer-uploads'
key_prefix = '1777/2/'

# Get date partitions
partition_query = """
SELECT DISTINCT DATE(ts) as partition_date
FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk`
ORDER BY partition_date
"""
partitions = list(bq_client.query(partition_query).result())
print(f"Found {len(partitions)} date partitions to process")

# Process each partition as multiple smaller files
for i, partition in enumerate(partitions):
    partition_date = partition.partition_date
    date_str = partition_date.strftime('%Y%m%d')
    print(f"Processing partition date: {partition_date} ({i+1}/{len(partitions)})")
    
    # Get count for this partition
    count_query = f"""
    SELECT COUNT(*) as total 
    FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk`
    WHERE DATE(ts) = '{partition_date}'
    """
    total_rows = list(bq_client.query(count_query).result())[0].total
    print(f"  Total rows for date {partition_date}: {total_rows}")
    
    # Process in chunks of 1 million rows (~500MB per file)
    rows_per_chunk = 1000000
    chunks_in_partition = math.ceil(total_rows / rows_per_chunk)
    
    for chunk in range(chunks_in_partition):
        offset = chunk * rows_per_chunk
        print(f"  Processing chunk {chunk+1}/{chunks_in_partition} (offset {offset})")
        
        # Create a filename for this chunk
        chunk_key = f"{key_prefix}ozone_liveramp_{date_str}_part{chunk+1}of{chunks_in_partition}.csv"
        
        # Query this partition chunk
        chunk_query = f"""
        SELECT ts, id_type, liveramp_id
        FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk`
        WHERE DATE(ts) = '{partition_date}'
        LIMIT {rows_per_chunk} OFFSET {offset}
        """
        
        # Process this chunk
        chunk_df = bq_client.query(chunk_query).result().to_dataframe()
        print(f"    Retrieved {len(chunk_df)} rows")
        
        # Convert to CSV
        csv_buffer = io.StringIO()
        chunk_df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue().encode('utf-8')
        
        # Upload to S3
        print(f"    Uploading {len(csv_bytes)/1024/1024:.2f} MB to S3...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=chunk_key,
            Body=csv_bytes
        )
        print(f"    Uploaded chunk {chunk+1} to {chunk_key}")

# Create an index file with information about all the uploaded files
index_content = "Date,Partition,Filename,Row Count\n"
for i, partition in enumerate(partitions):
    partition_date = partition.partition_date
    date_str = partition_date.strftime('%Y%m%d')
    
    # Query count for this partition
    count_query = f"""
    SELECT COUNT(*) as total 
    FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk`
    WHERE DATE(ts) = '{partition_date}'
    """
    total_rows = list(bq_client.query(count_query).result())[0].total
    chunks_in_partition = math.ceil(total_rows / 1000000)
    
    for chunk in range(chunks_in_partition):
        filename = f"ozone_liveramp_{date_str}_part{chunk+1}of{chunks_in_partition}.csv"
        start_row = chunk * 1000000 + 1
        end_row = min((chunk + 1) * 1000000, total_rows)
        row_count = end_row - start_row + 1
        
        index_content += f"{partition_date},{chunk+1}/{chunks_in_partition},{filename},{row_count}\n"

# Upload the index file
s3_client.put_object(
    Bucket=bucket_name,
    Key=f"{key_prefix}INDEX.csv",
    Body=index_content.encode('utf-8')
)

print("Upload process completed successfully!")