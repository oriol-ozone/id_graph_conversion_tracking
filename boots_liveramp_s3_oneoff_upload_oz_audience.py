from google.cloud import bigquery
import boto3
import pandas as pd
import os
import io
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
key = '1777/2/ozone_liveramp_ad_requests_last7_20250416.csv'

# Set up multipart upload
multipart_upload = s3_client.create_multipart_upload(
    Bucket=bucket_name,
    Key=key
)

upload_id = multipart_upload['UploadId']
parts = []
part_number = 1

# First, get a list of partition dates
partition_query = """
SELECT DISTINCT DATE(ts) as partition_date
FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk`
ORDER BY partition_date
"""

partitions = list(bq_client.query(partition_query).result())
print(f"Found {len(partitions)} date partitions to process")

try:
    # Process each date partition
    is_first_chunk = True
    
    for partition in partitions:
        partition_date = partition.partition_date
        print(f"Processing partition date: {partition_date}")
        
        # Get count for this partition
        count_query = f"""
        SELECT COUNT(*) as total 
        FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk`
        WHERE DATE(ts) = '{partition_date}'
        """
        
        partition_total = list(bq_client.query(count_query).result())[0].total
        print(f"  Total rows for date {partition_date}: {partition_total}")
        
        # Process this partition in chunks
        rows_per_chunk = 100000
        chunks_in_partition = math.ceil(partition_total / rows_per_chunk)
        
        for chunk in range(chunks_in_partition):
            offset = chunk * rows_per_chunk
            print(f"  Processing chunk {chunk+1}/{chunks_in_partition} (offset {offset})")
            
            # Query this partition chunk
            chunk_query = f"""
            SELECT ts, id_type, liveramp_id
            FROM `analytics.046_boots_liveramp_ids_last7_oz_audience_uk`
            WHERE DATE(ts) = '{partition_date}'
            LIMIT {rows_per_chunk} OFFSET {offset}
            """
            
            # Process this chunk
            chunk_df = bq_client.query(chunk_query).result().to_dataframe()
            
            # Convert to CSV in memory
            csv_buffer = io.StringIO()
            chunk_df.to_csv(
                csv_buffer, 
                index=False, 
                header=is_first_chunk  # Include header only in first chunk
            )
            is_first_chunk = False
            
            # Convert to bytes for S3
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            
            # Upload this part if it has data
            if len(csv_bytes) > 0:
                part = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=csv_bytes
                )
                
                # Keep track of the parts
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })
                
                part_number += 1
                print(f"  Chunk {chunk+1} uploaded")
    
    # Complete the multipart upload
    s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )
    print("Upload complete!")

except Exception as e:
    # Abort the multipart upload if something goes wrong
    s3_client.abort_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id
    )
    print(f"Upload failed: {e}")
    raise