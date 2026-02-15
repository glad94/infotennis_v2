from prefect import task
import duckdb
import os
import boto3
import json

@task
def load_to_motherduck_task(s3_uri, s3_meta):
    # Download file from S3
    bucket = s3_uri.split('/')[2]
    key = '/'.join(s3_uri.split('/')[3:])
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION'))
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_json = obj['Body'].read().decode('utf-8')
    # Connect to MotherDuck
    con = duckdb.connect(f"md:{os.getenv('MOTHERDUCK_DATABASE')}?token={os.getenv('MOTHERDUCK_TOKEN')}")
    # Create table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_atp_calendar (
            data VARIANT,
            meta_file_name VARCHAR,
            meta_file_modified TIMESTAMP,
            meta_file_size BIGINT,
            meta_s3_uri VARCHAR,
            meta_ingest_time TIMESTAMP,
            scrape_time TIMESTAMP,
            endpoint VARCHAR
        )
    """)
    # Insert row
    con.execute("""
        INSERT INTO raw_atp_calendar VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [json.loads(raw_json),
          s3_meta['meta_file_name'],
          s3_meta['meta_file_modified'],
          s3_meta['meta_file_size'],
          s3_meta['meta_s3_uri'],
          s3_meta['meta_ingest_time'],
          s3_meta['scrape_time'],
          s3_meta['endpoint']])
    return {'status': 'success', 'row_count': 1}
