"""
AWS S3 Storage Task for infotennis_v2.

Handles uploading JSON data to S3 with partitioned naming convention:
s3://{bucket}/raw/{endpoint_name}/year={YYYY}/month={MM}/{timestamp}.json
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from prefect import task

logger = logging.getLogger(__name__)


def get_s3_client():
    """Create and return an S3 client. Uses AWS env vars automatically."""
    return boto3.client("s3")


def get_bucket_name() -> str:
    """Get S3 bucket name from env var or use default."""
    return os.getenv("S3_BUCKET", "infotennis-v2")


def generate_s3_key(endpoint_name: str, timestamp: datetime = None) -> str:
    """
    Generate S3 key with partitioned naming convention.
    
    Format: raw/{endpoint_name}/year={YYYY}/month={MM}/{timestamp}.json
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
    
    return f"raw/{endpoint_name}/year={year}/month={month}/{ts_str}.json"


@task(
    name="upload_to_s3",
    description="Upload JSON data to S3 with partitioned naming",
    retries=3,
    retry_delay_seconds=5,
    log_prints=True
)
def upload_to_s3(data: dict, endpoint_name: str) -> str:
    """
    Upload JSON data to S3 with partitioned naming convention.
    
    Args:
        data: JSON-compatible Python dict to upload
        endpoint_name: Name of the endpoint (e.g., 'atp_results_archive')
        
    Returns:
        S3 URI of the uploaded file (s3://{bucket}/{key})
    """
    timestamp = datetime.now(timezone.utc)
    s3_key = generate_s3_key(endpoint_name, timestamp)
    bucket = get_bucket_name()
    
    # Serialize data to JSON
    json_data = json.dumps(data, indent=2, default=str, ensure_ascii=False)
    
    # Upload to S3
    s3_client = get_s3_client()
    
    print(f"Uploading to s3://{bucket}/{s3_key}")
    
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "endpoint": endpoint_name,
            "upload_timestamp": timestamp.isoformat(),
        }
    )
    
    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"✅ Successfully uploaded to {s3_uri}")
    
    return s3_uri
