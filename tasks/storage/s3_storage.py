"""
AWS S3 Storage Task for infotennis_v2.

Handles uploading JSON data to S3 with partitioned naming convention:
s3://{bucket}/raw/{endpoint_name}/year={YYYY}/month={MM}/{timestamp}.json
"""
import json
import logging
import os
from datetime import datetime, timezone

from prefect import task

import yaml

logger = logging.getLogger(__name__)

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_s3_client():
    """Create and return an S3 client. Uses AWS env vars automatically."""
    import boto3
    return boto3.client("s3")


def get_bucket_name() -> str:
    """Get S3 bucket name from env var or use default."""
    config = get_config()
    bucket = os.getenv("S3_BUCKET", config['s3']['default_bucket'])
    return bucket.replace("s3://", "").strip("/")


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


def upload_json_to_s3(data: dict, bucket: str, key: str, metadata: dict = None) -> str:
    """
    Utility function to upload JSON data to S3.
    
    Args:
        data: JSON-compatible Python dict to upload
        bucket: S3 bucket name
        key: S3 key (path)
        metadata: Optional metadata for the S3 object
        
    Returns:
        S3 URI of the uploaded file
    """
    timestamp = datetime.now(timezone.utc)
    s3_client = get_s3_client()
    
    # Serialize data to JSON
    json_data = json.dumps(data, indent=2, default=str, ensure_ascii=False)
    
    # Prepare metadata
    s3_metadata = metadata or {}
    if "upload_timestamp" not in s3_metadata:
        s3_metadata["upload_timestamp"] = timestamp.isoformat()
    
    print(f"Uploading to s3://{bucket}/{key}")
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json",
        Metadata=s3_metadata
    )
    
    s3_uri = f"s3://{bucket}/{key}"
    print(f"Successfully uploaded to {s3_uri}")
    
    return s3_uri


def move_s3_file(bucket: str, source_key: str, dest_key: str) -> None:
    """
    Move a file in S3 by copying it to the destination and deleting the source.
    """
    s3_client = get_s3_client()
    copy_source = {"Bucket": bucket, "Key": source_key}
    
    try:
        print(f"Moving s3://{bucket}/{source_key} to s3://{bucket}/{dest_key}")
        # Copy the object
        s3_client.copy(copy_source, bucket, dest_key)
        # Delete the original object
        s3_client.delete_object(Bucket=bucket, Key=source_key)
        print(f"Successfully moved to s3://{bucket}/{dest_key}")
    except Exception as e:
        logger.error(f"Failed to move s3://{bucket}/{source_key}: {e}")
        raise
