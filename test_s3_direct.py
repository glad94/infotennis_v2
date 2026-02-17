import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import json

from dotenv import load_dotenv

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent))
load_dotenv()

from tasks.storage.s3_storage import upload_json_to_s3, get_bucket_name

def test_s3_direct():
    print("Testing S3 upload directly...")
    try:
        bucket = get_bucket_name()
        print(f"Bucket name retrieved: '{bucket}'")
        
        data = {"test": "data"}
        key = "raw/test-connection.json"
        
        metadata = {"test": "metadata"}
        
        uri = upload_json_to_s3(data, bucket, key, metadata)
        print(f"Upload successful: {uri}")
    except Exception as e:
        print(f"Upload failed with error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_s3_direct()
