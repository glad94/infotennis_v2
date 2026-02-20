"""
ATP Tournaments Ingestion Task for infotennis_v2.

Fetches tournament detail data (surfaces, financial commitments, draw sizes, etc.)
from the ATP Tour calendar API and uploads to S3.
"""
import sys
from pathlib import Path

# Add project root to sys.path for standalone execution
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import argparse
import datetime
import logging
import os
import random
from typing import Any

import httpx
import yaml
from dotenv import load_dotenv
from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential

from tasks.storage.s3_storage import get_bucket_name, upload_json_to_s3

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

API_URL = "https://www.atptour.com/en/-/tournaments/calendar/tour"


def get_config() -> dict[str, Any]:
    """Load project configuration from config.yaml.

    Returns:
        Parsed YAML configuration dictionary.
    """
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


@task(name="get_atp_tournaments")
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_atp_tournaments_task() -> dict[str, Any]:
    """Fetch ATP tournament calendar data from the ATP Tour API.

    The API returns JSON with tournament details including surfaces,
    financial commitments, draw sizes, and scheduling information
    for the current year.

    Returns:
        Dictionary with 'metadata' and 'data' keys wrapping the API response.
    """
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    try:
        with httpx.Client(timeout=20, follow_redirects=True) as client:
            print(f"Fetching ATP tournament data from: {API_URL}")
            resp = client.get(API_URL, headers=headers)
            resp.raise_for_status()
            api_data = resp.json()
    except Exception as e:
        logger.error(f"Request failed for {API_URL}: {e}")
        raise

    retrieved_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return {
        "metadata": {
            "retrieved_at": retrieved_at,
            "source_url": API_URL,
        },
        "data": api_data,
    }


@task(name="upload_atp_tournaments_to_s3")
def upload_atp_tournaments_to_s3_task(data: dict[str, Any]) -> str:
    """Upload ATP tournament data to S3 staging (incoming/).

    Args:
        data: Tournament data dict with metadata wrapper.

    Returns:
        S3 URI of the uploaded file.
    """
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
    year = timestamp.year

    config = get_config()
    bucket = os.getenv("S3_BUCKET", config['s3']['default_bucket'])
    key_template = config['s3']['paths']['atp_tournaments']
    key = key_template % {'year': year, 'timestamp': ts_str}

    metadata = {
        "endpoint": "atp_tournaments",
        "year": str(year),
        "scraped_at": data.get("metadata", {}).get(
            "retrieved_at", timestamp.isoformat()
        ),
    }

    return upload_json_to_s3(data, bucket, key, metadata)


if __name__ == "__main__":
    load_dotenv()
    print("Fetching ATP tournament data...")
    results = get_atp_tournaments_task()

    if results and results.get("data"):
        s3_uri = upload_atp_tournaments_to_s3_task(results)
        print(f"Upload complete: {s3_uri}")
    else:
        print("No tournament data found.")
