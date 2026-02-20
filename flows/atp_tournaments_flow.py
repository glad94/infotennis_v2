"""
ATP Tournaments ELT Flow for infotennis_v2.

Orchestrates:
1. Extraction (JSON API)
2. Staging to S3 (incoming/)
3. Loading to MotherDuck (schema-on-read)
4. S3 File Management (Success -> loaded/, Failure -> Fail)
"""
import datetime
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, get_run_logger

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.ingestion.get_atp_tournaments import (
    get_atp_tournaments_task,
    upload_atp_tournaments_to_s3_task,
)
from tasks.storage.load_atp_tournaments_motherduck import (
    load_atp_tournaments_to_motherduck_task,
)
from tasks.storage.s3_storage import get_bucket_name, move_s3_file

# Load credentials
load_dotenv()


@flow(
    name="ATP Tournaments ELT Pipeline",
    description="Fetches ATP tournament details, stages in S3, and loads to MotherDuck.",
    retries=1,
    retry_delay_seconds=60,
    log_prints=True,
)
def atp_tournaments_elt_flow() -> None:
    """Complete ELT flow for ATP Tournament details."""
    logger = get_run_logger()
    year = datetime.datetime.now().year

    logger.info(f"Starting ATP Tournaments ELT for Year: {year}")

    # 1. Extract Data
    logger.info("📥 Step 1: Fetching ATP tournament data...")
    results = get_atp_tournaments_task()

    if not results or not results.get("data"):
        logger.error("No data retrieved from ATP tournaments API.")
        raise ValueError("Failed to retrieve ATP tournament data")

    # 2. Stage to S3 (Incoming)
    logger.info("📤 Step 2: Staging data to S3 (incoming)...")
    s3_uri = upload_atp_tournaments_to_s3_task(results)

    bucket = get_bucket_name()
    incoming_key = s3_uri.replace(f"s3://{bucket}/", "")

    # 3. Load to MotherDuck
    logger.info("🦆 Step 3: Loading data into MotherDuck...")
    try:
        pattern = f"raw/atp_tournaments/incoming/year={year}"

        load_atp_tournaments_to_motherduck_task(
            bucket=bucket,
            pattern=pattern,
            database=None,
        )

        # 4. Post-Load S3 File Management (Success)
        logger.info("✅ Load successful! Moving file to 'loaded/' prefix...")
        loaded_key = incoming_key.replace("/incoming/", "/loaded/")
        move_s3_file(bucket, incoming_key, loaded_key)

    except Exception as e:
        logger.error(f"❌ MotherDuck load failed: {e}")
        logger.warning(f"File remains in 'incoming/': {s3_uri}")
        raise

    logger.info("🎉 ATP Tournaments ELT Pipeline Completed Successfully!")


if __name__ == "__main__":
    atp_tournaments_elt_flow()
