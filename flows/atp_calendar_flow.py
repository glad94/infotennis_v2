"""
ATP Calendar ELT Flow for infotennis_v2.

Orchestrates:
1. Extraction (JSON)
2. Staging to S3 (incoming/)
3. Loading to MotherDuck (schema-on-read)
4. S3 File Management (Success -> loaded/, Failure -> Fail)
"""
import datetime
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from prefect import flow, get_run_logger

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.ingestion.get_atp_calendar import get_atp_results_archive_task, upload_atp_calendar_to_s3_task
from tasks.storage.load_atp_calendar_motherduck import load_atp_calendar_to_motherduck_task
from tasks.storage.s3_storage import get_bucket_name, move_s3_file

# Load credentials
load_dotenv()

@flow(
    name="ATP Calendar ELT Pipeline",
    description="Extracts ATP calendar, stages in S3, and loads to MotherDuck with state management.",
    retries=1,
    retry_delay_seconds=60,
    log_prints=True
)
def atp_calendar_elt_flow(year: int | None = None):
    """
    Complete ELT flow for ATP Calendar.
    """
    logger = get_run_logger()
    
    # 1. Determine Current Year
    if year is None:
        year = datetime.datetime.now().year
    
    logger.info(f"Starting ATP Calendar ELT for Year: {year}")
    
    # 2. Extract Data
    logger.info("📥 Step 1: Extracting latest ATP calendar data...")
    results = get_atp_results_archive_task(year=year)
    
    if not results or not results.get("data"):
        logger.error("No data retrieved from ATP archive.")
        raise ValueError(f"Failed to retrieve data for year {year}")
    
    # 3. Stage to S3 (Incoming)
    logger.info("📤 Step 2: Staging data to S3 (incoming)...")
    s3_uri = upload_atp_calendar_to_s3_task(results, year)
    
    # Extract bucket and key for manual management
    bucket = get_bucket_name()
    incoming_key = s3_uri.replace(f"s3://{bucket}/", "")
    
    # 4. Load to MotherDuck
    logger.info("🦆 Step 3: Loading data into MotherDuck...")
    try:
        from tasks.storage.motherduck_load import get_config
        config = get_config()
        # Derive pattern from config path template
        # Path: "raw/atp_results_archive/incoming/year=%(year)s/atp_calendar_%(year)s_%(timestamp)s.json"
        # We need the prefix for the whole year folder
        pattern = f"raw/atp_results_archive/incoming/year={year}"
        
        load_atp_calendar_to_motherduck_task(
            bucket=bucket, 
            pattern=pattern,
            database=None  # Uses default from env
        )
        
        # 5. Post-Load S3 File Management (Success)
        logger.info("✅ Load successful! Moving file to 'loaded/' prefix...")
        loaded_key = incoming_key.replace("/incoming/", "/loaded/")
        move_s3_file(bucket, incoming_key, loaded_key)
        
    except Exception as e:
        # 5. Post-Load S3 File Management (Failure)
        logger.error(f"❌ MotherDuck load failed: {e}")
        logger.warning(f"File remains in 'incoming/': {s3_uri}")
        # Re-raise so Prefect marks the flow as failed
        raise

    logger.info("🎉 ATP Calendar ELT Pipeline Completed Successfully!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run ATP Calendar ELT Flow")
    parser.add_argument("--year", type=int, default=None, help="Year to process")
    args = parser.parse_args()
    
    atp_calendar_elt_flow(year=args.year)
