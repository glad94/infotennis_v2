"""
ATP Main Flow for infotennis_v2.

Orchestrates the complete ELT pipeline:
1. Scrape ATP Results Archive
2. Upload to S3
3. Load into MotherDuck
"""
import logging
import os
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from prefect import flow, get_run_logger

# Load .env file for local development
# In Prefect Cloud, set env vars in the work pool or deployment
load_dotenv()

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.ingestion.get_atp_calendar import get_atp_results_archive_task
from tasks.storage.s3_storage import upload_to_s3
from tasks.storage.motherduck_load import load_to_motherduck

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s - %(message)s",
)


@flow(
    name="ATP Results Archive Pipeline",
    description="Monthly ETL to scrape ATP results and load into MotherDuck",
    retries=1,
    retry_delay_seconds=30,
    log_prints=True
)
def atp_results_archive_flow(year: Optional[int] = None):
    """
    Complete ELT flow for ATP Results Archive.
    """
    logger = get_run_logger()
    
    if year is None:
        year = datetime.now().year
    
    logger.info(f"{'='*60}")
    logger.info(f"ATP Results Archive Pipeline - Year {year}")
    logger.info(f"{'='*60}")
    
    # Step 1: Scrape
    logger.info("\n📥 Step 1: Scraping ATP Results Archive...")
    tournaments = get_atp_results_archive_task(year=year)
    
    if not tournaments:
        logger.warning("No tournament data scraped")
        return {"status": "no_data", "year": year}
    
    print(f"✅ Scraped {len(tournaments)} tournaments")
    
    # Step 2: Upload to S3
    print("\n📤 Step 2: Uploading to S3...")
    payload = {
        "metadata": {
            "year": year,
            "scraped_at": datetime.now().isoformat(),
            "record_count": len(tournaments)
        },
        "tournaments": tournaments
    }
    s3_uri = upload_to_s3(data=payload, endpoint_name="atp_results_archive")
    
    # # Step 3: Load to MotherDuck
    # print("\n🦆 Step 3: Loading to MotherDuck...")
    # rows_loaded = load_to_motherduck(s3_uri=s3_uri)
    
    # # Summary
    # print(f"\n{'='*60}")
    # print("✅ Pipeline Complete!")
    # print(f"   Year: {year}")
    # print(f"   Tournaments: {len(tournaments)}")
    # print(f"   S3 URI: {s3_uri}")
    # print(f"   Rows loaded: {rows_loaded}")
    # print(f"{'='*60}")
    
    # return {
    #     "status": "success",
    #     "year": year,
    #     "tournaments_scraped": len(tournaments),
    #     "s3_uri": s3_uri,
    #     "rows_loaded": rows_loaded
    # }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run ATP Results Archive Pipeline")
    parser.add_argument("--year", type=int, default=None, help="Year to scrape")
    args = parser.parse_args()
    
    result = atp_results_archive_flow(year=args.year)
    print("\nResult:", result)
