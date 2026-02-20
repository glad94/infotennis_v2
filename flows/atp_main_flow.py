"""
ATP Main Orchestration Flow for infotennis_v2.

End-to-end pipeline that:
1. Ingests, uploads, and transforms ATP calendar data
2. Determines which tournaments have new/in-progress results
3. Scrapes, uploads, and transforms tournament results for those targets
4. Refreshes the staging layer for downstream consumption

NOTE: Heavy library imports (httpx, boto3, duckdb, etc.) are deferred into
task / flow function bodies so that cloudpickle can serialise the flow
without hitting unpicklable _thread._local objects.
"""
from __future__ import annotations

import datetime
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load credentials
load_dotenv()

logger = logging.getLogger(__name__)

DBT_DIR = Path(__file__).parent.parent / "dbt"


@task(name="run_dbt_models")
def run_dbt_models_task(select: str | None = None) -> None:
    """Run dbt models with optional selector.

    Args:
        select: Optional dbt model selector string (e.g. '+stg_atp_calendar_changes_test').
    """
    cmd = [
        "uv", "run", "dbt", "run",
        "--profiles-dir", ".",
        "--project-dir", ".",
    ]
    if select:
        cmd.extend(["--select", select])

    env = {**os.environ}
    result = subprocess.run(
        cmd,
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        env=env,
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed: {result.stderr}")


@task(name="query_tournaments_to_scrape")
def query_tournaments_to_scrape_task() -> list[dict[str, Any]]:
    """Query MotherDuck for tournaments that need results scraped.

    Reads from the stg_atp_calendar_changes_test view in the staging
    database to get tournaments with newly completed or ongoing results.

    Returns:
        List of dicts with tournament_id, url, year, tournament name, and change_type.
    """
    import duckdb

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = duckdb.connect(f"md:infotennis_v2_staging?motherduck_token={token}")

    rows = con.execute("""
        SELECT
            year,
            tournament,
            tournament_id,
            url,
            change_type
        FROM dev.stg_atp_calendar_changes_test
    """).fetchall()

    con.close()

    atp_root = "https://www.atptour.com"
    targets = []
    for row in rows:
        url = row[3]
        if url and not url.startswith("http"):
            url = atp_root + url
        targets.append({
            "year": row[0],
            "tournament": row[1],
            "tournament_id": row[2],
            "url": url,
            "change_type": row[4],
        })

    return targets


@flow(
    name="ATP Main Orchestration Pipeline",
    description=(
        "End-to-end pipeline: Calendar ELT -> Determine targets -> "
        "Tournament Results ELT -> Staging refresh"
    ),
    retries=0,
    log_prints=True,
)
def atp_main_orchestration_flow(year: int | None = None) -> None:
    """Main orchestration flow for ATP data pipeline.

    Args:
        year: Calendar year to process. Defaults to current year.
    """
    # Deferred imports — keeps cloudpickle happy
    from tasks.ingestion.get_atp_calendar import (
        get_atp_results_archive_task,
        upload_atp_calendar_to_s3_task,
    )
    from tasks.ingestion.get_atp_tournament_results import (
        get_atp_tournament_results_task,
        upload_atp_tournament_results_to_s3_task,
    )
    from tasks.storage.load_atp_calendar_motherduck import (
        load_atp_calendar_to_motherduck_task,
    )
    from tasks.storage.load_atp_tournament_results_motherduck import (
        load_atp_tournament_results_to_motherduck_task,
    )
    from tasks.storage.s3_storage import get_bucket_name, move_s3_file

    flow_logger = get_run_logger()

    if year is None:
        year = datetime.datetime.now().year

    bucket = get_bucket_name()

    # ═══════════════════════════════════════════════════════════
    # PHASE 1: Calendar ELT
    # ═══════════════════════════════════════════════════════════
    flow_logger.info(f"{'='*60}")
    flow_logger.info("PHASE 1: ATP Calendar ELT")
    flow_logger.info(f"{'='*60}")

    # 1a. Extract
    flow_logger.info("📥 Extracting ATP calendar data...")
    calendar_data = get_atp_results_archive_task(year=year)

    if not calendar_data or not calendar_data.get("data"):
        raise ValueError(f"Failed to retrieve calendar data for year {year}")

    flow_logger.info(
        f"✅ Scraped {len(calendar_data.get('data', []))} tournaments"
    )

    # 1b. Upload to S3
    flow_logger.info("📤 Uploading calendar to S3...")
    s3_uri = upload_atp_calendar_to_s3_task(data=calendar_data, year=year)
    incoming_key = s3_uri.replace(f"s3://{bucket}/", "")

    # 1c. Load to MotherDuck
    flow_logger.info("🦆 Loading calendar into MotherDuck...")
    pattern = f"raw/atp_results_archive/incoming/year={year}"
    try:
        load_atp_calendar_to_motherduck_task(
            bucket=bucket, pattern=pattern, database=None
        )
        loaded_key = incoming_key.replace("/incoming/", "/loaded/")
        move_s3_file(bucket, incoming_key, loaded_key)
        flow_logger.info("✅ Calendar loaded and file moved to loaded/")
    except Exception as e:
        flow_logger.error(f"❌ Calendar load failed: {e}")
        raise

    # 1d. Transform: run dbt calendar staging models
    flow_logger.info("🔄 Running dbt calendar models...")
    run_dbt_models_task(
        select="stg_atp_calendar_test stg_atp_calendar_changes_test"
    )

    # ═══════════════════════════════════════════════════════════
    # PHASE 2: Determine Tournament Targets
    # ═══════════════════════════════════════════════════════════
    flow_logger.info(f"\n{'='*60}")
    flow_logger.info("PHASE 2: Determining tournaments to scrape")
    flow_logger.info(f"{'='*60}")

    targets = query_tournaments_to_scrape_task()

    if not targets:
        flow_logger.info("ℹ️ No tournaments need scraping. Pipeline complete.")
        return

    flow_logger.info(f"🎯 Found {len(targets)} tournaments to scrape:")
    for t in targets:
        flow_logger.info(
            f"  - {t['tournament']} ({t['tournament_id']}) "
            f"[{t['change_type']}]"
        )

    # ═══════════════════════════════════════════════════════════
    # PHASE 3: Tournament Results ELT
    # ═══════════════════════════════════════════════════════════
    flow_logger.info(f"\n{'='*60}")
    flow_logger.info("PHASE 3: Tournament Results ELT")
    flow_logger.info(f"{'='*60}")

    scraped_tournaments = []

    for t in targets:
        tourn_name = t["tournament"]
        tourn_id = t["tournament_id"]
        tourn_url = t["url"]
        tourn_year = t["year"]

        flow_logger.info(f"\n📥 Scraping: {tourn_name} ({tourn_id})...")

        try:
            # 3a. Scrape tournament results
            results = get_atp_tournament_results_task(
                url=tourn_url,
                tournament_name=tourn_name,
                tournament_id=tourn_id,
                year=tourn_year,
            )

            if not results or not results.get("data"):
                flow_logger.warning(
                    f"⚠️ No match data for {tourn_name}. Skipping."
                )
                continue

            match_count = len(results.get("data", []))
            flow_logger.info(f"  ✅ Found {match_count} matches")

            # 3b. Upload to S3
            s3_uri = upload_atp_tournament_results_to_s3_task(
                data=results,
                tournament_id=tourn_id,
                year=tourn_year,
            )
            flow_logger.info(f"  📤 Uploaded to {s3_uri}")

            scraped_tournaments.append({
                "tournament_id": tourn_id,
                "tournament": tourn_name,
                "year": tourn_year,
                "matches": match_count,
            })

            # Brief pause between scrapes to be respectful
            time.sleep(2)

        except Exception as e:
            flow_logger.error(
                f"❌ Failed to scrape {tourn_name} ({tourn_id}): {e}"
            )
            continue

    if not scraped_tournaments:
        flow_logger.info("ℹ️ No tournament results were scraped. Skipping load.")
        return

    # 3c. Load all tournament results to MotherDuck
    flow_logger.info(f"\n🦆 Loading {len(scraped_tournaments)} tournaments to MotherDuck...")
    for t in scraped_tournaments:
        tourn_id = t["tournament_id"]
        tourn_year = t["year"]
        pattern = f"raw/atp_tournament/year={tourn_year}/tourn={tourn_id}"

        try:
            load_atp_tournament_results_to_motherduck_task(
                bucket=bucket, pattern=pattern, database=None
            )
            flow_logger.info(f"  ✅ Loaded {t['tournament']} ({tourn_id})")
        except Exception as e:
            flow_logger.error(f"  ❌ Failed to load {t['tournament']}: {e}")

    # 3d. Transform: run dbt tournament results models
    flow_logger.info("🔄 Running dbt tournament results models...")
    run_dbt_models_task(
        select="stg_atp_tournament_results stg_atp_tournament_results_new"
    )

    # ═══════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════
    flow_logger.info(f"\n{'='*60}")
    flow_logger.info("🎉 ATP Main Pipeline Complete!")
    flow_logger.info(f"   Year: {year}")
    flow_logger.info(
        f"   Tournaments scraped: {len(scraped_tournaments)}"
    )
    total_matches = sum(t["matches"] for t in scraped_tournaments)
    flow_logger.info(f"   Total matches: {total_matches}")
    flow_logger.info(f"{'='*60}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run ATP Main Orchestration Pipeline"
    )
    parser.add_argument(
        "--year", type=int, default=None, help="Year to process"
    )
    args = parser.parse_args()

    # atp_main_orchestration_flow(year=args.year)
    atp_main_orchestration_flow.serve(name="atp-main-orchestration")
