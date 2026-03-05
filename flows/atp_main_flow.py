"""
ATP Main Orchestration Flow for infotennis_v2.

End-to-end pipeline decomposed into independently deployable subflows:
  Phase 1 — Calendar ELT (scrape → S3 → MotherDuck → dbt)
  Phase 2 — Query Targets (identify tournaments needing results)
  Phase 3 — Tournament Results ELT (scrape → S3 → MotherDuck → dbt)

Each subflow can be triggered independently from the Prefect UI.
The parent orchestration flow chains all three phases.

NOTE: Heavy library imports (httpx, boto3, duckdb, etc.) are deferred into
task / flow function bodies so that cloudpickle can serialise the flows
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


# ═══════════════════════════════════════════════════════════════
# SHARED TASKS
# ═══════════════════════════════════════════════════════════════


@task(name="run_dbt_models")
def run_dbt_models_task(select: str | None = None) -> None:
    """Run dbt models with optional selector.

    Args:
        select: Optional dbt model selector string.
    """
    cmd = [
        "uv", "run", "dbt", "run",
        "--profiles-dir", ".",
        "--project-dir", ".",
    ]
    if select:
        cmd.extend(["--select", select])

    result = subprocess.run(
        cmd,
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        env={**os.environ},
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed: {result.stderr}")


@task(name="query_tournaments_to_scrape")
def query_tournaments_to_scrape_task() -> list[dict[str, Any]]:
    """Query MotherDuck for tournaments that need results scraped.

    Returns:
        List of dicts with year, tournament, tournament_id, url, change_type.
    """
    import duckdb

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = duckdb.connect(f"md:infotennis_v2_staging?motherduck_token={token}")

    rows = con.execute("""
        SELECT year, tournament, tournament_id, url, change_type
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


# ═══════════════════════════════════════════════════════════════
# SUBFLOW 1: Calendar ELT
# ═══════════════════════════════════════════════════════════════


@flow(
    name="Phase 1: Calendar ELT",
    description="Scrape ATP calendar → upload to S3 → load into MotherDuck → run dbt staging models.",
    log_prints=True,
)
def calendar_elt_flow(year: int | None = None) -> None:
    """Calendar ELT subflow: scrape, upload, load, transform.

    Args:
        year: Calendar year to process. Defaults to current year.
    """
    from tasks.ingestion.get_atp_calendar import (
        get_atp_results_archive_task,
        upload_atp_calendar_to_s3_task,
    )
    from tasks.storage.load_atp_calendar_motherduck import (
        load_atp_calendar_to_motherduck_task,
    )
    from tasks.storage.s3_storage import get_bucket_name, move_s3_file

    flow_logger = get_run_logger()

    if year is None:
        year = datetime.datetime.now().year

    bucket = get_bucket_name()

    # 1. Extract
    flow_logger.info("📥 Extracting ATP calendar data...")
    calendar_data = get_atp_results_archive_task(year=year)

    if not calendar_data or not calendar_data.get("data"):
        raise ValueError(f"Failed to retrieve calendar data for year {year}")

    flow_logger.info(
        f"✅ Scraped {len(calendar_data.get('data', []))} tournaments"
    )

    # 2. Upload to S3
    flow_logger.info("📤 Uploading calendar to S3...")
    s3_uri = upload_atp_calendar_to_s3_task(data=calendar_data, year=year)
    incoming_key = s3_uri.replace(f"s3://{bucket}/", "")

    # 3. Load to MotherDuck
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

    # 4. Transform
    flow_logger.info("🔄 Running dbt calendar models...")
    run_dbt_models_task(
        select="stg_atp_calendar_test stg_atp_calendar_changes_test"
    )

    flow_logger.info("🎉 Calendar ELT complete.")


# ═══════════════════════════════════════════════════════════════
# SUBFLOW 2: Query Targets
# ═══════════════════════════════════════════════════════════════


@flow(
    name="Phase 2: Query Targets",
    description="Query stg_atp_calendar_changes_test to identify tournaments needing results.",
    log_prints=True,
)
def query_targets_flow() -> list[dict[str, Any]]:
    """Query which tournaments need results scraped.

    Returns:
        List of target tournament dicts.
    """
    flow_logger = get_run_logger()

    targets = query_tournaments_to_scrape_task()

    if not targets:
        flow_logger.info("ℹ️ No tournaments need scraping.")
    else:
        flow_logger.info(f"🎯 Found {len(targets)} tournaments:")
        for t in targets:
            flow_logger.info(
                f"  - {t['tournament']} ({t['tournament_id']}) "
                f"[{t['change_type']}]"
            )

    return targets


# ═══════════════════════════════════════════════════════════════
# SUBFLOW 3: Tournament Results ELT
# ═══════════════════════════════════════════════════════════════


@flow(
    name="Phase 3: Tournament Results ELT",
    description=(
        "Scrape, upload, load, and transform tournament results. "
        "Accepts explicit targets for backfilling."
    ),
    log_prints=True,
)
def tournament_results_elt_flow(
    targets: list[dict[str, Any]] | None = None,
    tournament_ids: list[str] | None = None,
    year: int | None = None,
) -> None:
    """Tournament Results ELT subflow.

    Can be called from the parent orchestration flow with auto-detected
    targets, or run standalone for backfilling by providing either:
    - ``targets``: full list of target dicts (used by parent flow)
    - ``tournament_ids`` + ``year``: for manual backfill from the UI
      (looks up URLs from the calendar staging table)

    Args:
        targets: Pre-built target list from Phase 2 (used by parent flow).
        tournament_ids: List of tournament IDs for manual backfill.
        year: Year for manual backfill. Defaults to current year.
    """
    from tasks.ingestion.get_atp_tournament_results import (
        get_atp_tournament_results_task,
        upload_atp_tournament_results_to_s3_task,
    )
    from tasks.storage.load_atp_tournament_results_motherduck import (
        load_atp_tournament_results_to_motherduck_task,
    )
    from tasks.storage.s3_storage import get_bucket_name

    flow_logger = get_run_logger()

    if year is None:
        year = datetime.datetime.now().year

    bucket = get_bucket_name()

    # -----------------------------------------------------------
    # Resolve targets
    # -----------------------------------------------------------
    if targets is None and tournament_ids is not None:
        # Manual backfill mode: look up URLs from MotherDuck
        flow_logger.info(
            f"🔍 Backfill mode: looking up {len(tournament_ids)} tournament(s)..."
        )
        targets = _resolve_backfill_targets(tournament_ids, year)
    elif targets is None:
        # No targets at all — query from staging
        flow_logger.info("🔍 No targets supplied. Querying staging view...")
        targets = query_tournaments_to_scrape_task()

    if not targets:
        flow_logger.info("ℹ️ No tournaments to process.")
        return

    flow_logger.info(f"📋 Processing {len(targets)} tournament(s):")
    for t in targets:
        flow_logger.info(f"  - {t['tournament']} ({t['tournament_id']})")

    # -----------------------------------------------------------
    # Scrape & upload
    # -----------------------------------------------------------
    scraped_tournaments: list[dict[str, Any]] = []

    for t in targets:
        tourn_name = t["tournament"]
        tourn_id = t["tournament_id"]
        tourn_url = t["url"]
        tourn_year = t.get("year", year)

        flow_logger.info(f"\n📥 Scraping: {tourn_name} ({tourn_id})...")

        try:
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

            time.sleep(2)

        except Exception as e:
            flow_logger.error(
                f"❌ Failed to scrape {tourn_name} ({tourn_id}): {e}"
            )
            continue

    if not scraped_tournaments:
        flow_logger.info("ℹ️ No tournament results were scraped. Skipping load.")
        return

    # -----------------------------------------------------------
    # Load to MotherDuck
    # -----------------------------------------------------------
    flow_logger.info(
        f"\n🦆 Loading {len(scraped_tournaments)} tournament(s) to MotherDuck..."
    )
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

    # -----------------------------------------------------------
    # Transform
    # -----------------------------------------------------------
    flow_logger.info("🔄 Running dbt tournament results models...")
    run_dbt_models_task(
        select="stg_atp_tournament_results stg_atp_tournament_results_new"
    )

    total_matches = sum(t["matches"] for t in scraped_tournaments)
    flow_logger.info(
        f"🎉 Tournament Results ELT complete. "
        f"{len(scraped_tournaments)} tournaments, {total_matches} matches."
    )


def _resolve_backfill_targets(
    tournament_ids: list[str], year: int
) -> list[dict[str, Any]]:
    """Look up tournament URLs from the calendar staging table for backfill.

    Args:
        tournament_ids: List of ATP tournament IDs to backfill.
        year: Calendar year.

    Returns:
        List of target dicts suitable for the results ELT flow.
    """
    import duckdb

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = duckdb.connect(f"md:infotennis_v2_staging?motherduck_token={token}")

    placeholders = ", ".join(f"'{tid}'" for tid in tournament_ids)
    rows = con.execute(f"""
        SELECT DISTINCT year, tournament, tournament_id, url
        FROM dev.stg_atp_calendar_test
        WHERE tournament_id IN ({placeholders})
          AND year = {year}
          AND url IS NOT NULL
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
            "change_type": "backfill",
        })

    return targets


# ═══════════════════════════════════════════════════════════════
# PARENT ORCHESTRATION FLOW
# ═══════════════════════════════════════════════════════════════


@flow(
    name="ATP Main Orchestration Pipeline",
    description=(
        "End-to-end pipeline: Calendar ELT → Determine targets → "
        "Tournament Results ELT. Calls each phase as a subflow."
    ),
    retries=0,
    log_prints=True,
)
def atp_main_orchestration_flow(year: int | None = None) -> None:
    """Main orchestration flow chaining all three phases as subflows.

    Args:
        year: Calendar year to process. Defaults to current year.
    """
    flow_logger = get_run_logger()

    if year is None:
        year = datetime.datetime.now().year

    flow_logger.info(f"{'='*60}")
    flow_logger.info(f"ATP Main Orchestration — Year {year}")
    flow_logger.info(f"{'='*60}")

    # Phase 1: Calendar ELT (subflow)
    calendar_elt_flow(year=year)

    # Phase 2: Query Targets (subflow)
    targets = query_targets_flow()

    if not targets:
        flow_logger.info("ℹ️ No tournaments need scraping. Pipeline complete.")
        return

    # Phase 3: Tournament Results ELT (subflow)
    tournament_results_elt_flow(targets=targets, year=year)

    flow_logger.info(f"\n{'='*60}")
    flow_logger.info("🎉 ATP Main Pipeline Complete!")
    flow_logger.info(f"{'='*60}")


# ═══════════════════════════════════════════════════════════════
# SERVE ALL DEPLOYMENTS
# ═══════════════════════════════════════════════════════════════


if __name__ == "__main__":
    from prefect import serve

    main_deploy = atp_main_orchestration_flow.to_deployment(
        name="atp-main-orchestration",
    )
    calendar_deploy = calendar_elt_flow.to_deployment(
        name="atp-calendar-elt",
    )
    targets_deploy = query_targets_flow.to_deployment(
        name="atp-query-targets",
    )
    results_deploy = tournament_results_elt_flow.to_deployment(
        name="atp-tournament-results-elt",
    )

    serve(main_deploy, calendar_deploy, targets_deploy, results_deploy)
