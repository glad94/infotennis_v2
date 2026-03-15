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


def _resolve_matches_for_tournaments(
    tournament_ids: list[str], year: int
) -> list[dict[str, Any]]:
    """Look up all matches for given tournaments from the staging table.

    Args:
        tournament_ids: List of ATP tournament IDs.
        year: Calendar year.

    Returns:
        List of match dicts suitable for match_data_elt_flow.
    """
    import duckdb

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = duckdb.connect(f"md:infotennis_v2_staging?motherduck_token={token}")

    placeholders = ", ".join(f"'{tid}'" for tid in tournament_ids)
    rows = con.execute(f"""
        SELECT
            year,
            tournament_id,
            match_id,
            round,
            tournament_name
        FROM dev.stg_atp_tournament_results
        WHERE tournament_id IN ({placeholders})
          AND year = {year}
          AND match_id IS NOT NULL
          AND match_id != ''
    """).fetchall()

    con.close()

    matches = []
    for row in rows:
        matches.append({
            "year": row[0],
            "tournament_id": row[1],
            "match_id": row[2],
            "round": row[3],
            "tournament_name": row[4],
        })

    return matches


# ═══════════════════════════════════════════════════════════════
# SUBFLOW 4: Match-Level Data ELT
# ═══════════════════════════════════════════════════════════════

MATCH_DATA_TYPES = [
    "match-info",
    "key-stats",
    "stroke-analysis",
    "rally-analysis",
    "court-vision",
]


@task(name="query_new_matches")
def query_new_matches_task() -> list[dict[str, Any]]:
    """Query MotherDuck for newly added matches from stg_atp_tournament_results_new.

    Returns:
        List of dicts with year, tournament_id, match_id, round,
        player info, and url for match data retrieval.
    """
    import duckdb

    token = os.environ["MOTHERDUCK_TOKEN"]
    con = duckdb.connect(f"md:infotennis_v2_staging?motherduck_token={token}")

    rows = con.execute("""
        SELECT
            year,
            tournament_id,
            match_id,
            round,
            tournament_name,
            url
        FROM dev.stg_atp_tournament_results_new
        WHERE match_id IS NOT NULL
          AND match_id != ''
    """).fetchall()

    con.close()

    matches = []
    for row in rows:
        matches.append({
            "year": row[0],
            "tournament_id": row[1],
            "match_id": row[2],
            "round": row[3],
            "tournament_name": row[4],
            "url": row[5],
        })

    return matches


@flow(
    name="Phase 4: Match Data ELT",
    description=(
        "Fetch match-level data (match-info, key-stats, stroke-analysis, "
        "rally-analysis, court-vision) for new matches, upload to S3, "
        "load to MotherDuck, and run dbt staging models."
    ),
    log_prints=True,
)
def match_data_elt_flow(
    tournament_ids: list[str] | None = None,
    data_types: list[str] | None = None,
    year: int | None = None,
    matches_override: list[dict[str, Any]] | None = None,
) -> None:
    """Match-level data ELT subflow.

    From the Prefect UI, provide tournament_ids and year to backfill.
    If neither is supplied, auto-queries stg_atp_tournament_results_new.

    Args:
        tournament_ids: List of tournament IDs to backfill (UI: add items).
        data_types: List of stat types to collect (UI: add items).
            Valid values: match-info, key-stats, stroke-analysis,
            rally-analysis, court-vision. Defaults to all 5.
        year: Year for context (defaults to current year).
        matches_override: Internal use by parent flow — pre-built match list.
    """
    import asyncio

    from tasks.ingestion.get_atp_match_data import (
        get_atp_match_data_task,
        upload_atp_match_data_to_s3_task,
    )
    from tasks.storage.load_atp_match_data_motherduck import (
        load_atp_match_data_to_motherduck_task,
    )
    from tasks.storage.s3_storage import get_bucket_name

    flow_logger = get_run_logger()

    if year is None:
        year = datetime.datetime.now().year

    # Resolve which data types to fetch
    if data_types:
        invalid = [dt for dt in data_types if dt not in MATCH_DATA_TYPES]
        if invalid:
            raise ValueError(
                f"Invalid data_types: {invalid}. "
                f"Valid values: {MATCH_DATA_TYPES}"
            )
        active_types = data_types
    else:
        active_types = MATCH_DATA_TYPES

    bucket = get_bucket_name()

    # -----------------------------------------------------------
    # Resolve matches (priority: matches_override > tournament_ids > auto)
    # -----------------------------------------------------------
    if matches_override is not None:
        matches = matches_override
        flow_logger.info(f"📋 Using {len(matches)} match(es) from parent flow.")
    elif tournament_ids:
        flow_logger.info(
            f"🔍 Backfill mode: looking up all matches for "
            f"{len(tournament_ids)} tournament(s) in {year}..."
        )
        matches = _resolve_matches_for_tournaments(tournament_ids, year)
    else:
        flow_logger.info("🔍 No inputs supplied. Querying new matches...")
        matches = query_new_matches_task()

    if not matches:
        flow_logger.info("ℹ️ No matches to process.")
        return

    flow_logger.info(f"📋 Processing {len(matches)} match(es) x {len(active_types)} data types: {active_types}")
    for m in matches[:10]:  # Show first 10
        flow_logger.info(
            f"  - {m.get('tournament_name', '?')} {m['tournament_id']}/"
            f"{m['match_id']} ({m.get('round', '?')})"
        )
    if len(matches) > 10:
        flow_logger.info(f"  ... and {len(matches) - 10} more")

    # -----------------------------------------------------------
    # Fetch + upload to S3 (async, max 15 concurrent requests)
    # -----------------------------------------------------------
    MAX_CONCURRENT = 15
    loaded_combos: dict[str, set[str]] = {dt: set() for dt in active_types}

    def _safe_log(msg: str) -> str:
        """Sanitise log messages to avoid UnicodeDecodeError in Prefect."""
        return msg.encode("ascii", errors="replace").decode("ascii")

    def _error_reason(e: Exception) -> str:
        """Extract a human-readable error reason from an exception."""
        import httpx as _httpx

        if isinstance(e, _httpx.HTTPStatusError):
            code = e.response.status_code
            if code == 404:
                return "404 Not Found"
            elif code == 403:
                return "403 Forbidden"
            elif code == 429:
                return "429 Rate Limited"
            elif code >= 500:
                return f"{code} Server Error"
            return f"HTTP {code}"
        if isinstance(e, _httpx.TimeoutException):
            return "Timeout"
        if isinstance(e, _httpx.ConnectError):
            return "Connection Failed"
        return f"{type(e).__name__}"

    # Results: list of (ok: bool, data_type, tourn_id, match_id, error_reason)
    async def _fetch_and_upload_one(
        sem: asyncio.Semaphore,
        m: dict,
        data_type: str,
        results: list,
    ) -> None:
        """Fetch a single data type for one match and upload to S3.

        Args:
            sem: Semaphore limiting concurrency.
            m: Match dict with year, tournament_id, match_id.
            data_type: One of the MATCH_DATA_TYPES.
            results: Shared list to accumulate result tuples.
        """
        m_year = m.get("year", year)
        tourn_id = m["tournament_id"]
        match_id = m["match_id"]
        label = f"{tourn_id}/{match_id}"

        async with sem:
            try:
                data = await get_atp_match_data_task.fn(
                    year=m_year,
                    tourn_id=tourn_id,
                    match_id=match_id,
                    data_type=data_type,
                )

                if not data or not data.get("data"):
                    flow_logger.warning(
                        _safe_log(f"  No {data_type} data for {label}. Skipping.")
                    )
                    results.append((False, data_type, tourn_id, match_id, "No data"))
                    return

                # S3 upload is synchronous — fine inside the semaphore
                s3_uri = upload_atp_match_data_to_s3_task.fn(
                    data=data,
                    year=m_year,
                    tourn_id=tourn_id,
                    match_id=match_id,
                    data_type=data_type,
                )
                flow_logger.info(
                    _safe_log(f"  {data_type} {label} -> {s3_uri}")
                )
                results.append((True, data_type, tourn_id, match_id, None))

            except Exception as e:
                reason = _error_reason(e)
                flow_logger.error(
                    _safe_log(
                        f"  {data_type} failed for {label}: {reason} - {e}"
                    )
                )
                results.append((False, data_type, tourn_id, match_id, reason))

            # Brief random pause to be respectful
            await asyncio.sleep(0.5)

    async def _fetch_all() -> list:
        """Run all fetch+upload tasks concurrently with a semaphore."""
        sem = asyncio.Semaphore(MAX_CONCURRENT)
        results: list = []
        tasks = []
        for m in matches:
            for data_type in active_types:
                tasks.append(
                    _fetch_and_upload_one(sem, m, data_type, results)
                )
        flow_logger.info(
            f"Dispatching {len(tasks)} async tasks "
            f"(max {MAX_CONCURRENT} concurrent)..."
        )
        await asyncio.gather(*tasks)
        return results

    # Run the async event loop
    results = asyncio.run(_fetch_all())

    # -----------------------------------------------------------
    # Tally results and build summary
    # -----------------------------------------------------------
    success_count = 0
    fail_count = 0
    # Per (data_type, tourn_id) tallies
    summary: dict[tuple[str, str], dict[str, int]] = {}
    error_details: dict[tuple[str, str], dict[str, int]] = {}

    for ok, dt, tid, mid, reason in results:
        key = (dt, tid)
        if key not in summary:
            summary[key] = {"ok": 0, "fail": 0}
            error_details[key] = {}
        if ok:
            loaded_combos[dt].add(tid)
            summary[key]["ok"] += 1
            success_count += 1
        else:
            summary[key]["fail"] += 1
            fail_count += 1
            if reason:
                error_details[key][reason] = error_details[key].get(reason, 0) + 1

    # Print the summary table
    flow_logger.info(
        f"\n{'='*70}\n"
        f" FETCH SUMMARY: {success_count} succeeded, {fail_count} failed\n"
        f"{'='*70}"
    )
    flow_logger.info(
        f"{'Data Type':<20s} {'Tournament':<12s} {'OK':>5s} {'Fail':>5s}  Errors"
    )
    flow_logger.info("-" * 70)
    for (dt, tid), counts in sorted(summary.items()):
        errs = error_details.get((dt, tid), {})
        err_str = ", ".join(f"{r}: {c}" for r, c in sorted(errs.items())) if errs else ""
        flow_logger.info(
            f"{dt:<20s} {tid:<12s} {counts['ok']:>5d} {counts['fail']:>5d}  {err_str}"
        )
    flow_logger.info("-" * 70)

    # -----------------------------------------------------------
    # Load to MotherDuck (per data_type × per tournament)
    # -----------------------------------------------------------
    flow_logger.info("\n🦆 Loading to MotherDuck...")

    for data_type, tourn_ids in loaded_combos.items():
        if not tourn_ids:
            continue

        config_key = data_type.replace("-", "_")
        for tourn_id in tourn_ids:
            pattern = f"raw/{config_key}/year={year}/tourn={tourn_id}"
            try:
                load_atp_match_data_to_motherduck_task(
                    bucket=bucket,
                    pattern=pattern,
                    data_type=data_type,
                )
                flow_logger.info(
                    f"  ✅ Loaded {data_type} for tourn={tourn_id}"
                )
            except Exception as e:
                flow_logger.error(
                    f"  ❌ Failed to load {data_type} for tourn={tourn_id}: {e}"
                )

    # -----------------------------------------------------------
    # Transform: run dbt staging models
    # -----------------------------------------------------------
    # Build dbt select for only the active data types
    _dbt_model_map = {
        "match-info": "stg_atp_match_info",
        "key-stats": "stg_atp_key_stats",
        "stroke-analysis": "stg_atp_stroke_analysis",
        "rally-analysis": "stg_atp_rally_analysis",
        "court-vision": "stg_atp_court_vision",
    }
    dbt_select = " ".join(_dbt_model_map[dt] for dt in active_types)
    flow_logger.info(f"\n🔄 Running dbt staging models: {dbt_select}")
    run_dbt_models_task(select=dbt_select)

    flow_logger.info(
        f"🎉 Match Data ELT complete. "
        f"{success_count} data files processed across {len(matches)} matches."
    )


# ═══════════════════════════════════════════════════════════════
# PARENT ORCHESTRATION FLOW
# ═══════════════════════════════════════════════════════════════


@flow(
    name="ATP Main Orchestration Pipeline",
    description=(
        "End-to-end pipeline: Calendar ELT → Determine targets → "
        "Tournament Results ELT → Match Data ELT. "
        "Calls each phase as a subflow."
    ),
    retries=0,
    log_prints=True,
)
def atp_main_orchestration_flow(year: int | None = None) -> None:
    """Main orchestration flow chaining all four phases as subflows.

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

    # Phase 4: Match Data ELT (subflow)
    match_data_elt_flow(year=year)

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
    match_data_deploy = match_data_elt_flow.to_deployment(
        name="atp-match-data-elt",
    )

    serve(
        main_deploy,
        calendar_deploy,
        targets_deploy,
        results_deploy,
        match_data_deploy,
    )
