"""
MotherDuck Load Task for ATP Match-Level Data.

Generic loader that handles all 5 match data types: match-info, key-stats,
stroke-analysis, rally-analysis, court-vision.
"""
from __future__ import annotations

import logging
import os

from prefect import task

from tasks.storage.motherduck_load import execute_sql_file, get_config, get_motherduck_database

logger = logging.getLogger(__name__)

# Maps data type identifiers to config keys
DATA_TYPE_TABLE_MAP = {
    "match-info": "match_info",
    "key-stats": "key_stats",
    "stroke-analysis": "stroke_analysis",
    "rally-analysis": "rally_analysis",
    "court-vision": "court_vision",
}


@task(name="load_atp_match_data_to_motherduck")
def load_atp_match_data_to_motherduck_task(
    bucket: str,
    pattern: str,
    data_type: str,
    database: str | None = None,
) -> int:
    """Load ATP match-level JSON files from S3 into MotherDuck.

    Args:
        bucket: S3 bucket name.
        pattern: S3 key pattern (prefix) for the files.
        data_type: One of match-info, key-stats, stroke-analysis,
                   rally-analysis, court-vision.
        database: MotherDuck database name (optional, defaults to env var).

    Returns:
        Number of rows in the table after load.
    """
    if database is None:
        database = get_motherduck_database()

    config_key = DATA_TYPE_TABLE_MAP.get(data_type)
    if config_key is None:
        raise ValueError(
            f"Unknown data_type '{data_type}'. "
            f"Expected one of: {list(DATA_TYPE_TABLE_MAP.keys())}"
        )

    config = get_config()
    table_name = config["motherduck"]["tables"][config_key]

    sql_path = os.path.join(
        os.path.dirname(__file__), "sql", "load_atp_match_data.sql"
    )

    params = {
        "database": database,
        "table_name": table_name,
        "bucket": bucket,
        "pattern": pattern,
    }

    try:
        con = execute_sql_file(
            sql_path=sql_path,
            params=params,
            description=f"Loading {data_type} from s3://{bucket}/{pattern}",
        )

        res = con.execute(
            f"SELECT COUNT(*) FROM {database}.{table_name}"
        ).fetchone()
        count = res[0]
        con.close()

        return count

    except Exception as e:
        logger.error(f"Failed to load {data_type} to MotherDuck: {e}")
        raise


if __name__ == "__main__":
    from dotenv import load_dotenv
    from prefect import flow

    @flow(name="Utility: Load ATP Match Data to MotherDuck")
    def run_load_utility(
        bucket: str, pattern: str, data_type: str, database: str | None = None
    ) -> None:
        """Run the MotherDuck load task standalone for testing.

        Args:
            bucket: S3 bucket name.
            pattern: S3 key prefix.
            data_type: Match data type identifier.
            database: MotherDuck database name.
        """
        load_atp_match_data_to_motherduck_task(
            bucket, pattern, data_type, database
        )

    load_dotenv()
    print("🚀 Running standalone MotherDuck load for ATP Match Data...")
    run_load_utility(
        bucket="infotennis-v2",
        pattern="raw/key_stats/year=2026/tourn=339",
        data_type="key-stats",
        database=None,
    )
