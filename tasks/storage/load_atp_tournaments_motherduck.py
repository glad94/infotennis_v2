"""
MotherDuck Load Task for ATP Tournaments.
"""
import logging
import os

from prefect import task

from tasks.storage.motherduck_load import execute_sql_file, get_config, get_motherduck_database

logger = logging.getLogger(__name__)


@task(name="load_atp_tournaments_to_motherduck")
def load_atp_tournaments_to_motherduck_task(
    bucket: str, pattern: str, database: str | None = None
) -> int:
    """Load ATP Tournament JSON files from S3 into MotherDuck.

    Args:
        bucket: S3 bucket name.
        pattern: S3 key pattern (prefix) for the files.
        database: MotherDuck database name (optional, defaults to env var).

    Returns:
        Number of rows in the table after load.
    """
    if database is None:
        database = get_motherduck_database()

    config = get_config()
    table_name = config['motherduck']['tables']['atp_tournaments']

    sql_path = os.path.join(os.path.dirname(__file__), "sql", "load_atp_tournaments.sql")

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
            description=f"Loading ATP Tournaments from s3://{bucket}/{pattern}",
        )

        res = con.execute(f"SELECT COUNT(*) FROM {database}.{table_name}").fetchone()
        count = res[0]
        con.close()

        return count

    except Exception as e:
        logger.error(f"Failed to load ATP Tournaments to MotherDuck: {e}")
        raise


if __name__ == "__main__":
    from dotenv import load_dotenv
    from prefect import flow

    @flow(name="Utility: Load ATP Tournaments to MotherDuck")
    def run_load_utility(bucket: str, pattern: str, database: str | None = None) -> None:
        """Run the MotherDuck load task standalone for testing.

        Args:
            bucket: S3 bucket name.
            pattern: S3 key prefix.
            database: MotherDuck database name.
        """
        load_atp_tournaments_to_motherduck_task(bucket, pattern, database)

    load_dotenv()
    print("🚀 Running standalone MotherDuck load for ATP Tournaments...")
    run_load_utility(
        bucket="infotennis-v2",
        pattern="raw/atp_tournaments/incoming/year=2026",
        database=None,
    )
