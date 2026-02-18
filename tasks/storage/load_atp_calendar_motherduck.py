"""
MotherDuck Load Task for ATP Calendar.
"""
import logging
import os
from prefect import task
from tasks.storage.motherduck_load import execute_sql_file, get_motherduck_database, get_config

logger = logging.getLogger(__name__)

@task(name="load_atp_calendar_to_motherduck")
def load_atp_calendar_to_motherduck_task(bucket: str, pattern: str, database: str | None = None) -> int:
    """
    Load ATP Calendar JSON files from S3 into MotherDuck using a standalone SQL file.
    
    Args:
        bucket: S3 bucket name
        pattern: S3 key pattern (prefix) for the files
        database: MotherDuck database name (optional, defaults to env var)
        
    Returns:
        Number of rows in the table after load
    """
    if database is None:
        database = get_motherduck_database()
        
    config = get_config()
    table_name = config['motherduck']['tables']['atp_results_archive']
    
    # Path to the SQL file relative to this script
    sql_path = os.path.join(os.path.dirname(__file__), "sql", "load_atp_calendar.sql")
    
    params = {
        "database": database,
        "table_name": table_name,
        "bucket": bucket,
        "pattern": pattern
    }
    
    try:
        con = execute_sql_file(
            sql_path=sql_path,
            params=params,
            description=f"Loading ATP Calendar from s3://{bucket}/{pattern}"
        )
        
        # Get count of newly inserted rows
        res = con.execute(f"SELECT COUNT(*) FROM {database}.{table_name}").fetchone()
        count = res[0]
        con.close()
        
        return count
        
    except Exception as e:
        logger.error(f"Failed to load ATP Calendar to MotherDuck: {e}")
        raise

if __name__ == "__main__":
    from prefect import flow
    from dotenv import load_dotenv
    
    # Define a temporary utility flow to run the task in isolation
    @flow(name="Utility: Load ATP Calendar to MotherDuck")
    def run_load_utility(bucket: str, pattern: str, database: str | None = None):
        load_atp_calendar_to_motherduck_task(bucket, pattern, database)

    load_dotenv()
    # Default values for easy testing (matches the atp_calendar_flow logic)
    print("🚀 Running standalone MotherDuck load...")
    run_load_utility(
        bucket="infotennis-v2",
        pattern="raw/atp_results_archive/incoming/year=2024",
        database=None
    )
