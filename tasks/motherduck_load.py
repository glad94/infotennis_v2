"""
MotherDuck Loading Task for infotennis_v2.

Loads JSON data from S3 into MotherDuck using schema-on-read pattern.
Implements idempotency by tracking loaded files in a metadata table.
"""
import logging
import os
from datetime import datetime, timezone

import duckdb
from prefect import task

logger = logging.getLogger(__name__)


def get_motherduck_connection():
    """Create and return a MotherDuck connection using env vars."""
    token = os.getenv("MOTHERDUCK_TOKEN")
    database = os.getenv("MOTHERDUCK_DATABASE", "infotennis_raw")
    
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable is required")
    
    return duckdb.connect(f"md:{database}?motherduck_token={token}")


def extract_endpoint_from_s3_uri(s3_uri: str) -> str:
    """
    Extract endpoint name from S3 URI.
    Format: s3://{bucket}/raw/{endpoint_name}/year={YYYY}/month={MM}/{timestamp}.json
    """
    parts = s3_uri.replace("s3://", "").split("/")
    if len(parts) >= 3:
        return parts[2]
    raise ValueError(f"Cannot extract endpoint from S3 URI: {s3_uri}")


def ensure_loaded_files_table(con) -> None:
    """Create the _loaded_files metadata table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS _loaded_files (
            s3_uri VARCHAR PRIMARY KEY,
            endpoint VARCHAR,
            table_name VARCHAR,
            rows_loaded INTEGER,
            loaded_at TIMESTAMP
        )
    """)


def is_file_already_loaded(con, s3_uri: str) -> bool:
    """Check if a file has already been loaded."""
    result = con.execute(
        "SELECT COUNT(*) FROM _loaded_files WHERE s3_uri = ?",
        [s3_uri]
    ).fetchone()
    return result[0] > 0


@task(
    name="load_to_motherduck",
    description="Load JSON from S3 into MotherDuck using schema-on-read",
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def load_to_motherduck(s3_uri: str) -> int:
    """
    Load JSON data from S3 into MotherDuck using schema-on-read.
    
    Args:
        s3_uri: Full S3 URI of the JSON file
        
    Returns:
        Number of rows loaded (0 if file was already loaded)
    """
    endpoint = extract_endpoint_from_s3_uri(s3_uri)
    table_name = f"raw_{endpoint}"
    
    print(f"Loading {s3_uri} into table '{table_name}'")
    
    con = get_motherduck_connection()
    
    try:
        ensure_loaded_files_table(con)
        
        # Idempotency check
        if is_file_already_loaded(con, s3_uri):
            print(f"⏭️ File already loaded, skipping: {s3_uri}")
            return 0
        
        # Configure S3 access
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_region = '{os.getenv('AWS_REGION', 'us-east-1')}'")
        con.execute(f"SET s3_access_key_id = '{os.getenv('AWS_ACCESS_KEY_ID')}'")
        con.execute(f"SET s3_secret_access_key = '{os.getenv('AWS_SECRET_ACCESS_KEY')}'")
        
        # Check if table exists
        table_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0] > 0
        
        if not table_exists:
            print(f"Creating table '{table_name}' with schema-on-read")
            con.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT *, '{s3_uri}' as _source_file, CURRENT_TIMESTAMP as _loaded_at
                FROM read_json_auto('{s3_uri}', maximum_object_size=67108864)
            """)
        else:
            print(f"Inserting into existing table '{table_name}'")
            con.execute(f"""
                INSERT INTO {table_name}
                SELECT *, '{s3_uri}' as _source_file, CURRENT_TIMESTAMP as _loaded_at
                FROM read_json_auto('{s3_uri}', maximum_object_size=67108864)
            """)
        
        # Get row count
        row_count = con.execute(f"""
            SELECT COUNT(*) FROM {table_name} WHERE _source_file = '{s3_uri}'
        """).fetchone()[0]
        
        # Register loaded file
        con.execute("""
            INSERT INTO _loaded_files (s3_uri, endpoint, table_name, rows_loaded, loaded_at)
            VALUES (?, ?, ?, ?, ?)
        """, [s3_uri, endpoint, table_name, row_count, datetime.now(timezone.utc)])
        
        print(f"✅ Successfully loaded {row_count} rows into '{table_name}'")
        return row_count
        
    finally:
        con.close()
