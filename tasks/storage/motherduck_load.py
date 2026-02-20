"""
MotherDuck Loading Task for infotennis_v2.

Loads JSON data from S3 into MotherDuck using schema-on-read pattern.
Implements idempotency by tracking loaded files in a metadata table.
"""
from __future__ import annotations

import logging
import os

import yaml

logger = logging.getLogger(__name__)

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_motherduck_connection():
    """Create and return a MotherDuck connection using env vars."""
    import duckdb

    token = os.getenv("MOTHERDUCK_TOKEN")
    database = get_motherduck_database()
    
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable is required")
    
    return duckdb.connect(f"md:{database}?motherduck_token={token}")


def get_motherduck_database() -> str:
    """Get MotherDuck database name from env var or use default."""
    config = get_config()
    return os.getenv("MOTHERDUCK_DATABASE", config['motherduck']['default_database'])




def execute_sql_file(sql_path: str, params: dict, description: str = "SQL Execution") -> duckdb.DuckDBPyConnection:
    """
    Read a SQL file, format it with params, and execute it in MotherDuck.
    
    Args:
        sql_path: Path to the .sql file
        params: Dictionary of parameters to format the SQL string
        description: A short description for logging
        
    Returns:
        The active DuckDB connection (caller should close it if not needed further)
    """
    print(f"🚀 Starting: {description}")
    
    # 1. Read SQL file
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found at: {sql_path}")
        
    with open(sql_path, "r") as f:
        sql_template = f.read()
        
    # 2. Format SQL
    try:
        sql = sql_template.format(**params)
    except KeyError as e:
        logger.error(f"Missing parameter for SQL template: {e}")
        raise
        
    # 3. Connect and Execute
    con = get_motherduck_connection()
    try:
        # Configure S3 access
        print("🔧 Configuring MotherDuck S3 access (httpfs)...")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_region = '{os.getenv('AWS_REGION')}'")
        con.execute(f"SET s3_access_key_id = '{os.getenv('AWS_ACCESS_KEY_ID')}'")
        con.execute(f"SET s3_secret_access_key = '{os.getenv('AWS_SECRET_ACCESS_KEY')}'")
        
        print(f"📊 Executing SQL from {sql_path}...")
        print("--- COMPILED SQL START ---")
        print(sql)
        print("--- COMPILED SQL END ---")
        con.execute(sql)
        print(f"✅ {description} completed successfully.")
        return con
    except Exception as e:
        con.close()
        logger.error(f"Failed to execute SQL file {sql_path}: {e}")
        raise
