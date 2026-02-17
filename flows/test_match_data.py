from prefect import flow
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.ingestion.get_atp_match_data import get_atp_match_data_task, upload_atp_match_data_to_s3_task

@flow(name="Test Match Data Ingestion and S3 Upload")
async def test_match_data_flow():
    year = 2025
    tourn_id = "8998" # Adelaide
    match_id = "ms024" # Some match ID for Adelaide 2025
    
    # Dummy match metadata for naming
    match_metadata = {
        "player1_name": "Novak Djokovic",
        "player2_name": "Rafael Nadal",
        "round": "Finals"
    }
    
    # Test 1: Hawkeye Match Info (No decode)
    print("Testing Hawkeye 'match-info' ingestion & S3 upload...")
    try:
        match_info = await get_atp_match_data_task(
            year=year,
            tourn_id=tourn_id,
            match_id=match_id,
            data_type="match-info"
        )
        print(f"Successfully retrieved match-info. Keys: {list(match_info.keys())[:5]}...")
        
        s3_uri = upload_atp_match_data_to_s3_task(
            data=match_info,
            year=year,
            tourn_id=tourn_id,
            match_id=match_id,
            data_type="match-info",
            match_metadata=match_metadata
        )
        print(f"Uploaded to S3: {s3_uri}")
        
    except Exception as e:
        print(f"Match-info/S3 failed: {e}")

    # Test 2: Infosys Key Stats (Requires decode)
    print("\nTesting Infosys 'key-stats' ingestion & S3 upload...")
    try:
        key_stats = await get_atp_match_data_task(
            year=year,
            tourn_id=tourn_id,
            match_id=match_id,
            data_type="key-stats"
        )
        print("Successfully retrieved and decoded key-stats.")
        
        s3_uri = upload_atp_match_data_to_s3_task(
            data=key_stats,
            year=year,
            tourn_id=tourn_id,
            match_id=match_id,
            data_type="key-stats",
            match_metadata=match_metadata
        )
        print(f"Uploaded to S3: {s3_uri}")
        
    except Exception as e:
        print(f"Key-stats/S3 failed: {e}")

import asyncio

if __name__ == "__main__":
    asyncio.run(test_match_data_flow())
