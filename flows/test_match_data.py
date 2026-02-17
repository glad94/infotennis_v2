from prefect import flow
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.ingestion.get_atp_match_data import get_atp_match_data_task

@flow(name="Test Match Data Ingestion")
async def test_match_data_flow():
    year = 2025
    tourn_id = "8998" # Adelaide
    match_id = "ms024" # Some match ID for Adelaide 2025
    
    # Test 1: Hawkeye Match Info (No decode)
    print("Testing Hawkeye 'match-info' ingestion...")
    try:
        match_info = await get_atp_match_data_task(
            year=year,
            tourn_id=tourn_id,
            match_id=match_id,
            data_type="match-info"
        )
        print(f"Successfully retrieved match-info. Keys: {list(match_info.keys())[:5]}...")
    except Exception as e:
        print(f"Match-info failed: {e}")

    # Test 2: Infosys Key Stats (Requires decode)
    print("\nTesting Infosys 'key-stats' ingestion...")
    try:
        key_stats = await get_atp_match_data_task(
            year=year,
            tourn_id=tourn_id,
            match_id=match_id,
            data_type="key-stats"
        )
        print("Successfully retrieved and decoded key-stats.")
    except Exception as e:
        print(f"Key-stats failed: {e}")

import asyncio

if __name__ == "__main__":
    asyncio.run(test_match_data_flow())
