from prefect import flow
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.ingestion.get_atp_tournament import get_atp_tournament_task

@flow(name="Test Tournament Scraper")
def test_tournament_flow():
    # Example: Adelaide International 2024
    # URL: https://www.atptour.com/en/scores/archive/adelaide/8998/2024/results
    url = "https://www.atptour.com/en/scores/archive/adelaide/8998/2024/results"
    tournament_name = "Adelaide International"
    tournament_id = "8998"
    year = 2024
    
    print(f"Testing scraper for {tournament_name} ({year})...")
    results = get_atp_tournament_task(
        url=url,
        tournament_name=tournament_name,
        tournament_id=tournament_id,
        year=year
    )
    
    print(f"Scraped {len(results)} matches.")
    if results:
        print("\nSample match data:")
        print(results[0])

if __name__ == "__main__":
    test_tournament_flow()
