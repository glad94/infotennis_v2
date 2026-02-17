from prefect import flow
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from tasks.ingestion.get_atp_tournament import get_atp_tournament_task, upload_atp_tournament_to_s3_task

@flow(name="Test Tournament Scraper and S3 Upload")
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
    
    if results:
        print(f"Successfully scraped {len(results)} matches.")
        s3_uri = upload_atp_tournament_to_s3_task(results, tournament_id, year)
        print(f"Uploaded to S3: {s3_uri}")
    else:
        print("Failed to scrape tournament results.")

if __name__ == "__main__":
    test_tournament_flow()
