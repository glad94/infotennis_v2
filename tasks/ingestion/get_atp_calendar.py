import sys
from pathlib import Path
# Add project root to sys.path for standalone execution
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import argparse
from dotenv import load_dotenv

import httpx
import datetime
import random
import os
import yaml
import logging
from typing import Any, Dict
from bs4 import BeautifulSoup
from prefect import task
from tenacity import ( 
    retry,
    stop_after_attempt,
    wait_exponential
)

USER_AGENTS = [
    # A few common user agents; you can expand this list or use fake-useragent if available
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

@task
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_atp_results_archive_task(year: int | None = None):
    """
    Scrape the ATP Results Archive for a specific year and return a list of tournament dicts.
    """
    if year is None:
        year = datetime.datetime.now().year
    config = get_config()
    root_url = config['atp']['root_url']
    archive_path = config['atp']['results_archive_path']
    url = f"{root_url}{archive_path}?year={year}"
    headers = {"User-Agent": get_random_user_agent()}
    logger = logging.getLogger("get_atp_results_archive_task")
    try:
        with httpx.Client(timeout=20, follow_redirects=True) as client:
            print(f"Fetching ATP archive from: {url}")
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            html_content = resp.text
    except Exception as e:
        logger.error(f"Request failed for {url}: {e}")
        raise

    soup = BeautifulSoup(html_content, 'html.parser')
    tournaments_data = []
    for ul in soup.find_all('ul', class_='events'):
        try:
            li = ul.find('li')
            if not li:
                continue
            # Tournament Info
            tinfo = li.find('div', class_='tournament-info')
            # Category (from badge img alt)
            badge_img = tinfo.find('img', class_='events_banner')
            category = badge_img['alt'].strip() if badge_img and badge_img.has_attr('alt') else 'Other'
            # Tournament name and profile link
            profile_link = tinfo.find('a', class_='tournament__profile')
            name = profile_link.find('span', class_='name').text.strip() if profile_link else None
            relative_profile_url = profile_link['href'] if profile_link and profile_link.has_attr('href') else None
            # Tournament ID from profile URL
            tourn_id = None
            if relative_profile_url:
                url_parts = relative_profile_url.strip('/').split('/')
                tourn_id = url_parts[-2] if len(url_parts) > 2 else None
            # Venue/city/country
            venue_span = profile_link.find('span', class_='venue') if profile_link else None
            location_text = venue_span.text.strip().rstrip('|').strip() if venue_span else ''
            if ',' in location_text:
                city, country = [part.strip() for part in location_text.split(',', 1)]
            else:
                city, country = location_text, "Unknown"
            # Dates
            date_span = profile_link.find('span', class_='Date') if profile_link else None
            dates = date_span.text.strip() if date_span else ''
            # Winners
            cta = li.find('div', class_='cta-holder')
            singles_winner = None
            doubles_winner = []
            if cta:
                for dl in cta.find_all('dl', class_='winner'):
                    dt = dl.find('dt')
                    if not dt:
                        continue
                    label = dt.text.strip().lower()
                    if 'singles' in label or 'team' in label:
                        # Team or singles winner
                        dd = dl.find('dd')
                        if dd:
                            a = dd.find('a')
                            singles_winner = a.text.strip() if a else dd.text.strip()
                    elif 'doubles' in label:
                        # Doubles winners (multiple dd)
                        dds = dl.find_all('dd')
                        for dd in dds:
                            a = dd.find('a')
                            if a:
                                doubles_winner.append(a.text.strip())
                            elif dd.text.strip():
                                doubles_winner.append(dd.text.strip())
            # Results URL
            results_url = None
            non_live_cta = li.find('div', class_='non-live-cta')
            if non_live_cta:
                results_a = non_live_cta.find('a', class_='results')
                if results_a and results_a.has_attr('href'):
                    results_url = f"{root_url}{results_a['href']}"
            tournaments_data.append({
                "year": year,
                "tournament": name,
                "tournament_id": tourn_id,
                "category": category,
                "city": city,
                "country": country,
                "dates": dates,
                "singles_winner": singles_winner,
                "doubles_winner": doubles_winner if doubles_winner else None,
                "url": results_url
            })
        except Exception as e:
            logger.warning(f"Skipping a tournament due to parsing error: {e}")
            continue
    
    retrieved_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return {
        "metadata": {
            "retrieved_at": retrieved_at
        },
        "data": tournaments_data
    }
from tasks.storage.s3_storage import upload_json_to_s3, get_bucket_name

@task(name="upload_atp_calendar_to_s3")
def upload_atp_calendar_to_s3_task(data: dict, year: int) -> str:
    """
    Upload ATP Calendar data (wrapped in metadata) to S3 staging (incoming).
    """
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
    
    config = get_config()
    bucket = os.getenv("S3_BUCKET", config['s3']['default_bucket'])
    key_template = config['s3']['paths']['atp_results_archive']
    key = key_template % {'year': year, 'timestamp': ts_str}
    
    metadata = {
        "endpoint": "atp_results_archive",
        "year": str(year),
        "count": str(len(data.get("data", []))),
        # Also preserve scraped_at if available or use current
        "scraped_at": data.get("metadata", {}).get("retrieved_at", timestamp.isoformat())
    }
    
    return upload_json_to_s3(data, bucket, key, metadata)
    

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Scrape ATP Calendar and upload to S3")
    parser.add_argument("--year", type=int, help="Year to scrape", default=datetime.datetime.now().year)
    args = parser.parse_args()
    
    print(f"Scraping ATP Calendar for year: {args.year}")
    results = get_atp_results_archive_task(args.year)
    
    if results and results.get("data"):
        s3_uri = upload_atp_calendar_to_s3_task(results, args.year)
        print(f"Upload complete: {s3_uri}")
    else:
        print("No tournament data found.")
