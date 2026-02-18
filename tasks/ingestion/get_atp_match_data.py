import base64
import datetime
import json
import logging
import os
import random
from typing import Dict, Any

import httpx
import numpy as np
import yaml
from bs4 import BeautifulSoup
import cryptography.hazmat.backends
import cryptography.hazmat.primitives.ciphers
import cryptography.hazmat.primitives.ciphers.algorithms
import cryptography.hazmat.primitives.ciphers.modes
import cryptography.hazmat.primitives.padding
import sys
from pathlib import Path
# Add project root to sys.path for standalone execution
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

# Decrypting Utilities (Ported from original infotennis)
def format_date_for_aes(t):
    """
    Returns a formatted form of the 'lastModified' key from the encrypted data object.
    """
    t_tstamp = datetime.datetime.fromtimestamp(t/1000, tz=datetime.timezone.utc)
    n = t_tstamp.day
    r = int(str(n if n >= 10 else "0" + str(n))[::-1])
    i = t_tstamp.year
    a = int(str(i)[::-1])
    # Reproduce logic from original project
    o = np.base_repr(int(str(t), 16), 36).lower() + np.base_repr((i + a) * (n + r), 24).lower()
    s = len(o)
    if s < 14:
        o += "0" * (14 - s)
    elif s > 14:
        o = o[:14]
    return "#" + o + "$"

def decode_infosys_data(data):
    """
    Decrypting algorithm for encrypted ATP match statistics data.
    """
    e = format_date_for_aes(data['lastModified'])
    n = e.encode()
    r = e.upper().encode()
    cipher = cryptography.hazmat.primitives.ciphers.Cipher(
        cryptography.hazmat.primitives.ciphers.algorithms.AES(n),
        cryptography.hazmat.primitives.ciphers.modes.CBC(r),
        backend=cryptography.hazmat.backends.default_backend()
    )
    decryptor = cipher.decryptor()
    i = decryptor.update(base64.b64decode(data['response'])) + decryptor.finalize()
    # Handle padding/trailing chars as per original project logic
    decoded_str = i.decode("utf-8")
    return json.loads(decoded_str.replace(decoded_str[-1], ""))

@task(name="get_atp_match_data")
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def get_atp_match_data_task(year: int, tourn_id: str, match_id: str, data_type: str) -> Dict[str, Any]:
    """
    Retrieves match-level data (stats or info) from ATP/Infosys asynchronously.
    """
    logger = logging.getLogger("get_atp_match_data")
    config = get_config()
    match_id_upper = str(match_id).upper()
    
    # Select endpoint and URL
    if data_type == "match-info":
        url_template = config['atp']['hawkeye_url_template']
        url = url_template % {'year': year, 'tourn_id': tourn_id, 'match_id': match_id_upper}
        need_decode = False
    else:
        # Infosys endpoints (key-stats, rally-analysis, etc.)
        base_url = config['infosys']['base_url']
        endpoints = config['infosys']['endpoints']
        if data_type not in endpoints:
            raise ValueError(f"Unknown match data_type: {data_type}")
        url = endpoints[data_type] % {
            'base_url': base_url,
            'year': year,
            'tourn_id': tourn_id,
            'match_id': match_id_upper
        }
        need_decode = True

    headers = {"User-Agent": get_random_user_agent()}
    logger.info(f"Fetching {data_type} from {url}")

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            
            if need_decode:
                # Decrypt Infosys data
                results_json = resp.json()
                match_payload = decode_infosys_data(results_json)
            else:
                # Direct JSON from Hawkeye
                match_payload = resp.json()
            
            retrieved_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            return {
                "metadata": {
                    "retrieved_at": retrieved_at
                },
                "data": match_payload
            }
                
    except Exception as e:
        logger.error(f"Failed to get {data_type} for {year}/{tourn_id}/{match_id}: {e}")
        raise
                
from tasks.storage.s3_storage import upload_json_to_s3, get_bucket_name

def get_round_short(round_n: str) -> str:
    """
    Abbreviate round names for file naming.
    Ported from original infotennis project.
    """
    if "Round Of" in round_n:
        return round_n.split(" ")[0][0] + round_n.split(" ")[-1]
    elif "Round Qualifying" in round_n:
        return "Q" + round_n.split(" ")[0][0]
    elif "Round" in round_n:
        return "".join([s[0] for s in round_n.split(" ")])
    elif round_n in ["Quarterfinals", "Quarter-Finals"]:
        return "QF"
    elif round_n in ["Semifinals", "Semi-Finals"]:
        return "SF"
    elif round_n in ["Final", "Finals"]:
        return "F"
    return round_n

@task(name="upload_atp_match_data_to_s3")
def upload_atp_match_data_to_s3_task(data: dict, year: int, tourn_id: str, match_id: str, data_type: str, match_metadata: dict) -> str:
    """
    Upload ATP Match Data (stats or info) to S3 with custom naming.
    """
    # Normalize player names for filename
    p1 = match_metadata.get("player1_name", "P1").replace(" ", "-")
    p2 = match_metadata.get("player2_name", "P2").replace(" ", "-")
    round_name = match_metadata.get("round", "R")
    round_short = get_round_short(round_name)
    match_id_upper = str(match_id).upper()
    
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    ts_str = timestamp.strftime("%Y%m%d_%H%M%S")
    
    config = get_config()
    bucket = os.getenv("S3_BUCKET", config['s3']['default_bucket'])
    key_template = config['s3']['paths']['match_stats']
    key = key_template % {
        'year': year,
        'tourn_id': tourn_id,
        'filename': filename,
        'timestamp': ts_str
    }
    
    metadata = {
        "endpoint": "match_stats",
        "data_type": data_type,
        "year": str(year),
        "tournament_id": tourn_id,
        "match_id": match_id_upper
    }
    
    return upload_json_to_s3(data, bucket, key, metadata)

import argparse
import asyncio
from dotenv import load_dotenv

async def main_cli():
    parser = argparse.ArgumentParser(description="Get ATP Match Stats/Info and upload to S3")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--tourn-id", type=str, required=True)
    parser.add_argument("--match-id", type=str, required=True)
    parser.add_argument("--type", type=str, required=True, choices=["match-info", "key-stats", "rally-analysis", "stroke-analysis", "court-vision"])
    
    # Optional metadata for naming
    parser.add_argument("--p1", type=str, default="P1")
    parser.add_argument("--p2", type=str, default="P2")
    parser.add_argument("--round", type=str, default="R")
    
    args = parser.parse_args()
    
    match_metadata = {
        "player1_name": args.p1,
        "player2_name": args.p2,
        "round": args.round
    }
    
    print(f"Fetching {args.type} for {args.year}/{args.tourn_id}/{args.match_id}...")
    data = await get_atp_match_data_task(
        year=args.year,
        tourn_id=args.tourn_id,
        match_id=args.match_id,
        data_type=args.type
    )
    
    if data and data.get("data"):
        s3_uri = upload_atp_match_data_to_s3_task(
            data=data,
            year=args.year,
            tourn_id=args.tourn_id,
            match_id=args.match_id,
            data_type=args.type,
            match_metadata=match_metadata
        )
        print(f"Upload complete: {s3_uri}")
    else:
        print("No data found.")

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main_cli())
