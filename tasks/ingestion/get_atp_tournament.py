from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential
import httpx
from bs4 import BeautifulSoup
import logging
import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def parse_player_scores(elem_player_match_score):
    player_scores = []
    for set_score in elem_player_match_score.find_all("div", class_="score-item"):
        score_list = set_score.find_all("span")
        if len(score_list) == 0:
            continue
        else:
            if len(score_list) == 1:
                score = score_list[0].text
            elif len(score_list) == 2:
                score = score_list[0].text
                score_tb = score_list[1].text
                score = f"{score}({score_tb})"
        player_scores.append(score)
    return player_scores

def move_bracketed_parts(lst):
    new_lst = []
    for item in lst:
        bracketed_part = ""
        remaining_part = item
        if '(' in item and ')' in item:
            bracket_start = item.index('(')
            bracket_end = item.index(')')
            bracketed_part = item[bracket_start:bracket_end+1]
            remaining_part = item[:bracket_start] + item[bracket_end+1:]
        new_item = remaining_part + bracketed_part
        new_lst.append(new_item)
    return new_lst

def parse_match_score(elems_match_score):
    if len(elems_match_score) < 2:
        return ""
    player_1_score = parse_player_scores(elems_match_score[0])
    player_2_score = parse_player_scores(elems_match_score[1])

    score_list = [f"{i}{j}" for i,j in zip(player_1_score, player_2_score)]
    score_str = " ".join(move_bracketed_parts(score_list))
    return score_str

def parse_match_content(elem_match):
    try:
        round_elem = elem_match.find("strong")
        round_text = round_elem.text.split(" - ")[0].replace("-", "") if round_elem else "Unknown Round"
        round_name = " ".join([word.capitalize() for word in round_text.split()])
        
        url_atp = "https://www.atptour.com"
        cta_elem = elem_match.find("div", class_="match-cta")
        url = ""
        if cta_elem:
            stats_link = cta_elem.find("a", string=lambda text: text and ("Match Stats" in text or "Stats" in text))
            if stats_link:
                url = url_atp + stats_link["href"]
        
        match_id = url.split("/")[-1] if url else ""
        score = parse_match_score(elem_match.find_all("div", class_="scores"))

        elem_players = elem_match.find_all("div", class_="name")
        elem_pinfos = [e.find({"a", "p"}) for e in elem_players]
        
        player_1_name = player_2_name = player_1_id = player_2_id = player_1_seed = player_2_seed = player_1_nation = player_2_nation = ""

        if len(elem_players) == 2:
            p1_info = elem_pinfos[0]
            p2_info = elem_pinfos[1]
            player_1_name = " ".join(p1_info.contents[0].text.split()) if p1_info else ""
            player_2_name = " ".join(p2_info.contents[0].text.split()) if p2_info else ""
            player_1_seed = elem_players[0].find("span").text.strip("()") if elem_players[0].find("span") else ""
            player_2_seed = elem_players[1].find("span").text.strip("()") if elem_players[1].find("span") else ""
            player_1_id = p1_info["href"].split("/")[-2] if p1_info and p1_info.has_attr("href") else ""
            player_2_id = p2_info["href"].split("/")[-2] if p2_info and p2_info.has_attr("href") else ""
            
            try:
                flags = elem_match.find_all("svg")
                if len(flags) >= 2:
                    player_1_nation = flags[0].find("use")["href"].split('/')[-1].split('#')[1].replace('flag-','') if flags[0].find("use") else "-"
                    player_2_nation = flags[1].find("use")["href"].split('/')[-1].split('#')[1].replace('flag-','') if flags[1].find("use") else "-"
            except:
                player_1_nation = player_2_nation = "-"
                
        elif len(elem_players) == 4:
            # Doubles logic
            p1a_info, p1b_info, p2a_info, p2b_info = elem_pinfos
            player_1_name = f"{' '.join(p1a_info.contents[0].text.split())}, {' '.join(p1b_info.contents[0].text.split())}"
            player_2_name = f"{' '.join(p2a_info.contents[0].text.split())}, {' '.join(p2b_info.contents[0].text.split())}"
            
            player_1_seed = [e.find("span").text.strip("()") for e in elem_players][0] if elem_players[0].find("span") else ""
            player_2_seed = [e.find("span").text.strip("()") for e in elem_players][2] if elem_players[2].find("span") else ""
            
            player_1_id = f"{p1a_info['href'].split('/')[-2]}, {p1b_info['href'].split('/')[-2]}"
            player_2_id = f"{p2a_info['href'].split('/')[-2]}, {p2b_info['href'].split('/')[-2]}"

            try:
                flags = elem_match.find_all("svg")
                if len(flags) >= 4:
                    f1a = flags[0].find("use")["href"].split('/')[-1].split('#')[1].replace('flag-','')
                    f1b = flags[1].find("use")["href"].split('/')[-1].split('#')[1].replace('flag-','')
                    f2a = flags[2].find("use")["href"].split('/')[-1].split('#')[1].replace('flag-','')
                    f2b = flags[3].find("use")["href"].split('/')[-1].split('#')[1].replace('flag-','')
                    player_1_nation = f"{f1a}, {f1b}"
                    player_2_nation = f"{f2a}, {f2b}"
            except:
                player_1_nation = player_2_nation = "-"

        return {
            "round": round_name,
            "player1_name": player_1_name,
            "player1_id": player_1_id,
            "player1_seed": player_1_seed,
            "player1_nation": player_1_nation,
            "player2_name": player_2_name,
            "player2_id": player_2_id,
            "player2_seed": player_2_seed,
            "player2_nation": player_2_nation,
            "score": score,
            "url": url,
            "match_id": match_id
        }
    except Exception as e:
        logging.getLogger("scrape_atp_tournament").warning(f"Error parsing match content: {e}")
        return None

@task(name="get_atp_tournament")
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_atp_tournament_task(url: str, tournament_name: str, tournament_id: str, year: int, match_type: str = "singles"):
    """
    Scrapes ATP Tournament Results for a given tournament.
    """
    logger = logging.getLogger("get_atp_tournament")
    
    if "matchType" not in url:
        url = f"{url}?matchType={match_type}"
    
    headers = {"User-Agent": get_random_user_agent()}
    
    try:
        with httpx.Client(timeout=20, follow_redirects=True) as client:
            resp = client.get(url, headers=headers)
            resp.raise_for_status()
            html_content = resp.text
    except Exception as e:
        logger.error(f"Request failed for {url}: {e}")
        raise

    soup = BeautifulSoup(html_content, 'html.parser')
    elem_days = soup.find_all("div", class_="atp_accordion-item")
    
    matches_data = []
    for day in elem_days:
        elem_matches = day.find_all("div", class_="match")
        for elem_match in elem_matches:
            match_dict = parse_match_content(elem_match)
            if match_dict:
                match_dict.update({
                    "year": year,
                    "tournament_name": tournament_name,
                    "tournament_id": tournament_id
                })
                matches_data.append(match_dict)
    
    logger.info(f"Successfully scraped {len(matches_data)} matches for {tournament_name} ({year}).")
    return matches_data
