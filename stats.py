import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import sqlite3
import re

import os
import sys

# this enables imports from other folders in parent directory
current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

from db import get_connection

import time
import random
from tqdm import tqdm

TM_BASE_URL = "https://www.transfermarkt.com"


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}


def load_players_from_db():
    """
    Load all player records from the 'players' table in the database into a pandas DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing all player records.
    """
    print("[INFO] Establishing database connection...")

    try:
        conn = get_connection()
        print("[INFO] Connection established.")

        query = "SELECT * FROM players"
        df = pd.read_sql_query(query, conn)
        print(f"[SUCCESS] Loaded {len(df)} players from the database.")

        return df

    except Exception as e:
        print(f"[ERROR] Failed to load players: {e}")
        return pd.DataFrame()  # return empty DataFrame on failure

    finally:
        if conn:
            conn.close()
            print("[INFO] Database connection closed.")



    # Example usage:
    # match_data = parse_match_table(html)
    # for m in match_data:
    #     print(m)


#  function to generate the URL to scrape given a player's details
def generate_player_details_url(tm_player_id, tm_player_url_name, club_name, season='2025'):

    team_country = {
        'FC Cincinnati': 'MLS1',
        'Cesena FC': 'IT2',
        'New York City FC': 'MLS1',
        'Crystal Palace': 'GB1',
        'FC Augsburg': 'L1',
        'Vancouver Whitecaps FC': 'MLS1',
        'Charlotte FC': 'MLS1',
        'PSV Eindhoven': 'NL1',
        'Philadelphia Union': 'MLS1',
        'Orlando City SC': 'MLS1',
        'AFC Bournemouth': 'GB1',
        'Houston Dynamo FC': 'MLS1',
        'Seattle Sounders FC': 'MLS1',
        'San Diego FC': 'MLS1',
        'Columbus Crew': 'MLS1',
        'Real Salt Lake City': 'MLS1',
        'AC Milan': 'IT1',
        'Olympique Marseille': 'FR1',
        'CF América': 'MEXA',
        'AS Monaco': 'FR1',
        'Norwich City': 'GB2',
        'Southampton FC': 'GB2'
    }

    club_code = team_country[club_name]

    return f'{TM_BASE_URL}/{tm_player_url_name}/leistungsdatendetails/spieler/{tm_player_id}/wettbewerb/{club_code}/saison/{season}'


def parse_season_data(player, season='2025'):

    player_id = player.player_id.iloc[0]
    tm_player_id = player.tm_player_id.iloc[0]
    tm_player_url_name = player.tm_player_url_name.iloc[0]
    club_name = player.club_name.iloc[0]

    player_season_details_url = generate_player_details_url(tm_player_id, tm_player_url_name, club_name)

    r = requests.get(player_season_details_url, headers=HEADERS)
    if r.status_code != 200:
        print(f"Warning: Failed to fetch match {player_season_details_url}: {r.status_code}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    season_stats = []

    try:
        table = soup.find_all("div", class_="responsive-table")[1].find("table")
        tbody = table.find("tbody")

        rank_cells = False

        for row in tbody.find_all("tr"):
            cells = row.find_all("td")

            if not cells:
                continue

            if 'Match preview' in str(cells):
                continue

            # sometimes there is team rank info which can throw off the scraper. Ignore this for the match data
            pattern = r'\(\d+\.\)'

            # save an indicator that the rank info was there
            if len([c for c in cells if not re.search(pattern, c.text)]) != len(cells):
                rank_cells = True

            cells = [c for c in cells if not re.search(pattern, c.text)]

            try:
                # Matchday
                matchday = cells[0].text.strip()

                # Date
                date = cells[1].text.strip()

                # Venue
                venue = cells[2].text.strip()

                # Team name (for USMNT you might replace this later)
                team_tag = cells[3].find("a")
                team_name = team_tag["title"].strip() if team_tag else ""

                # Opponent name
                opponent_tag = cells[4].find("a")
                opponent_link = TM_BASE_URL + opponent_tag["href"]
                opponent_name = opponent_tag["title"] if opponent_tag else ""

                #  here is where table cells diverge if ranking info was present
                p = 5

                if not rank_cells:
                    p = p + 1 # if we've removed the rank cells, we've grabbed a little too much. adjust for that

                remaining_data = cells[p:]

                # Match report URL
                report_tag = remaining_data[0].find("a", class_="ergebnis-link")
                match_report_url = TM_BASE_URL + report_tag["href"] if report_tag else ""

                # Result
                result = report_tag.text.strip() if report_tag else ""

                # Position
                pos_tag = remaining_data[1].find("a")
                position = pos_tag["title"].strip() if pos_tag else ""

                # Goals, assists, cards, minutes
                goals = remaining_data[2].text.strip() if remaining_data[2].text.strip() else "0"
                assists = remaining_data[3].text.strip() if remaining_data[3].text.strip() else "0"
                yellow_cards = remaining_data[4].text.strip() if remaining_data[4].text.strip() else "0"
                second_yellow = remaining_data[5].text.strip() if remaining_data[5].text.strip() else "0"
                red_cards = remaining_data[6].text.strip() if remaining_data[6].text.strip() else "0"
                minutes_played = remaining_data[7].text.strip().replace("'", "") if remaining_data[7].text.strip() else "0"

                season_stats.append({
                    "player_id": player_id,
                    "season": season,
                    "matchday": matchday,
                    "date": date,
                    "venue": venue,
                    "team": team_name,
                    "opponent_name": opponent_name,
                    "opponent_link": opponent_link,
                    "match_report_url": match_report_url,
                    "result": result,
                    "position": position,
                    "goals": int(goals),
                    "assists": int(assists),
                    "yellow_cards": int(yellow_cards),
                    "second_yellow": int(second_yellow),
                    "red_cards": int(red_cards),
                    "minutes_played": minutes_played,
                    "last_updated": datetime.now().isoformat()
                })

            except Exception as e:
                print(f"Error parsing row: {e}")
                continue

    except:
        season_stats.append({
            "player_id": player_id,
            "season": 'unknown',
            "matchday":  'unknown',
            "date":  'unknown',
            "venue":  'unknown',
            "team":  'unknown',
            "opponent_name":  'unknown',
            "opponent_link":  'unknown',
            "match_report_url":  'unknown',
            "result":  'unknown',
            "position":  'unknown',
            "goals": None,
            "assists": None,
            "yellow_cards": None,
            "second_yellow": None,
            "red_cards": None,
            "minutes_played": None,
            "last_updated": datetime.now().isoformat()
        })

    season_stats = pd.DataFrame(season_stats)

    return season_stats


def save_player_stats_to_db(df: pd.DataFrame):
    """
    Save player stats to the database.

    Expected DataFrame columns:
    ['player_id', 'season', 'matchday', 'date', 'venue', 'team', 'opponent_name', 'opponent_link',
     'match_report_url', 'result', 'position', 'goals', 'assists', 'yellow_cards',
     'second_yellow', 'red_cards', 'minutes_played', 'last_updated']
    """
    print("[INFO] Connecting to database...")

    try:
        conn = get_connection()
        cursor = conn.cursor()
        print("[INFO] Connection established.")

        # # Enable foreign key constraints
        # cursor.execute("PRAGMA foreign_keys = ON;")

        # Prepare for UPSERT
        records = df.to_dict(orient="records")
        print(f"[INFO] Saving {len(records)} player match records...")

        cursor.executemany("""
            INSERT INTO player_stats
            (player_id, season, matchday, date, venue, team, opponent_name, opponent_link,
             match_report_url, result, position, goals, assists, yellow_cards,
             second_yellow, red_cards, minutes_played, last_updated)
            VALUES (:player_id, :season, :matchday, :date, :venue, :team, :opponent_name, :opponent_link,
                    :match_report_url, :result, :position, :goals, :assists, :yellow_cards,
                    :second_yellow, :red_cards, :minutes_played, :last_updated)
            ON CONFLICT(player_id, season, matchday, date)
            DO UPDATE SET
                venue = excluded.venue,
                team = excluded.team,
                opponent_name = excluded.opponent_name,
                opponent_link = excluded.opponent_link,
                match_report_url = excluded.match_report_url,
                result = excluded.result,
                position = excluded.position,
                goals = excluded.goals,
                assists = excluded.assists,
                yellow_cards = excluded.yellow_cards,
                second_yellow = excluded.second_yellow,
                red_cards = excluded.red_cards,
                minutes_played = excluded.minutes_played,
                last_updated = excluded.last_updated;
        """, records)

        conn.commit()
        print("[SUCCESS] Player stats saved/updated successfully.")

    except Exception as e:
        print(f"[ERROR] Failed to save match stats: {e}")

    finally:
        if conn:
            conn.close()
            print("[INFO] Database connection closed.")


def update_player_stats():
    """
    Main orchestration function to update all player stats.
    """
    print("Fetching player list...")
    data = load_players_from_db()

    print(f"Found {len(data)} players.")

    player_ids = data.player_id.unique()

    all_stats = pd.DataFrame()
    for i, id in enumerate(player_ids):

        if i > 0 and i % 5 == 0:
            time.sleep(55)

        player = data[data.player_id == id]

        print(player)
        stats = parse_season_data(player)

        # we need to sleep to avoid scraping detection
        sleep_duration = random.uniform(5, 20)
        print(f"Sleeping for {sleep_duration:.2f} seconds...")
        time.sleep(sleep_duration)
        print("Sleep finished.")

        all_stats = pd.concat([all_stats, stats])

    print(f"Saving {len(all_stats)} player stats to DB...")
    save_player_stats_to_db(all_stats)
    print("Player stats update complete.")
    all_stats.to_csv('player_stats.csv', index=False) # saves as a csv for easier online viewing. Not necessary for workflow


if __name__ == "__main__":
    update_player_stats()
