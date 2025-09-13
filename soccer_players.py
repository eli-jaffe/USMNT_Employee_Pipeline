import requests
from bs4 import BeautifulSoup
import hashlib

import os
import sys

# this enables imports from other folders in parent directory
current_path = os.path.dirname(os.path.realpath(__file__))
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

# from db import get_connection

from Personal_Research.Jobs.USMNT_Employee_Pipeline.db import get_connection
from datetime import datetime
import pandas as pd
import logging

# TRANSFERMARKT_USA = "https://www.transfermarkt.com/usa/aufstellung/verein/3434"  # placeholder
TRANSFERMARKT_USA = "https://www.transfermarkt.us/vereinigte-staaten/startseite/verein/3505"


def generate_player_id(name, dob):
    return hashlib.md5(f"{name}-{dob}".encode()).hexdigest()


def get_us_players():
    r = requests.get(TRANSFERMARKT_USA, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(r.text, "html.parser")

    players = []

    table = soup.find("table", {"class": "items"})
    rows = table.find_all("tr", {"class": ["odd", "even"]})

    for row in rows:
        name_tag = row.find("td", {"class": "hauptlink"})
        if name_tag:
            full_name = name_tag.text.strip()
            # store transfermarkt profile link and player id
            tm_profile_link = row.find_all("a")[0].get('href')
            tm_player_id = tm_profile_link.split('/')[-1]
            tm_player_url_name = tm_profile_link.split('/')[1] # gets the player's url trunk (to be used for finding match stats
            # dob_tag = row.find("td", {"class": "zentriert"}).text.strip()
            position = row.find_all("td")[4].text.strip()

            dob_info = row.find_all("td")[5].text.strip()

            # Remove the final parentheses
            clean_dob = dob_info.split(' (')[0]
            # Parse into a datetime object
            date_obj = datetime.strptime(clean_dob, "%b %d, %Y")
            # Convert to yyyy-mm-dd format as string
            dob = date_obj.strftime("%Y-%m-%d")

            club_name = row.find_all("a")[1].get("title")

            player_id = generate_player_id(full_name, dob)

            players.append({
                "player_id": player_id,
                "full_name": full_name,
                "tm_profile_link": tm_profile_link,
                "tm_player_id":  tm_player_id,
                "tm_player_url_name": tm_player_url_name,
                "birth_date": dob,
                "position": position,
                "club_name": club_name,
                "club_country": "",  # optional: scrape from club page
                "last_updated": datetime.now().isoformat()
            })

    players = pd.DataFrame(players)

    return players


def save_players_to_db(df: pd.DataFrame):
    """
    Save a pandas DataFrame of player data to the SQLite database using get_connection().

    Args:
        df (pd.DataFrame): DataFrame containing player data with columns:
            ['player_id', 'full_name', 'birth_date', 'position', 'club_name', 'club_country', 'last_updated']
    """
    logging.info("Connecting to the database...")

    print("[INFO] Establishing database connection...")
    conn = get_connection()
    c = conn.cursor()
    print("[INFO] Connection established.")

    try:
        logging.info(f"Saving {len(df)} players to the database...")

        # records = df.to_dict(orient="records")
        print(f"[INFO] Saving {len(df)} player records to database...")

        for idx, row in df.iterrows():
            c.execute("""
            INSERT OR REPLACE INTO players 
            (player_id, full_name, tm_profile_link, tm_player_id, tm_player_url_name, birth_date, position, club_name, club_country, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["player_id"],
                row["full_name"],
                row["tm_profile_link"],
                row["tm_player_id"],
                row["tm_player_url_name"],
                row.get("birth_date"),
                row.get("position"),
                row.get("club_name"),
                row.get("club_country"),
                row.get("last_updated")
            ))
        if (idx + 1) % 10 == 0 or (idx + 1) == len(df):
            logging.info(f"{idx + 1}/{len(df)} players saved...")

        # Use executemany for batch inserts
        # c.executemany("""
        #     INSERT OR REPLACE INTO players
        #     (player_id, full_name, tm_profile_link, tm_player_id, tm_player_url_name, birth_date, position,
        #      club_name, club_country, last_updated)
        #     VALUES (:player_id, :full_name, :tm_profile_link, :tm_player_id, :tm_player_url_name, :birth_date, :position, :club_name, :club_country, :last_updated)
        # """, records)

        conn.commit()
        print("[SUCCESS] All records saved successfully!")
        logging.info("All players saved successfully.")

    except Exception as e:
        print(f"[ERROR] Failed to save players: {e}")
        logging.exception("Error saving players to the database.")

    finally:
        if conn:
            conn.close()
            print("[INFO] Database connection closed.")

        logging.info("Database connection closed.")


if __name__ == "__main__":
    players = get_us_players()
    save_players_to_db(players)
    print(f"Saved {len(players)} players")
