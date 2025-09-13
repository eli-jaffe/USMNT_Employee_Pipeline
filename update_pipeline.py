# update_pipeline.py
import logging
from datetime import datetime
from soccer_players import get_us_players, save_players_to_db
from stats import update_player_stats

# -----------------------------
# Logging configuration
# -----------------------------
LOG_FILE = ./logs/update_pipeline.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def job():
    logging.info("Starting USMNT update pipeline.")

    try:
        # 1️⃣ Update players
        logging.info("Fetching latest player data...")
        players = get_us_players()
        save_players_to_db(players)
        logging.info(f"Saved {len(players)} players to the database.")

        # 2️⃣ Update stats
        logging.info("Updating player match stats...")
        update_player_stats()
        logging.info("Player stats updated successfully.")

    except Exception as e:
        logging.exception("Error occurred during update pipeline.")
    else:
        logging.info("USMNT update pipeline completed successfully.\n")


if __name__ == "__main__":
    job()
