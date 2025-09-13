import sqlite3
from datetime import datetime

DB_NAME = "us_soccer.db"


def get_connection():
    conn = sqlite3.connect(DB_NAME)
    print(f'Connection {conn} created')
    return conn


def setup_database():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id TEXT PRIMARY KEY,
        full_name TEXT,
        tm_profile_link TEXT,
        tm_player_id TEXT,
        tm_player_url_name TEXT,
        birth_date TEXT,
        position TEXT,
        club_name TEXT,
        club_country TEXT,
        last_updated TEXT
    )
    """)

    # c.execute("DROP TABLE IF EXISTS player_stats")
    c.execute("""
    CREATE TABLE player_stats (
        player_id TEXT,
        season TEXT,
        matchday TEXT,
        date TEXT,
        venue TEXT,
        team TEXT,
        opponent_name TEXT,
        opponent_link TEXT,
        match_report_url TEXT,
        result TEXT,
        position TEXT,
        goals INTEGER,
        assists INTEGER,
        yellow_cards INTEGER,
        second_yellow INTEGER,
        red_cards INTEGER,
        minutes_played INTEGER,
        last_updated TEXT,
        UNIQUE(player_id, season, matchday, date)
    )
    """)

    conn.commit()
    conn.close()