"""
data_agent_tools.py — SQLite connection manager

Responsibilities:
- Provide a read-only connection guard (no INSERT/UPDATE/DELETE ever executes)
- Expose schema introspection used by the schema_lookup tool
- Centralize all database configuration so adding new tables requires zero changes elsewhere
"""

from __future__ import annotations

import os
import sys
import sqlite3
import re
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

load_dotenv()

# this enables imports from other folders in parent directory
try:
    current_path = os.path.dirname(os.path.realpath(__file__))

except:
    load_dotenv(os.path.join(os.getcwd(), '.env'))
    current_path = os.getenv('USMNT_PROJECT_PATH')  # this allows for successful import when running locally
parent_path = os.path.dirname(current_path)
sys.path.append(parent_path)

try:
    from db import get_read_only_connection

except:
    load_dotenv(os.path.join(os.getcwd(), '.env'))
    current_path = os.getenv('USMNT_PROJECT_PATH')  # this allows for successful import when running locally
    parent_path = os.path.dirname(current_path)
    sys.path.append(parent_path)
    from db import get_read_only_connection


# ── Configuration ─────────────────────────────────────────────────────────────
#
# DB_PATH = Path(__file__).parent / "us_soccer.db"
# DB_NAME = "us_soccer.db"

# Hard cap on rows returned to the LLM to avoid blowing the context window.
# Increase if you find complex aggregations need more rows to synthesize.
MAX_RESULT_ROWS = 100

# SQL operations that are never permitted, regardless of context
_FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|TRUNCATE|ATTACH)\b",
    re.IGNORECASE,
)

# ── Connection ─────────────────────────────────────────────────────────────────

# def get_connection() -> sqlite3.Connection:
#     """
#     Return a sqlite3 connection in read-only URI mode.
#     Using URI mode at the driver level means even a prompt-injected
#     write statement will be rejected by SQLite itself, not just our regex.
#     """
#     # if not DB_PATH.exists():
#     #     raise FileNotFoundError(f"Database not found at {DB_PATH}")
#     # uri = f"file:{DB_PATH}?mode=ro"
#     # conn = sqlite3.connect(uri, uri=True)
#     conn = sqlite3.connect(parent_path  + '/' + DB_NAME)
#     conn.row_factory = sqlite3.Row  # lets us access columns by name
#     return conn


# ── Safety guard ──────────────────────────────────────────────────────────────

def is_safe_query(sql: str) -> tuple[bool, str]:
    """
    Pre-flight check before any query reaches SQLite.
    Returns (is_safe, reason_if_not).
    """
    if _FORBIDDEN_PATTERN.search(sql):
        matched = _FORBIDDEN_PATTERN.search(sql).group()
        return False, f"Query contains forbidden operation: {matched}"
    if not sql.strip().upper().startswith("SELECT"):
        return False, "Only SELECT statements are permitted"
    return True, ""


# ── Schema introspection ───────────────────────────────────────────────────────
def generate_table_profile_sql(table_name, columns):
    # """
    # Generates an optimized SQL profiling statement using a CTE
    # to handle the total row count efficiently.
    # """
    # queries = []
    #
    # for col in columns:
    #     query = f"""
    #     SELECT
    #         '{col}' AS column_name,
    #         (SELECT total FROM table_stats) AS total_table_rows,
    #         COUNT(DISTINCT "{col}") AS distinct_count,
    #         SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS null_count,
    #         ROUND(100.0 * SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) / (SELECT total FROM table_stats), 2) AS percent_null,
    #         CAST(MIN("{col}") AS TEXT) AS min_val,
    #         CAST(MAX("{col}") AS TEXT) AS max_val,
    #         GROUP_CONCAT(DISTINCT typeof("{col}")) AS actual_types
    #     FROM {table_name}"""
    #     queries.append(query)
    #
    # # Wrap everything in a CTE so the count happens only once
    # final_sql = f"""
    # WITH table_stats AS (
    #     SELECT COUNT(*) AS total FROM {table_name}
    # )
    # """ + "\nUNION ALL".join(queries) + ";"
    #
    # return final_sql
    """
        Generates a SQL profiling statement with one row per column,
        including total rows, distinct counts, and the first 3 distinct examples.
        """
    queries = []

    for col in columns:
        # Subquery to get the first 3 distinct non-null values
        examples_sq = f"""(
                SELECT GROUP_CONCAT(val) 
                FROM (
                    SELECT DISTINCT "{col}" AS val 
                    FROM {table_name} 
                    WHERE "{col}" IS NOT NULL 
                        AND "{col}"  != 'unknown'
                    LIMIT 3
                )
            )"""

        query = f"""
    SELECT 
        '{col}' AS column_name,
        (SELECT total FROM table_stats) AS total_table_rows,
        COUNT(DISTINCT "{col}") AS distinct_count,
        SUM(CASE WHEN "{col}" IS NULL THEN 1 WHEN "{col}" = 'unknown' THEN 1 ELSE 0 END) AS null_count,
        ROUND(100.0 * SUM(CASE WHEN "{col}" IS NULL THEN 1 WHEN "{col}" = 'unknown' THEN 1  ELSE 0 END) / (SELECT total FROM table_stats), 2) AS percent_null,
        CAST(MIN("{col}") AS TEXT) AS min_val,
        CAST(MAX("{col}") AS TEXT) AS max_val,
        GROUP_CONCAT(DISTINCT typeof("{col}")) AS actual_types,
        {examples_sq} AS first_3_examples
    FROM {table_name}"""
        queries.append(query)

    final_sql = f"""WITH table_stats AS (SELECT COUNT(*) AS total FROM {table_name})\n"""
    final_sql += "\nUNION ALL".join(queries) + ";"

    return final_sql


def get_schema() -> dict[str, dict]:
    """
    Return a dictionary of all user tables in the database with:
      - columns: list of {name, type, notnull, pk, ...profile fields}
      - sample: up to 3 rows as list-of-dicts (for prompt grounding)
      - row_count: approximate total rows

    This function is intentionally dynamic — adding a new table to the database
    automatically surfaces it to the agent without any code changes here.
    """
    schema = {}
    conn = get_read_only_connection()

    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()

        for (table_name,) in tables:
            # Column metadata
            cols_raw = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            columns = [
                {
                    "name": row["name"],
                    "type": row["type"],
                    "notnull": bool(row["notnull"]),
                    "pk": bool(row["pk"]),
                }
                for row in cols_raw
            ]

            # Profile metadata — one row per column, keyed by column_name for the join
            col_names = [col["name"] for col in columns]
            profile_sql = generate_table_profile_sql(table_name, col_names)
            profile_rows = conn.execute(profile_sql).fetchall()
            profile_by_col = {row["column_name"]: dict(row) for row in profile_rows}

            # Merge profile into columns on name == column_name
            PROFILE_FIELDS = (
                "distinct_count", "null_count", "percent_null",
                "min_val", "max_val", "actual_types", "first_3_examples"
            )
            for col in columns:
                prof = profile_by_col.get(col["name"], {})
                for field in PROFILE_FIELDS:
                    col[field] = prof.get(field)

            # Sample rows
            sample_rows = conn.execute(
                f"SELECT * FROM {table_name} LIMIT 3"
            ).fetchall()
            sample = [dict(row) for row in sample_rows]

            # Row count
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

            schema[table_name] = {
                "columns": columns,
                "sample": sample,
                "row_count": count,
            }

    finally:
        conn.close()

    return schema


def format_schema_for_prompt(schema: dict) -> str:
    """
    Render schema dict into a compact, LLM-readable string.
    Example output:

        TABLE: player_stats  (4932 rows)
        Columns:
          player_id       TEXT [PK]  | distinct: 312  nulls: 0 (0.0%)   min: aaron-long          max: zack-steffen        types: text    examples: pulisic-christian, dest-sergiño, adams-tyler
          season          TEXT       | distinct: 6    nulls: 0 (0.0%)   min: 2018                max: 2023                types: text    examples: 2018, 2019, 2020
          ...
        Sample rows:
          {"player_id": "pulisic-christian", ...}
    """
    lines = []
    for table_name, info in schema.items():
        lines.append(f"TABLE: {table_name}  ({info['row_count']} rows)")
        lines.append("Columns:")
        for col in info["columns"]:
            # --- base metadata ---
            pk_marker = " [PK]" if col["pk"] else ""
            null_marker = " NOT NULL" if col["notnull"] else ""
            base = f"  {col['name']:<25} {col['type']}{pk_marker}{null_marker}"

            # --- profile metadata (may be None if table was empty) ---
            distinct   = col.get("distinct_count")
            nulls      = col.get("null_count")
            pct_null   = col.get("percent_null")
            min_val    = col.get("min_val")
            max_val    = col.get("max_val")
            types      = col.get("actual_types")
            examples   = col.get("first_3_examples")

            if distinct is not None:
                profile = (
                    f" | distinct: {distinct:<6}"
                    f" nulls: {nulls} ({pct_null}%)"
                    f"   min: {str(min_val):<20}"
                    f" max: {str(max_val):<20}"
                    f" types: {str(types):<8}"
                    f" examples: {examples}"
                )
            else:
                profile = " | (no profile data)"

            lines.append(base + profile)

        if info["sample"]:
            lines.append("Sample rows:")
            for row in info["sample"]:
                lines.append(f"  {row}")
        lines.append("")
    return "\n".join(lines)