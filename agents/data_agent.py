"""
data_agent.py — responsible for tasks associated with querying the database or providing info about database tables

The four tools available to the data agent are:

Each tool is a plain Python function that returns a dict with:
  {
    "success": bool,
    "data": <result>,        # populated on success
    "error": <str>,          # populated on failure
  }

This uniform envelope means agent.py never has to handle tool-specific
error shapes — it always checks ["success"] first.

Adding a new tool: implement the function, add it to TOOL_REGISTRY at bottom.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any
import os
import sys


from dotenv import load_dotenv, find_dotenv

load_dotenv()


try:
    import db

except:
    load_dotenv(os.path.join(os.getcwd(), '.env'))
    current_path = os.getenv('USMNT_PROJECT_PATH')  # this allows for successful import when running locally
    parent_path = os.path.dirname(current_path)
    sys.path.append(parent_path)
    import db



# ── Tool: schema_lookup ────────────────────────────────────────────────────────

def schema_lookup(_input: dict | None = None) -> dict:
    """
    Fetch the live database schema and sample rows.
    Returns the formatted string used in prompts, plus the raw schema dict
    for any code that needs to inspect it programmatically.
    """
    try:
        schema = db.get_schema()
        formatted = db.format_schema_for_prompt(schema)
        return {
            "success": True,
            "data": {
                "formatted": formatted,
                "raw": schema,
            },
        }
    except Exception as e:
        return {"success": False, "data": None, "error": str(e)}


# ── Tool: run_sql ──────────────────────────────────────────────────────────────

def run_sql(input: dict) -> dict:
    """
    Execute a SELECT query and return up to MAX_RESULT_ROWS rows.

    Expected input: {"sql": "<SELECT ...>"}

    Safety layers (in order):
      1. Regex check for forbidden keywords (db.is_safe_query)
      2. SQLite read-only connection (URI mode, set in db.py)
      3. Row cap to prevent context overflow
    """
    sql = input.get("sql", "").strip()
    if not sql:
        return {"success": False, "data": None, "error": "No SQL provided"}

    safe, reason = db.is_safe_query(sql)
    if not safe:
        return {"success": False, "data": None, "error": f"Safety check failed: {reason}"}

    try:
        conn = db.get_read_only_connection()
        cursor = conn.execute(sql)
        rows = cursor.fetchmany(db.MAX_RESULT_ROWS)
        total_fetched = len(rows)

        # Convert sqlite3.Row objects to plain dicts
        result_rows = [dict(row) for row in rows]

        # Check if there were more rows than we returned
        extra = cursor.fetchone()
        was_capped = extra is not None

        conn.close()

        return {
            "success": True,
            "data": {
                "rows": result_rows,
                "row_count": total_fetched,
                "was_capped": was_capped,
                "cap_limit": db.MAX_RESULT_ROWS,
            },
        }

    except sqlite3.OperationalError as e:
        return {
            "success": False,
            "data": None,
            "error": f"SQL error: {e}",
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "error": f"Unexpected error: {e}",
        }


# ── Tool: compute_stats ────────────────────────────────────────────────────────

def compute_stats(input: dict) -> dict:
    """
    Run deterministic statistical operations on data already retrieved.
    This keeps math out of the LLM and ensures accuracy.

    Expected input:
      {
        "data": [ {col: val, ...}, ... ],   # list of row dicts from run_sql
        "operation": "correlation" | "describe" | "rank",
        "params": { ... }                   # operation-specific parameters
      }

    Operations:
      correlation:  Pearson r between two numeric columns.
                    params: {"col_a": str, "col_b": str}

      describe:     Summary stats (mean, median, std, min, max) for one column.
                    params: {"col": str}

      rank:         Sort rows by a column descending, return top N.
                    params: {"col": str, "n": int (default 10), "ascending": bool}
    """
    try:
        import statistics

        data = input.get("data", [])
        operation = input.get("operation", "")
        params = input.get("params", {})

        if not data:
            return {"success": False, "data": None, "error": "No data provided to compute_stats"}

        if operation == "correlation":
            col_a = params.get("col_a")
            col_b = params.get("col_b")
            if not col_a or not col_b:
                return {"success": False, "data": None, "error": "correlation requires col_a and col_b"}

            pairs = []
            for row in data:
                try:
                    a = float(row[col_a])
                    b = float(row[col_b])
                    pairs.append((a, b))
                except (TypeError, ValueError, KeyError):
                    continue  # skip rows with null/non-numeric values

            if len(pairs) < 3:
                return {
                    "success": False,
                    "data": None,
                    "error": f"Not enough valid numeric pairs to compute correlation (found {len(pairs)}, need >= 3)",
                }

            xs = [p[0] for p in pairs]
            ys = [p[1] for p in pairs]
            r = _pearson_r(xs, ys)

            return {
                "success": True,
                "data": {
                    "operation": "correlation",
                    "col_a": col_a,
                    "col_b": col_b,
                    "pearson_r": round(r, 4),
                    "n": len(pairs),
                    "interpretation": _interpret_r(r),
                },
            }

        elif operation == "describe":
            col = params.get("col")
            if not col:
                return {"success": False, "data": None, "error": "describe requires col"}

            values = []
            for row in data:
                try:
                    values.append(float(row[col]))
                except (TypeError, ValueError, KeyError):
                    continue

            if not values:
                return {"success": False, "data": None, "error": f"No numeric values found in column '{col}'"}

            return {
                "success": True,
                "data": {
                    "operation": "describe",
                    "col": col,
                    "n": len(values),
                    "mean": round(statistics.mean(values), 2),
                    "median": round(statistics.median(values), 2),
                    "stdev": round(statistics.stdev(values), 2) if len(values) > 1 else 0,
                    "min": round(min(values), 2),
                    "max": round(max(values), 2),
                },
            }

        elif operation == "rank":
            col = params.get("col")
            n = params.get("n", 10)
            ascending = params.get("ascending", False)

            if not col:
                return {"success": False, "data": None, "error": "rank requires col"}

            def sort_key(row):
                try:
                    return float(row[col])
                except (TypeError, ValueError):
                    return float("-inf") if not ascending else float("inf")

            sorted_rows = sorted(data, key=sort_key, reverse=not ascending)
            return {
                "success": True,
                "data": {
                    "operation": "rank",
                    "col": col,
                    "ascending": ascending,
                    "rows": sorted_rows[:n],
                },
            }

        else:
            return {
                "success": False,
                "data": None,
                "error": f"Unknown operation '{operation}'. Choose from: correlation, describe, rank",
            }

    except Exception as e:
        return {"success": False, "data": None, "error": f"compute_stats error: {e}"}


# ── Tool: clarify ──────────────────────────────────────────────────────────────

def clarify(input: dict) -> dict:
    """
    Ask the user a clarifying question and wait for their response.
    The agent should only use this tool when a question truly cannot be
    answered without more information — not as a default for ambiguity.

    Expected input: {"question": "<question to ask the user>"}
    """
    question = input.get("question", "").strip()
    if not question:
        return {"success": False, "data": None, "error": "No clarifying question provided"}

    print(f"\n  [Agent needs clarification]\n  {question}")
    print("  Your answer: ", end="", flush=True)
    user_response = input("").strip()

    return {
        "success": True,
        "data": {"clarification": user_response},
    }


# ── Statistical helpers ────────────────────────────────────────────────────────

def _pearson_r(xs: list[float], ys: list[float]) -> float:
    """Compute Pearson correlation coefficient without scipy dependency."""
    import statistics
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = statistics.mean(xs)
    mean_y = statistics.mean(ys)
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = sum((x - mean_x) ** 2 for x in xs) ** 0.5
    den_y = sum((y - mean_y) ** 2 for y in ys) ** 0.5
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


def _interpret_r(r: float) -> str:
    abs_r = abs(r)
    direction = "positive" if r > 0 else "negative"
    if abs_r >= 0.7:
        strength = "strong"
    elif abs_r >= 0.4:
        strength = "moderate"
    elif abs_r >= 0.2:
        strength = "weak"
    else:
        strength = "negligible"
    return f"{strength} {direction} correlation"


# ── Tool registry ──────────────────────────────────────────────────────────────
# Add new tools here — agent.py dispatches via this dict.

TOOL_REGISTRY: dict[str, callable] = {
    "schema_lookup": schema_lookup,
    "run_sql":       run_sql,
    "compute_stats": compute_stats,
    "clarify":       clarify,
}


def dispatch(tool_name: str, tool_input: dict) -> dict:
    """Route a tool call to its implementation. Returns tool result envelope."""
    if tool_name not in TOOL_REGISTRY:
        return {
            "success": False,
            "data": None,
            "error": f"Unknown tool '{tool_name}'. Available: {list(TOOL_REGISTRY.keys())}",
        }
    return TOOL_REGISTRY[tool_name](tool_input)