import pandas as pd
from pathlib import Path
from datetime import date
from logging_utils import log

# File paths (adjust if different in your repo)
PLAYER_HISTORY_FILE = Path("data/history/player_props_history.csv")
GAME_HISTORY_FILE = Path("data/history/game_props_history.csv")

# Required column sets
PLAYER_HISTORY_COLUMNS = [
    "player_id", "name", "team", "prop", "line", "value",
    "over_probability", "date", "game_id", "prop_correct", "prop_sort"
]

GAME_HISTORY_COLUMNS = [
    "game_id", "date", "home_team", "away_team", "venue_name",
    "favorite", "favorite_correct", "projected_real_run_total",
    "actual_real_run_total", "run_total_diff", "home_score", "away_score",
    "game_time", "pitcher_home", "pitcher_away",
    "proj_home_score", "proj_away_score"
]

def _ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def _align(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    # Ensure DataFrame has all required columns
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df[columns]

def _write_today_only(df: pd.DataFrame, path: Path, columns: list[str]):
    """Write only today's rows to the history file (overwrite)."""
    _ensure_parent(path)
    df = _align(df, columns)

    # Filter for today's date
    if "date" in df.columns:
        today_str = str(date.today())
        df = df[df["date"] == today_str]

    if df.empty:
        # Write only headers if nothing matches today
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        log(f"{path} -> wrote headers only (no rows for today)")
    else:
        df.to_csv(path, index=False)
        log(f"{path} -> wrote {len(df)} row(s) for today")

def update_player_history(df: pd.DataFrame):
    _write_today_only(df, PLAYER_HISTORY_FILE, PLAYER_HISTORY_COLUMNS)

def update_game_history(df: pd.DataFrame):
    _write_today_only(df, GAME_HISTORY_FILE, GAME_HISTORY_COLUMNS)
