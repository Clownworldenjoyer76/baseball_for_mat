# scripts/update_history.py
import pandas as pd
from pathlib import Path
from logging_utils import log

# Default locations (override via config if you have it)
try:
    from config import PLAYER_HISTORY_FILE, GAME_HISTORY_FILE
except Exception:
    PLAYER_HISTORY_FILE = "data/bets/player_props_history.csv"
    GAME_HISTORY_FILE = "data/bets/game_props_history.csv"

def _ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def _write_or_append(df: pd.DataFrame, path: Path):
    _ensure_parent(path)
    if df is None:
        log(f"None dataframe for {path} -> skip")
        return

    # If empty df: create file with header (if columns exist) so git can track it
    if df.empty:
        if not path.exists():
            log(f"{path} empty DataFrame -> creating file with header only")
            df.to_csv(path, index=False)
        else:
            log(f"{path} empty DataFrame -> no new rows to append")
        return

    if path.exists():
        try:
            existing = pd.read_csv(path)
            combined = pd.concat([existing, df], ignore_index=True)
        except Exception as e:
            log(f"Failed reading existing {path}: {e}; writing fresh")
            combined = df
    else:
        combined = df

    combined.to_csv(path, index=False)
    log(f"Wrote {len(df)} new row(s) to {path} (total {len(combined)})")

def update_player_history(df: pd.DataFrame):
    _write_or_append(df, Path(PLAYER_HISTORY_FILE))

def update_game_history(df: pd.DataFrame):
    _write_or_append(df, Path(GAME_HISTORY_FILE))
