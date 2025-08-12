# scripts/update_history.py
import pandas as pd
from pathlib import Path
from logging_utils import log
from config import (
    PLAYER_HISTORY_FILE, GAME_HISTORY_FILE,
    PLAYER_HISTORY_COLUMNS, GAME_HISTORY_COLUMNS,
)

def _ensure_parent(p: Path): p.parent.mkdir(parents=True, exist_ok=True)

def _align(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df is None: return pd.DataFrame(columns=columns)
    if df.empty and len(df.columns) == 0:
        return pd.DataFrame(columns=columns)
    return df.reindex(columns=columns, fill_value="")

def _write_or_append(df: pd.DataFrame, path: Path, columns: list[str]):
    _ensure_parent(path)
    df = _align(df, columns)

    if path.exists():
        try:
            existing = pd.read_csv(path)
        except Exception:
            existing = pd.DataFrame(columns=columns)
        existing = _align(existing, columns)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df

    if combined.empty:
        pd.DataFrame(columns=columns).to_csv(path, index=False)
        log(f"{path} -> wrote headers only")
    else:
        combined.to_csv(path, index=False)
        log(f"{path} -> wrote {len(df)} new row(s), total {len(combined)}")

def update_player_history(df: pd.DataFrame):
    _write_or_append(df, Path(PLAYER_HISTORY_FILE), PLAYER_HISTORY_COLUMNS)

def update_game_history(df: pd.DataFrame):
    _write_or_append(df, Path(GAME_HISTORY_FILE), GAME_HISTORY_COLUMNS)i
