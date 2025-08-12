# scripts/process_data.py
import pandas as pd
from config import PLAYER_HISTORY_COLUMNS, GAME_HISTORY_COLUMNS

def process_player_data(raw):
    if not raw:
        return pd.DataFrame(columns=PLAYER_HISTORY_COLUMNS)
    df = pd.DataFrame(raw)
    if df.empty:
        return pd.DataFrame(columns=PLAYER_HISTORY_COLUMNS)
    return df.reindex(columns=PLAYER_HISTORY_COLUMNS, fill_value="")

def process_game_data(raw):
    if not raw:
        return pd.DataFrame(columns=GAME_HISTORY_COLUMNS)
    df = pd.DataFrame(raw)
    if df.empty:
        return pd.DataFrame(columns=GAME_HISTORY_COLUMNS)
    return df.reindex(columns=GAME_HISTORY_COLUMNS, fill_value="")
