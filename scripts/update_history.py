# update_history.py
import pandas as pd
from pathlib import Path
from config import PLAYER_HISTORY_FILE, GAME_HISTORY_FILE

def append_to_history(df: pd.DataFrame, file_path: str):
    path = Path(file_path)
    if path.exists():
        existing = pd.read_csv(path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    combined.to_csv(path, index=False)

def update_player_history(df):
    append_to_history(df, PLAYER_HISTORY_FILE)

def update_game_history(df):
    append_to_history(df, GAME_HISTORY_FILE)
