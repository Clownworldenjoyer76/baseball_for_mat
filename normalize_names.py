import pandas as pd
import unicodedata
from pathlib import Path

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return ''.join(c for c in normalized if not unicodedata.combining(c))

def normalize_name(name):
    return strip_accents(name).lower().strip()

def apply_normalization(filepath, name_column):
    df = pd.read_csv(filepath)
    df["normalized_name"] = df[name_column].apply(normalize_name)
    df.to_csv(filepath, index=False)
    print(f"✅ Normalized: {filepath}")

apply_normalization("data/master/batters.csv", "last_name, first_name")
apply_normalization("data/master/pitchers.csv", "last_name, first_name")
apply_normalization("data/processed/player_team_master.csv", "last_name, first_name")
