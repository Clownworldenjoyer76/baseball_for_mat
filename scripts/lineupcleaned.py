# scripts/lineupcleaned.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

LINEUPS_FILE = Path("data/raw/lineups.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
OUTPUT_FILE = Path("data/raw/lineups_clean.csv")
TARGET_COLUMN = "last_name, first_name"

# --- Normalization Utilities ---
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "").replace("`", "")
    name = re.sub(r"[^\w\s,]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = strip_accents(name)

    if "," not in name:
        parts = name.split()
        if len(parts) >= 2:
            return f"{parts[1]}, {parts[0]}"
        return name.title()
    
    last, first = map(str.strip, name.split(",", 1))
    return f"{last.title()}, {first.title()}"

# --- Main Execution ---
def main():
    if not LINEUPS_FILE.exists() or not MASTER_FILE.exists():
        raise FileNotFoundError("One or both required files are missing.")

    lineups_df = pd.read_csv(LINEUPS_FILE)
    master_df = pd.read_csv(MASTER_FILE)

    if TARGET_COLUMN not in lineups_df.columns or TARGET_COLUMN not in master_df.columns:
        raise ValueError(f"Missing column '{TARGET_COLUMN}' in input files.")

    lineups_df["normalized_name"] = lineups_df[TARGET_COLUMN].apply(normalize_name)
    master_df["normalized_name"] = master_df[TARGET_COLUMN].apply(normalize_name)

    match_map = dict(zip(master_df["normalized_name"], master_df[TARGET_COLUMN]))
    lineups_df[TARGET_COLUMN] = lineups_df["normalized_name"].map(match_map).fillna(lineups_df[TARGET_COLUMN])
    lineups_df.drop(columns=["normalized_name"], inplace=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    lineups_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Normalized lineups file saved: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
