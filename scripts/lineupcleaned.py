# scripts/lineupcleaned.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

# --- File Paths ---
LINEUPS_IN = Path("data/raw/lineups.csv")
MASTER_IN = Path("data/processed/player_team_master.csv")
LINEUPS_OUT = Path("data/raw/lineups_clean.csv")
TARGET_COLUMN = "last_name, first_name"

# --- Regex Patterns ---
RE_NON_ALPHANUM_OR_SPACE_OR_COMMA = re.compile(r"[^\w\s,]")
RE_MULTI_SPACE = re.compile(r"\s+")

# --- Normalization Helpers ---
def strip_accents(text: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name: str) -> str:
    name = strip_accents(name)
    name = name.replace("’", "").replace("`", "")
    name = RE_NON_ALPHANUM_OR_SPACE_OR_COMMA.sub("", name)
    name = RE_MULTI_SPACE.sub(" ", name).strip()
    return name

# --- Main Logic ---
def main():
    print("📥 Loading lineups and master file...")
    df = pd.read_csv(LINEUPS_IN)
    master = pd.read_csv(MASTER_IN)

    # Normalize columns
    df.columns = df.columns.str.strip()
    master.columns = master.columns.str.strip()

    if TARGET_COLUMN not in df.columns or TARGET_COLUMN not in master.columns:
        raise ValueError(f"Missing column '{TARGET_COLUMN}' in input files.")

    # Normalize player names
    df[TARGET_COLUMN] = df[TARGET_COLUMN].astype(str).apply(normalize_name)
    master[TARGET_COLUMN] = master[TARGET_COLUMN].astype(str).apply(normalize_name)

    # Keep only rows with matching normalized names
    allowed_names = set(master[TARGET_COLUMN])
    df_clean = df[df[TARGET_COLUMN].isin(allowed_names)].copy()

    # Save cleaned output
    LINEUPS_OUT.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(LINEUPS_OUT, index=False)
    print(f"✅ Saved cleaned lineups to {LINEUPS_OUT} with {len(df_clean)} rows")

if __name__ == "__main__":
    main()
