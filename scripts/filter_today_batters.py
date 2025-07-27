import pandas as pd
from pathlib import Path
import unicodedata
import re

LINEUPS_FILE = "data/cleaned/lineups_cleaned.csv" # Updated to new file
BATTERS_FILE = "data/cleaned/batters_normalized_cleaned.csv"
OUTPUT_FILE = "data/cleaned/batters_today.csv"
UNMATCHED_FILE = "data/cleaned/unmatched_batters.txt"

# --- Utility Functions ---
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    if "," not in name:
        tokens = name.split()
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return name.title()

    parts = name.split(",")
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = parts[1].strip().title()
        return f"{last}, {first}"

    return name.title()

# --- Manual Overrides for Known Variants ---
NAME_OVERRIDES = {
    "Tatis Jr., Fernando": "Tatis, Fernando",
    "Witt Jr., Bobby": "Witt, Bobby",
    "P Muncy, Max": "Muncy, Max",
    "V Garcia, Luis": "Garcia, Luis",
    "O'Hearn, Ryan": "Ohearn, Ryan",
    "O'Hoppe, Logan": "Ohoppe, Logan",
    "Crow-Armstrong, Pete": "Crowarmstrong, Pete",
    "Encarnacion-Strand, Christian": "Encarnacionstrand, Christian",
    "Kiner-Falefa, Isiah": "Kinerfalefa, Isiah",
    "De La Cruz, Elly": "De La Cruz, Elly",  # already proper if exists
}

# --- Main ---
def main():
    print("📥 Loading lineups and batters...")
    try:
        lineups_df = pd.read_csv(LINEUPS_FILE)
        batters_df = pd.read_csv(BATTERS_FILE)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load input files: {e}")

    # Updated column for matching in lineups_df
    lineups_match_column = 'normalized_full_name_for_match'

    if lineups_match_column not in lineups_df.columns or 'name' not in batters_df.columns:
        raise ValueError(f"❌ Missing required columns. Ensure '{lineups_match_column}' is in lineups file and 'name' in batters file.")

    # No need to normalize lineups_df names, as they should already be normalized
    # based on the new column name.
    # We still need to normalize batter names.
    batters_df['normalized_name'] = batters_df['name'].astype(str).apply(normalize_name)

    expected_names_set = set(lineups_df[lineups_match_column].astype(str))
    filtered = batters_df[batters_df['normalized_name'].isin(expected_names_set)].copy()
    unmatched = sorted(expected_names_set - set(filtered['normalized_name']))

    print(f"✅ Filtered down to {len(filtered)} batters")
    if unmatched:
        print(f"⚠️ {len(unmatched)} unmatched batters found. Writing to {UNMATCHED_FILE}")
        Path(UNMATCHED_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(UNMATCHED_FILE).write_text("\n".join(unmatched))
    else:
        print("✅ All lineup batters matched successfully.")

    filtered.drop(columns=['normalized_name'], errors='ignore', inplace=True)
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved filtered batters to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
