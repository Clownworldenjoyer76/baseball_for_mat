import pandas as pd
from pathlib import Path
import unicodedata
import re

LINEUPS_FILE = "data/cleaned/lineups_cleaned.csv"
BATTERS_FILE = "data/cleaned/batters_normalized_cleaned.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_FILE = "data/cleaned/batters_today.csv"
UNMATCHED_FILE = "data/cleaned/unmatched_batters.txt"
PITCHERS_IN_LINEUPS_FILE = "data/cleaned/pitchers_in_lineups.txt"

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
    name = re.sub(r'\bJ\.P\.', 'JP', name, flags=re.IGNORECASE)
    name = re.sub(r'\bJ\.T\.', 'JT', name, flags=re.IGNORECASE)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    full_to_initials_mapping = {
        "john paul": "JP",
        "jacob": "JT",
    }

    last_name = ""
    first_name = ""

    if "," in name:
        parts = name.split(",")
        if len(parts) == 2:
            last_name = parts[0].strip()
            first_name = parts[1].strip()
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            first_name = tokens[0]
            last_name = " ".join(tokens[1:])
        else:
            return name.title()

    mapped_first_name = full_to_initials_mapping.get(first_name.lower(), first_name)
    final_first_name = mapped_first_name.upper() if mapped_first_name in ["JP", "JT"] else mapped_first_name.title()
    final_last_name = last_name.title()

    return f"{final_last_name}, {final_first_name}"

# --- Manual Overrides ---
NAME_OVERRIDES = {
    "Tatis Jr., Fernando": "Tatis, Fernando",
    "Witt Jr., Bobby": "Witt, Bobby",
    "Crawford, John Paul": "Crawford, Jp",
    "Crawford, JP": "Crawford, Jp",
    "Realmuto, JT": "Realmuto, Jt",
    "Realmuto, Jacob": "Realmuto, Jt",
    "P Muncy, Max": "Muncy, Max",
    "V Garcia, Luis": "Garcia, Luis",
    "O'Hearn, Ryan": "Ohearn, Ryan",
    "O'Hoppe, Logan": "Ohoppe, Logan",
    "Crow-Armstrong, Pete": "Crowarmstrong, Pete",
    "Encarnacion-Strand, Christian": "Encarnacionstrand, Christian",
    "Kiner-Falefa, Isiah": "Kinerfalefa, Isiah",
    "De La Cruz, Elly": "De La Cruz, Elly",
}

# --- Main ---
def main():
    print("📥 Loading lineups, batters, and pitchers...")
    try:
        lineups_df = pd.read_csv(LINEUPS_FILE)
        batters_df = pd.read_csv(BATTERS_FILE)
        pitchers_df = pd.read_csv(PITCHERS_FILE)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load input files: {e}")

    lineups_match_column = 'normalized_full_name_for_match'

    if lineups_match_column not in lineups_df.columns or \
       'name' not in batters_df.columns or \
       'name' not in pitchers_df.columns:
        raise ValueError(
            f"❌ Missing required columns. Ensure '{lineups_match_column}' is in lineups file, "
            f"'name' in batters file, and 'name' in pitchers file."
        )

    # 🛠 Apply manual name overrides before normalization
    batters_df['name'] = batters_df['name'].replace(NAME_OVERRIDES)
    pitchers_df['name'] = pitchers_df['name'].replace(NAME_OVERRIDES)

    # Normalize all names
    batters_df['normalized_name'] = batters_df['name'].astype(str).apply(normalize_name)
    pitchers_df['normalized_name'] = pitchers_df['name'].astype(str).apply(normalize_name)
    lineups_df['final_match_name'] = lineups_df[lineups_match_column].astype(str).apply(normalize_name)

    expected_names_from_lineups = set(lineups_df['final_match_name'])
    pitcher_names_set = set(pitchers_df['normalized_name'])

    pitchers_in_lineups = expected_names_from_lineups.intersection(pitcher_names_set)
    batters_for_matching = expected_names_from_lineups - pitcher_names_set

    print(f"✅ Identified {len(pitchers_in_lineups)} pitchers in the lineup (based on {PITCHERS_FILE}).")
    if pitchers_in_lineups:
        Path(PITCHERS_IN_LINEUPS_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(PITCHERS_IN_LINEUPS_FILE).write_text("\n".join(sorted(pitchers_in_lineups)))
        print(f"📝 Saved identified pitchers to {PITCHERS_IN_LINEUPS_FILE}")

    filtered = batters_df[batters_df['normalized_name'].isin(batters_for_matching)].copy()
    unmatched_batters = sorted(batters_for_matching - set(filtered['normalized_name']))

    print(f"✅ Filtered down to {len(filtered)} batters (excluding identified pitchers)")
    if unmatched_batters:
        print(f"⚠️ {len(unmatched_batters)} unmatched batters found (after removing pitchers). Writing to {UNMATCHED_FILE}")
        Path(UNMATCHED_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(UNMATCHED_FILE).write_text("\n".join(unmatched_batters))
    else:
        print("✅ All intended lineup batters matched successfully.")

    filtered.drop(columns=['normalized_name'], errors='ignore', inplace=True)
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved filtered batters to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
