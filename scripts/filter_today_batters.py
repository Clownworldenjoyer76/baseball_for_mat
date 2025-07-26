import pandas as pd
from pathlib import Path
import unicodedata
import re

LINEUPS_FILE = "data/raw/lineups.csv"
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

    if "," in name:
        parts = [p.strip().title() for p in name.split(",")]
        if len(parts) >= 2:
            return f"{parts[0]}, {parts[1]}"
        return ' '.join(parts).title()
    else:
        tokens = [t.title() for t in name.split()]
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return ' '.join(tokens).title()

# --- Main ---
def main():
    print("📥 Loading lineups and batters...")
    try:
        lineups_df = pd.read_csv(LINEUPS_FILE)
        batters_df = pd.read_csv(BATTERS_FILE)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load input files: {e}")

    if 'last_name, first_name' not in lineups_df.columns or 'name' not in batters_df.columns:
        raise ValueError("❌ Missing required columns in either lineups or batters file.")

    # Normalize names
    lineups_df['normalized_name'] = lineups_df['last_name, first_name'].astype(str).apply(normalize_name)
    batters_df['normalized_name'] = batters_df['name'].astype(str).apply(normalize_name)

    expected_names_set = set(lineups_df['normalized_name'])
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
