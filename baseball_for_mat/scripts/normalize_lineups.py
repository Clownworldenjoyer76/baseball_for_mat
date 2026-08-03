import pandas as pd
import unicodedata
import re
from pathlib import Path

# --- Normalization Helpers ---
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in text if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = strip_accents(name)
    name = re.sub(r"[^a-zA-Z.,' ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    suffixes = {"Jr", "Sr", "II", "III", "IV", "Jr.", "Sr."}
    tokens = name.replace(",", "").split()

    if len(tokens) >= 2:
        last_parts = [tokens[-1]]
        if tokens[-1].replace(".", "") in suffixes and len(tokens) >= 3:
            last_parts = [tokens[-2], tokens[-1]]
        first = " ".join(tokens[:-len(last_parts)])
        last = " ".join(last_parts)
        return f"{last.strip()}, {first.strip()}"

    return name.title()

# --- Main Normalization Logic ---
def normalize_lineups():
    INPUT_FILE = "data/raw/lineups.csv"
    OUTPUT_FILE = "data/raw/lineups_normalized.csv"
    SUMMARY_FILE = Path("summaries/summary.txt")

    df = pd.read_csv(INPUT_FILE)
    df.columns = [col.strip().lower() for col in df.columns]

    if 'last_name, first_name' not in df.columns and 'name' in df.columns:
        df.rename(columns={'name': 'last_name, first_name'}, inplace=True)

    df['last_name, first_name'] = df['last_name, first_name'].apply(normalize_name)
    df.dropna(subset=['last_name, first_name'], inplace=True)
    df.drop_duplicates(inplace=True)

    df.to_csv(OUTPUT_FILE, index=False)

    num_teams = df['team_code'].nunique() if 'team_code' in df.columns else 0
    num_players = len(df)

    with SUMMARY_FILE.open("a") as f:
        f.write(f"[normalize_lineups.py] ✅ {num_teams} teams, {num_players} players written to lineups_normalized.csv\n")

    print(f"✅ normalize_lineups.py completed: {num_players} rows written to {OUTPUT_FILE}")

if __name__ == "__main__":
    normalize_lineups()
