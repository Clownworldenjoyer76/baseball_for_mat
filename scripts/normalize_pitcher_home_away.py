import pandas as pd
from unidecode import unidecode
import os

# Paths
TODAY_GAMES_FILE = "data/raw/todaysgames_normalized.csv"
PITCHER_REF_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_HOME = "data/adjusted/pitchers_home.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away.csv"

def normalize_name(name):
    if pd.isna(name):
        return ""
    name = unidecode(name).strip()
    name = name.replace(" ,", ",").replace(", ", ",").replace(",", ", ")
    name = " ".join(name.split())
    # Ensure format: Last, First Suffix (if any)
    tokens = name.split()
    if len(tokens) >= 2:
        first = tokens[0]
        last = " ".join(tokens[1:])
        return f"{last}, {first}".title()
    return name.title()

def main():
    if not os.path.exists(TODAY_GAMES_FILE):
        print(f"❌ Missing file: {TODAY_GAMES_FILE}")
        return

    if not os.path.exists(PITCHER_REF_FILE):
        print(f"❌ Missing file: {PITCHER_REF_FILE}")
        return

    # Load data
    games = pd.read_csv(TODAY_GAMES_FILE)
    ref = pd.read_csv(PITCHER_REF_FILE)
    ref_names = set(ref["last_name, first_name"].apply(normalize_name))

    # Normalize and filter
    games["pitcher_home"] = games["pitcher_home"].apply(normalize_name)
    games["pitcher_away"] = games["pitcher_away"].apply(normalize_name)

    home_df = games[games["pitcher_home"].isin(ref_names)].copy()
    away_df = games[games["pitcher_away"].isin(ref_names)].copy()

    home_df = home_df.rename(columns={"pitcher_home": "last_name, first_name", "home_team": "team"})
    away_df = away_df.rename(columns={"pitcher_away": "last_name, first_name", "away_team": "team"})

    home_df["type"] = "pitcher"
    away_df["type"] = "pitcher"

    home_df = home_df[["last_name, first_name", "team", "type"]]
    away_df = away_df[["last_name, first_name", "team", "type"]]

    os.makedirs("data/adjusted", exist_ok=True)
    home_df.to_csv(OUTPUT_HOME, index=False)
    away_df.to_csv(OUTPUT_AWAY, index=False)

    print(f"✅ Wrote {len(home_df)} rows to {OUTPUT_HOME}")
    print(f"✅ Wrote {len(away_df)} rows to {OUTPUT_AWAY}")

if __name__ == "__main__":
    main()
