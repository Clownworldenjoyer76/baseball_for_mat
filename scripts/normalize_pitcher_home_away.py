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
    if "," in name:
        return name.title()
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

    # Normalize reference pitcher names
    ref["last_name, first_name"] = ref["last_name, first_name"].astype(str).str.strip().apply(normalize_name)
    ref_names = set(ref["last_name, first_name"])

    # Normalize pitcher names in games file
    games["pitcher_home"] = games["pitcher_home"].astype(str).str.strip().apply(normalize_name)
    games["pitcher_away"] = games["pitcher_away"].astype(str).str.strip().apply(normalize_name)

    # Drop invalid/null
    games = games.dropna(subset=["pitcher_home", "pitcher_away"])

    # Debug unmatched names
    dropped_home = games[~games["pitcher_home"].isin(ref_names)]["pitcher_home"].unique()
    dropped_away = games[~games["pitcher_away"].isin(ref_names)]["pitcher_away"].unique()

    if len(dropped_home) > 0:
        print("⚠️ Dropped unmatched home pitchers:", dropped_home.tolist())
    if len(dropped_away) > 0:
        print("⚠️ Dropped unmatched away pitchers:", dropped_away.tolist())

    # Filter and format
    home_df = games[games["pitcher_home"].isin(ref_names)].copy()
    away_df = games[games["pitcher_away"].isin(ref_names)].copy()

    home_df = home_df.rename(columns={"pitcher_home": "last_name, first_name", "home_team": "team"})
    away_df = away_df.rename(columns={"pitcher_away": "last_name, first_name", "away_team": "team"})

    home_df["type"] = "pitcher"
    away_df["type"] = "pitcher"

    # Rename "team" to "home_team" for home_df only
    home_df = home_df[["last_name, first_name", "team", "type"]].rename(columns={"team": "home_team"})
    away_df = away_df[["last_name, first_name", "team", "type"]]  # leave column name as "team"

    os.makedirs("data/adjusted", exist_ok=True)

    # Force update by writing to temp then replacing original
    temp_home = OUTPUT_HOME + ".tmp"
    temp_away = OUTPUT_AWAY + ".tmp"
    home_df.to_csv(temp_home, index=False)
    away_df.to_csv(temp_away, index=False)
    os.replace(temp_home, OUTPUT_HOME)
    os.replace(temp_away, OUTPUT_AWAY)

    print(f"✅ Wrote {len(home_df)} rows to {OUTPUT_HOME}")
    print(f"✅ Wrote {len(away_df)} rows to {OUTPUT_AWAY}")

if __name__ == "__main__":
    main()
