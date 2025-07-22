import pandas as pd
import os
from unidecode import unidecode
import re

INPUT_FILE = "data/raw/todaysgames.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = unidecode(name)
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

def normalize_todays_games():
    print("📥 Loading input files...")
    games = pd.read_csv(INPUT_FILE)
    pitchers = pd.read_csv(PITCHERS_FILE)

    print("🧼 Normalizing pitcher names...")
    games["pitcher_home_normalized"] = games["pitcher_home"].apply(normalize_name).str.lower()
    games["pitcher_away_normalized"] = games["pitcher_away"].apply(normalize_name).str.lower()
    pitchers["name_normalized"] = pitchers["last_name, first_name"].apply(normalize_name).str.lower()

    valid_names = set(pitchers["name_normalized"])

    missing = games[
        (~games["pitcher_home_normalized"].isin(valid_names)) |
        (~games["pitcher_away_normalized"].isin(valid_names))
    ]

    if not missing.empty:
        raise ValueError(f"❌ Unrecognized pitcher(s) found:\n{missing[['home_team', 'away_team', 'pitcher_home', 'pitcher_away']]}")

    print("✅ All pitchers recognized. Cleaning and saving...")
    games.drop(columns=["pitcher_home_normalized", "pitcher_away_normalized"], inplace=True)
    games.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ normalize_todays_games completed: {OUTPUT_FILE}")

if __name__ == "__main__":
    normalize_todays_games()
