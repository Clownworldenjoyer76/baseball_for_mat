import pandas as pd
import os
import unicodedata
import re

# Normalization helpers
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
    suffixes = ["Jr", "Sr", "II", "III", "IV", "Jr.", "Sr."]

    tokens = name.replace(",", "").split()
    if len(tokens) >= 2:
        last_parts = [tokens[-1]]
        if tokens[-1].replace(".", "") in suffixes and len(tokens) >= 3:
            last_parts = [tokens[-2], tokens[-1]]
        last = " ".join(last_parts)
        first = " ".join(tokens[:-len(last_parts)])
        return f"{last.strip()}, {first.strip()}"
    return name.title()

def normalize_todays_games():
    INPUT_FILE = "data/raw/todaysgames.csv"
    OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"
    PITCHER_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
    TEAM_MAP_FILE = "data/Data/team_abv_map.csv"

    games = pd.read_csv(INPUT_FILE)
    team_map = pd.read_csv(TEAM_MAP_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)

    # Normalize team names
    team_dict = dict(zip(team_map['code'].str.strip().str.upper(), team_map['name'].str.strip()))
    games['home_team'] = games['home_team'].str.strip().str.upper().map(team_dict).fillna(games['home_team'])
    games['away_team'] = games['away_team'].str.strip().str.upper().map(team_dict).fillna(games['away_team'])

    # Normalize pitcher names
    games['pitcher_home'] = games['pitcher_home'].apply(normalize_name)
    games['pitcher_away'] = games['pitcher_away'].apply(normalize_name)

    valid_pitchers = set(pitchers['last_name, first_name'])
    missing = games[~games['pitcher_home'].isin(valid_pitchers) & (games['pitcher_home'] != 'Undecided')]
    missing = pd.concat([missing, games[~games['pitcher_away'].isin(valid_pitchers) & (games['pitcher_away'] != 'Undecided')]])
    missing = missing.drop_duplicates()

    if not missing.empty:
        raise ValueError(f"❌ Unrecognized pitcher(s) found:\n{missing[['home_team', 'away_team', 'pitcher_home', 'pitcher_away']]}")

    games.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ normalize_todays_games.py completed: {len(games)} rows written to {OUTPUT_FILE}")

if __name__ == "__main__":
    normalize_todays_games()
