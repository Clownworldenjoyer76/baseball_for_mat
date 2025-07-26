# scripts/normalize_todays_games.py

import pandas as pd
import re
from unidecode import unidecode
from datetime import datetime
from pathlib import Path
import sys

INPUT_FILE = "data/raw/todaysgames.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"
UNMATCHED_OUTPUT = "data/cleaned/unmatched_pitchers.csv"
SUMMARY_FILE = Path("summaries/summary.txt")

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = unidecode(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    suffixes = {"Jr", "Sr", "II", "III", "IV", "Jr.", "Sr."}
    tokens = name.replace(",", "").split()

    if len(tokens) >= 2:
        if tokens[-1] in suffixes and len(tokens) >= 3:
            last = f"{tokens[-2]} {tokens[-1]}"
            first = " ".join(tokens[:-2])
        else:
            last = tokens[-1]
            first = " ".join(tokens[:-1])
        return f"{last.strip().title()}, {first.strip().title()}"
    return name.title()

def is_valid_time(t):
    try:
        datetime.strptime(t.strip(), "%I:%M %p")
        return True
    except Exception:
        return False

def normalize_todays_games():
    status = "PASS"
    try:
        games = pd.read_csv(INPUT_FILE)
        pitchers = pd.read_csv(PITCHERS_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
    except Exception as e:
        print(f"❌ Error loading input files: {e}")
        status = "FAIL"
        write_summary(0, 0, status)
        sys.exit(1)

    games["pitcher_home_normalized"] = games["pitcher_home"].apply(normalize_name).str.lower()
    games["pitcher_away_normalized"] = games["pitcher_away"].apply(normalize_name).str.lower()
    pitchers["name_normalized"] = pitchers["last_name, first_name"].apply(normalize_name).str.lower()
    valid_names = set(pitchers["name_normalized"])

    unmatched_rows = games[
        (~games["pitcher_home_normalized"].isin(valid_names)) |
        (~games["pitcher_away_normalized"].isin(valid_names))
    ]
    unmatched_count = len(unmatched_rows)

    if unmatched_count > 0:
        Path(UNMATCHED_OUTPUT).parent.mkdir(parents=True, exist_ok=True)
        unmatched_rows.to_csv(UNMATCHED_OUTPUT, index=False)

    team_map["team_code"] = team_map["team_code"].astype(str).str.strip().str.upper()
    team_map["team_name"] = team_map["team_name"].astype(str).str.strip()
    code_to_name = dict(zip(team_map["team_code"], team_map["team_name"]))

    for col in ["home_team", "away_team"]:
        original = games[col].astype(str).str.strip().str.upper()
        games[col] = original.map(code_to_name)
        games[col] = games[col].fillna(original)

    if not games["game_time"].apply(is_valid_time).all():
        print("❌ Invalid game_time values found")
        status = "FAIL"
        write_summary(len(games), unmatched_count, status)
        sys.exit(1)

    if games.duplicated(subset=["home_team", "away_team"]).any():
        print("❌ Duplicate matchups found")
        status = "FAIL"
        write_summary(len(games), unmatched_count, status)
        sys.exit(1)

    games.drop(columns=["pitcher_home_normalized", "pitcher_away_normalized"], inplace=True)
    games.to_csv(OUTPUT_FILE, index=False)

    write_summary(len(games), unmatched_count, status)
    print(f"✅ normalize_todays_games completed: {OUTPUT_FILE}")

def write_summary(matchup_count, unmatched_count, status):
    SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SUMMARY_FILE, "a") as f:
        f.write(f"normalize_todays_games: {matchup_count} matchups, {unmatched_count} unmatched pitchers — {status}\n")

if __name__ == "__main__":
    normalize_todays_games()
