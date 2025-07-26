# scripts/normalize_todays_games.py

import pandas as pd
import re
from unidecode import unidecode
from datetime import datetime
import sys

INPUT_FILE = "data/raw/todaysgames.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/raw/todaysgames_normalized.csv"

# ─── Name Normalization ─────────────────────────────────────────────

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

# ─── Time Format Check ──────────────────────────────────────────────

def is_valid_time(t):
    try:
        datetime.strptime(t.strip(), "%I:%M %p")
        return True
    except Exception:
        return False

# ─── Main Function ──────────────────────────────────────────────────

def normalize_todays_games():
    print("📥 Loading input files...")
    try:
        games = pd.read_csv(INPUT_FILE)
        pitchers = pd.read_csv(PITCHERS_FILE)
        team_map = pd.read_csv(TEAM_MAP_FILE)
    except Exception as e:
        print(f"❌ Error loading input files: {e}")
        sys.exit(1)

    # ─── Normalize Pitcher Names ─────────────────────────────────────

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
        print("❌ Unrecognized pitcher(s) found:")
        print(missing[["home_team", "away_team", "pitcher_home", "pitcher_away"]])
        sys.exit(1)

    print("✅ All pitchers recognized.")

    # ─── Normalize Team Names ────────────────────────────────────────

    print("🔁 Mapping team abbreviations to full names...")
    team_map["team_code"] = team_map["team_code"].astype(str).str.strip().str.upper()
    team_map["team_name"] = team_map["team_name"].astype(str).str.strip()
    code_to_name = dict(zip(team_map["team_code"], team_map["team_name"]))

    for col in ["home_team", "away_team"]:
        original = games[col].astype(str).str.strip().str.upper()
        games[col] = original.map(code_to_name)
        unmapped = original[games[col].isna()].unique()
        if len(unmapped) > 0:
            print(f"⚠️ Unmapped {col} codes: {list(unmapped)}")
        games[col] = games[col].fillna(original)

    # ─── Validate Time Format ────────────────────────────────────────

    print("⏱ Validating game times...")
    invalid_times = games[~games["game_time"].apply(is_valid_time)]
    if not invalid_times.empty:
        print("❌ Invalid game_time values:")
        print(invalid_times[["home_team", "away_team", "game_time"]])
        sys.exit(1)

    # ─── Check for Duplicate Matchups ────────────────────────────────

    print("🔁 Checking for duplicate matchups...")
    dupes = games.duplicated(subset=["home_team", "away_team"], keep=False)
    if dupes.any():
        print("❌ Duplicate matchups found:")
        print(games[dupes])
        sys.exit(1)

    # ─── Finalize ────────────────────────────────────────────────────

    games.drop(columns=["pitcher_home_normalized", "pitcher_away_normalized"], inplace=True)
    games.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ normalize_todays_games completed: {OUTPUT_FILE}")

if __name__ == "__main__":
    normalize_todays_games()
