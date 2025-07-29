import pandas as pd
from pathlib import Path

# === Config ===
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
BAT_TODAY_FILE = Path("data/end_chain/bat_today.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
BATTERS_TODAY_UPDATE_FILE = Path("data/cleaned/batters_today.csv") # New input file

OUTPUT_DIR = Path("data/end_chain/final/")


# === Input file paths ===
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
BAT_TODAY_FILE = Path("data/end_chain/bat_today.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")


# === Output file paths ===
BAT_HOME_FINAL = OUTPUT_DIR / "batter_home_final.csv"
BAT_AWAY_FINAL = OUTPUT_DIR / "batter_away_final.csv"
BAT_TODAY_FINAL = OUTPUT_DIR / "bat_today_final.csv"
PITCHERS_FINAL = OUTPUT_DIR / "startingpitchers_final.csv"

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    return pd.read_csv(path)

def enforce_last_first(name):
    if not isinstance(name, str) or "," not in name:
        parts = name.strip().split()
        if len(parts) >= 2:
            return f"{parts[-1].capitalize()}, {' '.join(p.capitalize() for p in parts[:-1])}"
        return name
    return name.strip()

def main():
    print("🔄 Loading normalized files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    bat_today = load_csv(BAT_TODAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    batters_today_update = load_csv(BATTERS_TODAY_UPDATE_FILE) # Load the update file

    print("✅ Files loaded. Checking formats...")

    # Enforce final name formatting again before final save
    bat_home["pitcher_home"] = bat_home["pitcher_home"].apply(enforce_last_first)
    bat_away["pitcher_away"] = bat_away["pitcher_away"].apply(enforce_last_first)
    bat_today["name"] = bat_today["name"].apply(enforce_last_first)
    bat_home["last_name, first_name"] = bat_home["last_name, first_name"].apply(enforce_last_first)
    bat_away["last_name, first_name"] = bat_away["last_name, first_name"].apply(enforce_last_first)
    bat_today["last_name, first_name"] = bat_today["last_name, first_name"].apply(enforce_last_first)
    pitchers["last_name, first_name"] = pitchers["last_name, first_name"].apply(enforce_last_first)

    print("🔄 Updating batter_home_final.csv and batter_away_final.csv with new values...")

    # Columns to update
    update_columns = [
        "adj_woba_combined", "whiff_percent", "zone_swing_miss_percent",
        "out_of_zone_swing_miss_percent", "gb_percent", "fb_percent",
        "innings_pitched", "strikeouts", "hit", "bb_percent", "double",
        "triple", "home_run"
    ]

    # Merge to update bat_home
    bat_home = pd.merge(
        bat_home,
        batters_today_update[["player_id"] + update_columns],
        on="player_id",
        how="left",
        suffixes=('', '_new')
    )
    for col in update_columns:
        bat_home[col].fillna(bat_home[col + '_new'], inplace=True)
        bat_home.drop(columns=[col + '_new'], inplace=True)

    # Merge to update bat_away
    bat_away = pd.merge(
        bat_away,
        batters_today_update[["player_id"] + update_columns],
        on="player_id",
        how="left",
        suffixes=('', '_new')
    )
    for col in update_columns:
        bat_away[col].fillna(bat_away[col + '_new'], inplace=True)
        bat_away.drop(columns=[col + '_new'], inplace=True)


    print("💾 Saving renamed final files...")
    bat_home.to_csv(BAT_HOME_FINAL, index=False)
    bat_away.to_csv(BAT_AWAY_FINAL, index=False)
    bat_today.to_csv(BAT_TODAY_FINAL, index=False)
    pitchers.to_csv(PITCHERS_FINAL, index=False)

    print("✅ Final files saved to data/end_chain/final/.")

if __name__ == "__main__":
    main()
