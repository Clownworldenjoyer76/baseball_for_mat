import pandas as pd
from pathlib import Path

# === Config ===
BAT_HOME_FILE = Path("data/end_chain/final/updating/bat_home3.csv")
BAT_AWAY_FILE = Path("data/end_chain/final/updating/bat_away4.csv")
BAT_TODAY_FILE = Path("data/end_chain/bat_today.csv")
PITCHERS_FILE = Path("data/end_chain/final/startingpitchers.csv")
UPDATE_SOURCE_FILE = Path("data/cleaned/batters_today.csv")
PITCHERS_XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

OUTPUT_DIR = Path("data/end_chain/final/")
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
    print("🔄 Loading files...")
    bat_home = load_csv(BAT_HOME_FILE)
    bat_away = load_csv(BAT_AWAY_FILE)
    bat_today = load_csv(BAT_TODAY_FILE)
    pitchers = load_csv(PITCHERS_FILE)
    batters_today_data = load_csv(UPDATE_SOURCE_FILE)
    pitchers_xtra = load_csv(PITCHERS_XTRA_FILE)

    print("✅ Files loaded. Normalizing names...")

    bat_home["pitcher_home"] = bat_home["pitcher_home"].apply(enforce_last_first)
    bat_away["pitcher_away"] = bat_away["pitcher_away"].apply(enforce_last_first)
    bat_today["name"] = bat_today["name"].apply(enforce_last_first)
    bat_home["last_name, first_name"] = bat_home["last_name, first_name"].apply(enforce_last_first)
    bat_away["last_name, first_name"] = bat_away["last_name, first_name"].apply(enforce_last_first)
    bat_today["last_name, first_name"] = bat_today["last_name, first_name"].apply(enforce_last_first)
    pitchers["last_name, first_name"] = pitchers["last_name, first_name"].apply(enforce_last_first)

    print("🔄 Updating batter files...")

    update_columns = [
        "adj_woba_combined", "whiff_percent", "zone_swing_miss_percent",
        "out_of_zone_swing_miss_percent", "gb_percent", "fb_percent",
        "strikeouts", "hit", "bb_percent", "double", "triple", "home_run"
    ]

    source_merge = batters_today_data[["player_id"] + [col for col in update_columns if col in batters_today_data.columns]]

    # === Update bat_home ===
    update_cols_home = [col for col in update_columns if col in bat_home.columns and col in source_merge.columns]
    bat_home = pd.merge(bat_home, source_merge[["player_id"] + update_cols_home], on="player_id", how="left", suffixes=('', '_update'))
    for col in update_cols_home:
        bat_home.loc[:, col] = bat_home[f"{col}_update"].combine_first(bat_home[col])
        bat_home.drop(columns=[f"{col}_update"], inplace=True)

    # === Update bat_away ===
    update_cols_away = [col for col in update_columns if col in bat_away.columns and col in source_merge.columns]
    bat_away = pd.merge(bat_away, source_merge[["player_id"] + update_cols_away], on="player_id", how="left", suffixes=('', '_update'))
    for col in update_cols_away:
        bat_away.loc[:, col] = bat_away[f"{col}_update"].combine_first(bat_away[col])
        bat_away.drop(columns=[f"{col}_update"], inplace=True)

    # === Update innings_pitched from pitchers_xtra ===
    innings_map = pitchers_xtra.set_index("last_name, first_name")["innings_pitched"].to_dict()
    for df in [bat_home, bat_away]:
        name_col = "last_name, first_name"
        if "innings_pitched" in df.columns:
            df.loc[:, "innings_pitched"] = df[name_col].map(innings_map).combine_first(df["innings_pitched"])

    print("💾 Saving final files...")
    bat_home.to_csv(BAT_HOME_FINAL, index=False)
    bat_away.to_csv(BAT_AWAY_FINAL, index=False)
    bat_today.to_csv(BAT_TODAY_FINAL, index=False)
    pitchers.to_csv(PITCHERS_FINAL, index=False)

    print("✅ Done.")

if __name__ == "__main__":
    main()
