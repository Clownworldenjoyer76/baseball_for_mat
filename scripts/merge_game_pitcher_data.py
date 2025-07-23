import pandas as pd
import subprocess
import sys

# Input paths
BATTERS_HOME_FILE = "data/adjusted/batters_home_weather_park.csv"
BATTERS_AWAY_FILE = "data/adjusted/batters_away_weather_park.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home_weather_park.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away_weather_park.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"

# Output paths
OUTPUT_HOME = "data/processed/batters_home_with_pitcher.csv"
OUTPUT_AWAY = "data/processed/batters_away_with_pitcher.csv"

def get_pitcher_woba(df, name_col):
    required = [name_col, "adj_woba_combined"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in pitcher file")
    return df[required].drop_duplicates(subset=[name_col])

def standardize_name(full_name):
    if pd.isna(full_name) or full_name.strip().lower() == "undecided":
        return full_name
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return full_name.title()

def verify_columns(df, required, label):
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in {label} file")

def main():
    bh = pd.read_csv(BATTERS_HOME_FILE)
    ba = pd.read_csv(BATTERS_AWAY_FILE)
    ph = pd.read_csv(PITCHERS_HOME_FILE)
    pa = pd.read_csv(PITCHERS_AWAY_FILE)
    games = pd.read_csv(GAMES_FILE)

    # Validate required columns
    verify_columns(bh, ["home_team", "last_name, first_name"], "batters_home")
    verify_columns(ba, ["away_team", "last_name, first_name"], "batters_away")
    verify_columns(ph, ["home_team", "last_name, first_name", "adj_woba_combined"], "pitchers_home")
    verify_columns(pa, ["away_team", "last_name, first_name", "adj_woba_combined"], "pitchers_away")
    verify_columns(games, ["home_team", "away_team", "pitcher_home", "pitcher_away"], "games")

    # Normalize names for merging
    bh["last_name, first_name"] = bh["last_name, first_name"].astype(str).str.title()
    ba["last_name, first_name"] = ba["last_name, first_name"].astype(str).str.title()
    ph["last_name, first_name"] = ph["last_name, first_name"].apply(standardize_name)
    pa["last_name, first_name"] = pa["last_name, first_name"].apply(standardize_name)
    games["pitcher_home"] = games["pitcher_home"].fillna("").astype(str).str.strip().apply(standardize_name)
    games["pitcher_away"] = games["pitcher_away"].fillna("").astype(str).str.strip().apply(standardize_name)

    # Merge pitcher names into batter files
    bh = bh.merge(
        games[["home_team", "pitcher_home"]],
        how="left",
        on="home_team"
    )
    ba = ba.merge(
        games[["away_team", "pitcher_away"]],
        how="left",
        on="away_team"
    )

    # Merge pitcher stats
    home_pitcher_stats = get_pitcher_woba(ph, "last_name, first_name")
    away_pitcher_stats = get_pitcher_woba(pa, "last_name, first_name")

    bh = bh.merge(
        home_pitcher_stats,
        how="left",
        left_on="pitcher_home",
        right_on="last_name, first_name",
        suffixes=("", "_pitcher")
    )

    ba = ba.merge(
        away_pitcher_stats,
        how="left",
        left_on="pitcher_away",
        right_on="last_name, first_name",
        suffixes=("", "_pitcher")
    )

    print("✅ HOME batters rows:", len(bh))
    print("✅ AWAY batters rows:", len(ba))
    print("🔍 HOME sample:", bh[["last_name, first_name", "home_team", "pitcher_home", "adj_woba_combined"]].head())
    print("🔍 AWAY sample:", ba[["last_name, first_name", "away_team", "pitcher_away", "adj_woba_combined"]].head())

    bh.to_csv(OUTPUT_HOME, index=False)
    ba.to_csv(OUTPUT_AWAY, index=False)

    subprocess.run(["git", "add", OUTPUT_HOME, OUTPUT_AWAY], check=True)
    subprocess.run(["git", "commit", "-m", "Add merged batter and pitcher matchup stats"], check=True)
    subprocess.run(["git", "push"], check=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)
