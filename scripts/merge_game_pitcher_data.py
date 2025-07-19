import pandas as pd
import subprocess

# Input paths
BATTERS_HOME_FILE = "data/adjusted/batters_home_weather_park.csv"
BATTERS_AWAY_FILE = "data/adjusted/batters_away_weather_park.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home_weather_park.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away_weather_park.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"

# Output paths
OUTPUT_HOME = "data/processed/batters_home_with_pitcher.csv"
OUTPUT_AWAY = "data/processed/batters_away_with_pitcher.csv"

def get_pitcher_woba(df, team_col, name_col):
    required = [team_col, name_col, "adj_woba_combined"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in pitcher file")
    return df[required].drop_duplicates(subset=[team_col, name_col])

def standardize_name(full_name):
    if pd.isna(full_name) or full_name.strip().lower() == "undecided":
        return full_name
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return full_name.title()

def main():
    bh = pd.read_csv(BATTERS_HOME_FILE)
    ba = pd.read_csv(BATTERS_AWAY_FILE)
    ph = pd.read_csv(PITCHERS_HOME_FILE)
    pa = pd.read_csv(PITCHERS_AWAY_FILE)
    games = pd.read_csv(GAMES_FILE)

    # Validate required columns
    for col in ["home_team"]:
        if col not in bh.columns:
            raise ValueError(f"Missing column '{col}' in batters_home file")
    for col in ["team"]:
        if col not in ba.columns:
            raise ValueError(f"Missing column '{col}' in batters_away file")
    for col in ["pitcher_home", "pitcher_away"]:
        if col not in games.columns:
            raise ValueError(f"Missing column '{col}' in games file")

    bh["home_team"] = bh["home_team"].str.title()
    ba["team"] = ba["team"].str.title()
    ph["name"] = ph["name"].str.title()
    pa["name"] = pa["name"].str.title()

    games["pitcher_home"] = games["pitcher_home"].fillna("").astype(str).str.strip()
    games["pitcher_away"] = games["pitcher_away"].fillna("").astype(str).str.strip()

    games["pitcher_home"] = games["pitcher_home"].apply(standardize_name)
    games["pitcher_away"] = games["pitcher_away"].apply(standardize_name)

    # Merge home batters with pitcher_home
    if "home_team" in games.columns:
        bh = bh.merge(
            games[["home_team", "pitcher_home"]],
            how="left",
            left_on="home_team",
            right_on="home_team"
        )
    else:
        raise ValueError("Missing 'home_team' in games file")

    # Merge away batters with pitcher_away
    if "away_team" in games.columns:
        ba = ba.merge(
            games[["away_team", "pitcher_away"]],
            how="left",
            left_on="team",
            right_on="away_team"
        )
    else:
        raise ValueError("Missing 'away_team' in games file")

    home_pitcher_stats = get_pitcher_woba(ph, "home_team", "name")
    away_pitcher_stats = get_pitcher_woba(pa, "away_team", "name")

    bh = bh.merge(
        home_pitcher_stats,
        how="left",
        left_on=["home_team", "pitcher_home"],
        right_on=["home_team", "name"],
        suffixes=("", "_pitcher")
    )

    ba = ba.merge(
        away_pitcher_stats,
        how="left",
        left_on=["away_team", "pitcher_away"],
        right_on=["away_team", "name"],
        suffixes=("", "_pitcher")
    )

    # Debug prints
    print("✅ Rows in HOME batters dataframe:", len(bh))
    print("✅ Rows in AWAY batters dataframe:", len(ba))
    print("🔎 Sample rows from HOME:")
    print(bh[["name", "home_team", "pitcher_home", "adj_woba_combined"]].head())
    print("🔎 Sample rows from AWAY:")
    print(ba[["name", "away_team", "pitcher_away", "adj_woba_combined"]].head())

    bh.to_csv(OUTPUT_HOME, index=False)
    ba.to_csv(OUTPUT_AWAY, index=False)

    subprocess.run(["git", "add", OUTPUT_HOME, OUTPUT_AWAY])
    subprocess.run(["git", "commit", "-m", "Add merged batter and pitcher matchup stats"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
