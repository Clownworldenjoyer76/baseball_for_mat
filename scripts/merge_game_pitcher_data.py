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
    return df[[team_col, name_col, "adj_woba_combined"]].drop_duplicates(subset=[team_col, name_col])

def main():
    bh = pd.read_csv(BATTERS_HOME_FILE)
    ba = pd.read_csv(BATTERS_AWAY_FILE)
    ph = pd.read_csv(PITCHERS_HOME_FILE)
    pa = pd.read_csv(PITCHERS_AWAY_FILE)
    games = pd.read_csv(GAMES_FILE)

    # Standardize casing for join
    bh["home_team_park"] = bh["home_team_park"].str.title()
    ba["team"] = ba["team"].str.title()
    games["home_team"] = games["home_team"].str.title()
    games["away_team"] = games["away_team"].str.title()

    # Merge batters with pitcher names
    bh = bh.merge(
        games[["home_team", "pitcher_home"]],
        how="left",
        left_on="home_team_park",
        right_on="home_team"
    )
    ba = ba.merge(
        games[["away_team", "pitcher_away"]],
        how="left",
        left_on="team",
        right_on="away_team"
    )

    # Merge pitcher stats
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

    # Drop duplicates and write output
    bh.to_csv(OUTPUT_HOME, index=False)
    ba.to_csv(OUTPUT_AWAY, index=False)

    # Commit files
    subprocess.run(["git", "add", OUTPUT_HOME, OUTPUT_AWAY])
    subprocess.run(["git", "commit", "-m", "Add merged batter and pitcher matchup stats"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
