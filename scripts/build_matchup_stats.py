import pandas as pd
from pathlib import Path
import subprocess

def build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games):
    batters_home = batters_home.rename(columns={"team_code": "home_team"}).assign(side="home")
    batters_away = batters_away.rename(columns={"team_code": "away_team"}).assign(side="away")

    pitchers_home = pitchers_home.rename(columns={"team": "home_team"}).assign(side="home")
    pitchers_away = pitchers_away.rename(columns={"team": "away_team"}).assign(side="away")

    # Deduplicate pitchers
    pitchers_home = pitchers_home.drop_duplicates(subset="home_team")
    pitchers_away = pitchers_away.drop_duplicates(subset="away_team")

    matchups = []

    for _, game in games.iterrows():
        home_team = game["home_team"]
        away_team = game["away_team"]

        home_pitcher = pitchers_home[pitchers_home["home_team"] == home_team]
        away_pitcher = pitchers_away[pitchers_away["away_team"] == away_team]

        home_batters = batters_home[batters_home["home_team"] == home_team]
        away_batters = batters_away[batters_away["away_team"] == away_team]

        for batter in home_batters.itertuples():
            matchup = {
                "team": home_team,
                "batter": batter.name,
                "vs_pitcher": away_pitcher["pitcher"].values[0] if not away_pitcher.empty else "N/A",
                "adj_woba_batter": getattr(batter, "adj_woba_weather_park", None),
                "adj_woba_pitcher": away_pitcher["adj_woba_weather_park"].values[0] if not away_pitcher.empty else None
            }
            matchups.append(matchup)

        for batter in away_batters.itertuples():
            matchup = {
                "team": away_team,
                "batter": batter.name,
                "vs_pitcher": home_pitcher["pitcher"].values[0] if not home_pitcher.empty else "N/A",
                "adj_woba_batter": getattr(batter, "adj_woba_weather_park", None),
                "adj_woba_pitcher": home_pitcher["adj_woba_weather_park"].values[0] if not home_pitcher.empty else None
            }
            matchups.append(matchup)

    return pd.DataFrame(matchups)

def main():
    batters_home = pd.read_csv("data/final/batters_home_adjusted.csv")
    batters_away = pd.read_csv("data/final/batters_away_adjusted.csv")
    pitchers_home = pd.read_csv("data/final/pitchers_home_adjusted.csv")
    pitchers_away = pd.read_csv("data/final/pitchers_away_adjusted.csv")
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")

    matchup_stats = build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games)

    output_path = Path("data/final")
    output_path.mkdir(parents=True, exist_ok=True)
    matchup_stats.to_csv(output_path / "matchup_stats.csv", index=False)

    try:
        subprocess.run(["git", "add", "data/final/matchup_stats.csv"], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: matchup stats built"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

if __name__ == "__main__":
    main()
