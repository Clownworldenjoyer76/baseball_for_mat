import pandas as pd
from pathlib import Path
import subprocess

def load_data():
    # Load inputs
    batters_home = pd.read_csv("data/adjusted/batters_home_adjusted.csv")
    batters_away = pd.read_csv("data/adjusted/batters_away_adjusted.csv")
    pitchers_home = pd.read_csv("data/adjusted/pitchers_home_adjusted.csv")
    pitchers_away = pd.read_csv("data/adjusted/pitchers_away_adjusted.csv")
    lineups = pd.read_csv("data/raw/lineups.csv")
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    return batters_home, batters_away, pitchers_home, pitchers_away, lineups, games

def clean_lineups(lineups):
    lineups["last_name, first_name"] = lineups["last_name, first_name"].str.strip()
    lineups["team_code"] = lineups["team_code"].str.strip()
    return lineups

def filter_batters(batters, lineups):
    return pd.merge(batters, lineups, left_on=["last_name, first_name", "team"], right_on=["last_name, first_name", "team_code"])

def build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games):
    games["matchup"] = games["away_team"] + " @ " + games["home_team"]

    batters_home["side"] = "home"
    batters_away["side"] = "away"
    all_batters = pd.concat([batters_home, batters_away], ignore_index=True)

    pitchers_home = pitchers_home.rename(columns={"team": "home_team"}).assign(side="home")
    pitchers_away = pitchers_away.rename(columns={"team": "away_team"}).assign(side="away")

    matchup_data = []
    for _, game in games.iterrows():
        home_team = game["home_team"]
        away_team = game["away_team"]
        game_matchup = f"{away_team} @ {home_team}"

        home_pitcher = pitchers_home[pitchers_home["home_team"] == home_team]
        away_pitcher = pitchers_away[pitchers_away["away_team"] == away_team]

        batters_in_game = all_batters[(all_batters["team"] == home_team) | (all_batters["team"] == away_team)].copy()
        batters_in_game["matchup"] = game_matchup

        if not home_pitcher.empty:
            batters_in_game["opposing_pitcher"] = home_pitcher.iloc[0]["pitcher"] if home_pitcher.iloc[0]["side"] == "away" else away_pitcher.iloc[0]["pitcher"]
            batters_in_game["pitcher_adj_woba"] = home_pitcher.iloc[0]["adj_woba_combined"]

        matchup_data.append(batters_in_game)

    return pd.concat(matchup_data, ignore_index=True)

def save_output(df):
    out_path = Path("data/final")
    out_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path / "matchup_stats.csv", index=False)

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/final/matchup_stats.csv"], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Auto-commit: matchup stats built"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Matchup stats committed.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    batters_home, batters_away, pitchers_home, pitchers_away, lineups, games = load_data()
    lineups = clean_lineups(lineups)

    batters_home = filter_batters(batters_home, lineups)
    batters_away = filter_batters(batters_away, lineups)

    matchup_stats = build_matchup_df(batters_home, batters_away, pitchers_home, pitchers_away, games)
    save_output(matchup_stats)
    commit_outputs()

if __name__ == "__main__":
    main()
