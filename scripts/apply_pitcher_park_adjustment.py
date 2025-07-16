import pandas as pd
from pathlib import Path
import subprocess

def load_game_times():
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    games["hour"] = pd.to_datetime(games["game_time"], format="%I:%M %p").dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if x < 18 else "night")
    return games[["home_team", "time_of_day"]]

def load_park_factors(time_of_day):
    path = f"data/Data/park_factors_{time_of_day}.csv"
    return pd.read_csv(path)[["home_team", "Park Factor"]]

def apply_park_adjustments(pitchers, games, is_home):
    if is_home:
        pitchers['home_team'] = pitchers['team']
    else:
        away_team_to_home_team = pd.read_csv("data/raw/todaysgames_normalized.csv").set_index("away_team")["home_team"].to_dict()
        pitchers['home_team'] = pitchers['team'].map(away_team_to_home_team)

    merged = pd.merge(pitchers, games, on='home_team', how='left')

    merged = pd.merge(merged, load_park_factors("day"), on="home_team", how="left")
    night_games = merged['time_of_day'] == 'night'
    merged.loc[night_games, 'Park Factor'] = pd.merge(
        merged.loc[night_games],
        load_park_factors("night"),
        on="home_team",
        how="left"
    )["Park Factor_y"].values

    if 'woba' not in merged.columns:
        merged['woba'] = 0.320

    merged["adj_woba_park"] = merged["woba"] * (merged["Park Factor"] / 100)
    merged["adj_woba_park"] = merged["adj_woba_park"].fillna(merged["woba"])
    return merged

def save_outputs(pitchers, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"pitchers_{label}_park.csv"
    logfile = out_path / f"log_pitchers_park_{label}.txt"
    pitchers.to_csv(outfile, index=False)
    top5 = pitchers[["pitcher", "team", "adj_woba_park"]].sort_values(by="adj_woba_park", ascending=False).head(5)
    with open(logfile, "w") as f:
        f.write(f"Top 5 {label} pitchers (park adjusted):\n")
        f.write(top5.to_string(index=False))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "Auto-commit: pitcher park adjustments"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed pitcher park adjustment files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    games = load_game_times()
    home = pd.read_csv("data/adjusted/pitchers_home.csv")
    away = pd.read_csv("data/adjusted/pitchers_away.csv")

    adjusted_home = apply_park_adjustments(home, games, True)
    save_outputs(adjusted_home, "home")

    adjusted_away = apply_park_adjustments(away, games, False)
    save_outputs(adjusted_away, "away")

    commit_outputs()

if __name__ == "__main__":
    main()
