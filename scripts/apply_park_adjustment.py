import pandas as pd
from pathlib import Path
import subprocess

def load_game_times():
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    games["hour"] = pd.to_datetime(games["game_time"], format="%I:%M %p").dt.hour
    games["time_of_day"] = games["hour"].apply(lambda x: "day" if x < 18 else "night")
    return games[["home_team", "time_of_day"]]

def load_park_factors(time_of_day):
    if time_of_day == "day":
        df = pd.read_csv("data/Data/park_factors_day.csv")
    else:
        df = pd.read_csv("data/Data/park_factors_night.csv")
    return df[["home_team", "Park Factor"]]

def apply_park_adjustments_home(batters, games):
    batters['home_team'] = batters['team']
    merged = pd.merge(batters, games, on='home_team', how='left')
    merged = pd.merge(merged, load_park_factors("day"), on="home_team", how="left")  # Assume default to day, overridden below
    merged.loc[merged['time_of_day'] == 'night', 'Park Factor'] = pd.merge(
        merged.loc[merged['time_of_day'] == 'night'],
        load_park_factors("night"),
        on="home_team",
        how="left"
    )["Park Factor_y"].values

    if 'woba' not in merged.columns:
        merged['woba'] = 0.320

    merged["adj_woba_park"] = merged["woba"] * merged["Park Factor"]
    merged["adj_woba_park"] = merged["adj_woba_park"].fillna(merged["woba"])
    return merged

def apply_park_adjustments_away(batters, games):
    away_team_to_home_team = pd.read_csv("data/raw/todaysgames_normalized.csv").set_index("away_team")["home_team"].to_dict()
    batters['home_team'] = batters['team'].map(away_team_to_home_team)
    merged = pd.merge(batters, games, on='home_team', how='left')
    merged = pd.merge(merged, load_park_factors("day"), on="home_team", how="left")
    merged.loc[merged['time_of_day'] == 'night', 'Park Factor'] = pd.merge(
        merged.loc[merged['time_of_day'] == 'night'],
        load_park_factors("night"),
        on="home_team",
        how="left"
    )["Park Factor_y"].values

    if 'woba' not in merged.columns:
        merged['woba'] = 0.320

    merged["adj_woba_park"] = merged["woba"] * merged["Park Factor"]
    merged["adj_woba_park"] = merged["adj_woba_park"].fillna(merged["woba"])
    return merged

def save_outputs(batters, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"batters_{label}_park.csv"
    logfile = out_path / f"log_park_{label}.txt"
    batters.to_csv(outfile, index=False)
    with open(logfile, 'w') as f:
        f.write(str(batters[['last_name, first_name', 'team', 'adj_woba_park']].head()))

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: adjusted park batters + log"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted park files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    games = load_game_times()

    batters_home = pd.read_csv("data/adjusted/batters_home.csv")
    adjusted_home = apply_park_adjustments_home(batters_home, games)
    save_outputs(adjusted_home, "home")

    batters_away = pd.read_csv("data/adjusted/batters_away.csv")
    adjusted_away = apply_park_adjustments_away(batters_away, games)
    save_outputs(adjusted_away, "away")

    commit_outputs()

if __name__ == "__main__":
    main()
