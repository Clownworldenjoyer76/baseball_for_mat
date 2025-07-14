import pandas as pd
from pathlib import Path
import subprocess

def apply_park_adjustments(batters, park_factors):
    if 'woba' not in batters.columns:
        batters['woba'] = 0.320

    batters = pd.merge(batters, park_factors, on='team', how='left')

    if 'park_factor' in batters.columns:
        batters['adj_woba_park'] = batters['woba'] * batters['park_factor']
    else:
        batters['adj_woba_park'] = batters['woba']

    return batters

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
        subprocess.run(["git", "commit", "-m", "Auto-commit: park adjusted batters + log"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted park files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    games['game_time'] = pd.to_datetime(games['game_time']).dt.hour
    games['day_night'] = games['game_time'].apply(lambda x: 'day' if x < 18 else 'night')

    home_teams = games[['home_team', 'day_night']].drop_duplicates()
    home_teams.columns = ['team', 'day_night']

    batters = {
        'home': pd.read_csv("data/adjusted/batters_home_adjusted.csv"),
        'away': pd.read_csv("data/adjusted/batters_away_adjusted.csv")
    }

    for label in ['home', 'away']:
        merged = pd.merge(batters[label], home_teams, on='team', how='left')

        df_day = pd.read_csv("data/Data/park_factors_day.csv")
        df_night = pd.read_csv("data/Data/park_factors_night.csv")

        merged_day = pd.merge(merged[merged['day_night'] == 'day'], df_day, on='team', how='left')
        merged_night = pd.merge(merged[merged['day_night'] == 'night'], df_night, on='team', how='left')

        final_df = pd.concat([merged_day, merged_night])
        adjusted = apply_park_adjustments(final_df, final_df[['team', 'park_factor']])
        save_outputs(adjusted, label)

    commit_outputs()

if __name__ == "__main__":
    main()
