import pandas as pd
from pathlib import Path
import subprocess
from datetime import datetime

def load_park_factors():
    day_factors = pd.read_csv("data/Data/park_factors_day.csv")
    night_factors = pd.read_csv("data/Data/park_factors_night.csv")
    return day_factors, night_factors

def determine_day_night(game_time_str):
    try:
        game_time = datetime.strptime(game_time_str.strip(), "%I:%M %p")
        return "day" if game_time.hour < 18 else "night"
    except Exception:
        return "day"  # default to day if parsing fails

def get_home_team_park_map():
    games = pd.read_csv("data/raw/todaysgames_normalized.csv")
    games['day_night'] = games['game_time'].apply(determine_day_night)
    park_map = games[['home_team', 'day_night']].drop_duplicates()
    return park_map.set_index('home_team')['day_night'].to_dict()

def apply_park_adjustments(batters, park_factors, label):
    if 'team' not in batters.columns or 'opponent' not in batters.columns:
        raise ValueError("Missing 'team' or 'opponent' column in batters file.")

    park_map = get_home_team_park_map()
    opponent_park_type = batters['opponent'].map(park_map).fillna('day')

    factors = []
    for typ in opponent_park_type:
        if typ == 'night':
            factors.append(park_factors['night'])
        else:
            factors.append(park_factors['day'])

    batters = batters.copy()
    if 'woba' not in batters.columns:
        batters['woba'] = 0.320
    batters['park_factor'] = factors
    batters['adj_woba_park'] = batters['woba'] * batters['park_factor']
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
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    day_factors = pd.read_csv("data/Data/park_factors_day.csv")
    night_factors = pd.read_csv("data/Data/park_factors_night.csv")
    park_factors = {
        'day': day_factors['park_factor'].mean(),
        'night': night_factors['park_factor'].mean()
    }

    for label in ['home', 'away']:
        infile = f"data/adjusted/batters_{label}_adjusted.csv"
        batters = pd.read_csv(infile)
        adjusted = apply_park_adjustments(batters, park_factors, label)
        save_outputs(adjusted, label)

    commit_outputs()

if __name__ == "__main__":
    main()
