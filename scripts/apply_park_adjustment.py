
import pandas as pd
from pathlib import Path

def apply_park_adjustments(batters, park_day, park_night):
    if 'team' in batters.columns:
        batters['home_team'] = batters['team']
    else:
        raise ValueError("Missing 'team' column in batters file.")

    park_day = park_day.drop_duplicates(subset='home_team')
    park_night = park_night.drop_duplicates(subset='home_team')

    batters = pd.merge(batters, park_day, on='home_team', how='left', suffixes=('', '_day'))
    batters = pd.merge(batters, park_night, on='home_team', how='left', suffixes=('', '_night'))

    if 'home_run' in batters.columns and 'HR' in batters.columns:
        batters['adj_home_run_park'] = batters['home_run'] * batters['HR']
    else:
        batters['adj_home_run_park'] = batters.get('home_run', 0)

    if 'hard_hit_percent' in batters.columns and 'HardHit' in batters.columns:
        batters['adj_hard_hit_percent_park'] = (batters['hard_hit_percent'] / 100.0) * batters['HardHit']
    else:
        batters['adj_hard_hit_percent_park'] = batters.get('hard_hit_percent', 0)

    return batters

def save_outputs(batters, label):
    out_path = Path("data/park_adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"batters_{label}_adjusted.csv"
    logfile = out_path / f"log_park_{label}.txt"

    batters.to_csv(outfile, index=False)

    with open(logfile, 'w') as f:
        f.write(str(batters[['last_name, first_name', 'team', 'adj_home_run_park', 'adj_hard_hit_percent_park']].head()))

def main():
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    for label in ['home', 'away']:
        infile = f"data/weather_adjusted/batters_{label}_weather.csv"
        batters = pd.read_csv(infile)
        adjusted = apply_park_adjustments(batters, park_day, park_night)
        save_outputs(adjusted, label)

if __name__ == "__main__":
    main()
