
import pandas as pd
from pathlib import Path

def apply_weather_adjustments(batters, weather):
    if 'team' in batters.columns:
        batters['home_team'] = batters['team']
    else:
        raise ValueError("Missing 'team' column in batters file.")

    weather = weather.drop_duplicates(subset='home_team')
    batters = pd.merge(batters, weather, on='home_team', how='left')

    if 'woba' not in batters.columns:
        batters['woba'] = 0.320

    if 'temperature' in batters.columns:
        batters['adj_woba_weather'] = batters['woba'] + ((batters['temperature'] - 70) * 0.001)
    else:
        batters['adj_woba_weather'] = batters['woba']

    return batters

def save_outputs(batters, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"batters_{label}_weather.csv"
    logfile = out_path / f"log_weather_{label}.txt"

    batters.to_csv(outfile, index=False)

    with open(logfile, 'w') as f:
        f.write(str(batters[['last_name, first_name', 'team', 'adj_woba_weather']].head()))

def main():
    weather = pd.read_csv("data/weather_adjustments.csv")
    for label in ['home', 'away']:
        infile = f"data/adjusted/batters_{label}.csv"
        batters = pd.read_csv(infile)
        adjusted = apply_weather_adjustments(batters, weather)
        save_outputs(adjusted, label)

if __name__ == "__main__":
    main()
