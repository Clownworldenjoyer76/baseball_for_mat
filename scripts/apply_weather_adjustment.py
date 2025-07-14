import pandas as pd
from pathlib import Path

def apply_weather_adjustments(batters, weather):
    if 'team' not in batters.columns:
        raise ValueError("Missing 'team' column in batters file.")
    batters['home_team'] = batters['team']
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
    output_path = Path("data/adjusted")
    output_path.mkdir(parents=True, exist_ok=True)
    batters.to_csv(output_path / f"batters_{label}.csv", index=False)
    with open(output_path / f"log_weather_{label}.txt", "w") as f:
        f.write(str(batters[['last_name, first_name', 'team', 'adj_woba_weather']].head()))

def main():
    weather = pd.read_csv("data/weather_adjustments.csv")
    for label in ['home', 'away']:
        path = f"data/adjusted/batters_{label}.csv"
        batters = pd.read_csv(path)
        adjusted = apply_weather_adjustments(batters, weather)
        save_outputs(adjusted, label)

if __name__ == "__main__":
    main()
