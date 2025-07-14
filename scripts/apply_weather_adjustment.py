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
    output_dir = Path("data/weather_adjusted")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"batters_{label}_weather.csv"
    log_file = output_dir / f"log_weather_{label}.txt"

    batters.to_csv(output_file, index=False)

    with open(log_file, "w") as f:
        f.write("✅ Weather Adjustment Output Sample:\n\n")
        f.write(batters[['last_name, first_name', 'team', 'adj_woba_weather']].head().to_string(index=False))
        f.write("\n\n✅ Adjustment complete.\n")

    print(f"📁 Saved: {output_file}")
    print(f"📝 Log: {log_file}")

def main():
    weather = pd.read_csv("data/weather_adjustments.csv")
    for label in ["home", "away"]:
        input_file = f"data/adjusted/batters_{label}.csv"
        print(f"\n📥 Loading: {input_file}")
        batters = pd.read_csv(input_file)
        adjusted = apply_weather_adjustments(batters, weather)
        save_outputs(adjusted, label)

if __name__ == "__main__":
    main()
