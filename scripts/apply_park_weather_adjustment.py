
import pandas as pd
from pathlib import Path
from apply_adjustments import apply_adjustments

def main():
    print("Loading data...", flush=True)
    batters = pd.read_csv("data/cleaned/batters_today.csv")
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"Batters: {len(batters)}, ParkDay: {len(park_day)}, Weather: {len(weather)}", flush=True)
    print("Applying adjustments to batters...", flush=True)

    adjusted = apply_adjustments(batters, weather, park_day, park_night)

    output_path = Path("data/adjusted")
    output_path.mkdir(parents=True, exist_ok=True)
    adjusted.to_csv(output_path / "batters_adjusted_weather_park.csv", index=False)

    # Diagnostics to stdout
    print("\n✅ Columns after adjustment:", flush=True)
    print(adjusted.columns.tolist(), flush=True)

    sample_cols = ['last_name, first_name', 'team', 'adj_woba', 'adj_home_run', 'adj_hard_hit_percent']
    available_cols = [col for col in sample_cols if col in adjusted.columns]

    print("\n📊 Sample rows:", flush=True)
    print(adjusted[available_cols].head().to_string(index=False), flush=True)

    # Also save to log file
    log_path = output_path / "adjustment_log.txt"
    with open(log_path, "w") as f:
        f.write("✅ Columns after adjustment:\n")
        f.write(str(adjusted.columns.tolist()) + "\n\n")
        f.write("📊 Sample rows:\n")
        f.write(adjusted[available_cols].head().to_string(index=False) + "\n\n")
        if 'adj_woba' in adjusted.columns:
            f.write(f"Mean adj_woba: {adjusted['adj_woba'].mean():.4f}\n")
        if 'adj_home_run' in adjusted.columns:
            f.write(f"Mean adj_home_run: {adjusted['adj_home_run'].mean():.4f}\n")
        if 'adj_hard_hit_percent' in adjusted.columns:
            f.write(f"Mean adj_hard_hit_percent: {adjusted['adj_hard_hit_percent'].mean():.4f}\n")

    print("✅ Done. Adjusted batter file + log written to data/adjusted/", flush=True)

if __name__ == "__main__":
    main()
