import pandas as pd
from pathlib import Path
from apply_adjustments import apply_adjustments

def process_batters(filepath, label):
    print(f"\n📥 Loading {label} batters from: {filepath}", flush=True)
    batters = pd.read_csv(filepath)
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"{label}: {len(batters)}, ParkDay: {len(park_day)}, Weather: {len(weather)}", flush=True)
    print(f"⚙️ Applying adjustments to {label} batters...", flush=True)

    adjusted = apply_adjustments(batters, weather, park_day, park_night)

    output_path = Path("data/adjusted")
    output_path.mkdir(parents=True, exist_ok=True)
    out_csv = output_path / f"batters_{label}_adjusted.csv"
    adjusted.to_csv(out_csv, index=False)

    print(f"\n✅ {label.capitalize()} Columns after adjustment:", flush=True)
    print(adjusted.columns.tolist(), flush=True)

    sample_cols = ['last_name, first_name', 'team', 'adj_woba', 'adj_home_run', 'adj_hard_hit_percent']
    available_cols = [col for col in sample_cols if col in adjusted.columns]

    print(f"\n📊 Sample rows ({label}):", flush=True)
    print(adjusted[available_cols].head().to_string(index=False), flush=True)

    log_path = output_path / f"adjustment_log_{label}.txt"
    with open(log_path, "w") as f:
        f.write(f"✅ {label.capitalize()} Columns after adjustment:\n")
        f.write(str(adjusted.columns.tolist()) + "\n\n")
        f.write("📊 Sample rows:\n")
        f.write(adjusted[available_cols].head().to_string(index=False) + "\n\n")
        if 'adj_woba' in adjusted.columns:
            f.write(f"Mean adj_woba: {adjusted['adj_woba'].mean():.4f}\n")
        if 'adj_home_run' in adjusted.columns:
            f.write(f"Mean adj_home_run: {adjusted['adj_home_run'].mean():.4f}\n")
        if 'adj_hard_hit_percent' in adjusted.columns:
            f.write(f"Mean adj_hard_hit_percent: {adjusted['adj_hard_hit_percent'].mean():.4f}\n")

    print(f"✅ {label.capitalize()} file + log written to {output_path}/", flush=True)

def main():
    process_batters("data/adjusted/batters_home.csv", "home")
    process_batters("data/adjusted/batters_away.csv", "away")

if __name__ == "__main__":
    main()
