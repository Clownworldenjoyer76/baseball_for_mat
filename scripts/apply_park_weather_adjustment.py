import pandas as pd
from pathlib import Path
from apply_adjustments import apply_adjustments
import subprocess

def save_with_logging(batters, label):
    output_path = Path("data/adjusted")
    output_path.mkdir(parents=True, exist_ok=True)
    out_file = output_path / f"batters_{label}_adjusted.csv"
    log_file = output_path / f"adjustment_log_{label}.txt"

    batters.to_csv(out_file, index=False)
    print(f"📁 Saved: {out_file}", flush=True)

    print(f"\n✅ {label.capitalize()} Columns after adjustment:", flush=True)
    print(batters.columns.tolist(), flush=True)

    sample_cols = ['last_name, first_name', 'team', 'adj_woba', 'adj_home_run', 'adj_hard_hit_percent']
    available_cols = [col for col in sample_cols if col in batters.columns]

    print(f"\n📊 Sample rows ({label}):", flush=True)
    print(batters[available_cols].head().to_string(index=False), flush=True)

    with open(log_file, "w") as f:
        f.write(f"✅ Columns after adjustment:\n{batters.columns.tolist()}\n\n")
        f.write(f"📊 Sample rows:\n{batters[available_cols].head().to_string(index=False)}\n\n")
        if 'adj_woba' in batters.columns:
            f.write(f"Mean adj_woba: {batters['adj_woba'].mean():.4f}\n")
        if 'adj_home_run' in batters.columns:
            f.write(f"Mean adj_home_run: {batters['adj_home_run'].mean():.4f}\n")
        if 'adj_hard_hit_percent' in batters.columns:
            f.write(f"Mean adj_hard_hit_percent: {batters['adj_hard_hit_percent'].mean():.4f}\n")
        f.write("✅ Adjustment complete.\n")

    print(f"✅ {label.capitalize()} file + log written to data/adjusted/", flush=True)

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted/*.csv", "data/adjusted/*.txt"], check=True)
        subprocess.run(["git", "commit", "-m", "Add adjusted batter files and logs"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted files.")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    print("Loading data...", flush=True)

    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    for label in ["home", "away"]:
        path = f"data/adjusted/batters_{label}.csv"
        print(f"\n📥 Loading {label} batters from: {path}", flush=True)

        if not Path(path).exists():
            raise FileNotFoundError(f"❌ File not found: {path}")

        batters = pd.read_csv(path)
        if 'team' not in batters.columns:
            raise ValueError(f"❌ Required column 'team' missing in {label} batter file")

        print(f"{label.capitalize()}: {len(batters)}, ParkDay: {len(park_day)}, Weather: {len(weather)}", flush=True)
        print(f"⚙️ Applying adjustments to {label} batters...", flush=True)

        adjusted = apply_adjustments(batters, weather, park_day, park_night)
        save_with_logging(adjusted, label)

    commit_outputs()

if __name__ == "__main__":
    main()
