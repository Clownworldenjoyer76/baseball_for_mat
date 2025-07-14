
import pandas as pd
from pathlib import Path
import subprocess

def apply_park_adjustments(batters, park_factors):
    if 'team' not in batters.columns:
        raise ValueError("Missing 'team' column in batters file.")

    park_factors = park_factors.drop_duplicates(subset='team')
    batters = pd.merge(batters, park_factors, on='team', how='left')

    if 'woba' not in batters.columns:
        batters['woba'] = 0.320

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
    park_factors_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_factors_night = pd.read_csv("data/Data/park_factors_night.csv")
    park_factors = pd.concat([park_factors_day, park_factors_night]).drop_duplicates(subset='team')

    for label in ['home', 'away']:
        infile = f"data/adjusted/batters_{label}_adjusted.csv"
        batters = pd.read_csv(infile)
        adjusted = apply_park_adjustments(batters, park_factors)
        save_outputs(adjusted, label)

    commit_outputs()

if __name__ == "__main__":
    main()
