
import pandas as pd
from pathlib import Path
import subprocess

def load_adjusted_data(filepath):
    df = pd.read_csv(filepath)
    print(f"Loaded {filepath} with columns: {df.columns.tolist()}")
    if 'name' not in df.columns or 'team' not in df.columns:
        raise KeyError("Missing 'name' or 'team' column in file: " + filepath)
    return df[['name', 'team', 'adj_woba_park']], df

def combine_adjustments(weather_df, park_df):
    if 'name' not in park_df.columns or 'team' not in park_df.columns:
        raise KeyError("'name' and 'team' must be present in park_df")
    combined = weather_df.merge(
        park_df[['name', 'team', 'adj_woba_park']],
        how='left',
        on=['name', 'team'],
        suffixes=('', '_park')
    )
    combined['adj_woba_park'] = combined['adj_woba_park'].fillna(combined['woba'])
    combined['adj_woba_combined'] = combined[['adj_woba_park', 'adj_woba_weather']].mean(axis=1)
    return combined

def save_outputs(df, label):
    out_path = Path("data/adjusted")
    out_path.mkdir(parents=True, exist_ok=True)
    outfile = out_path / f"pitchers_{label}_adjusted.csv"
    df.to_csv(outfile, index=False)
    print(f"✅ Saved: {outfile}")
    return outfile

def commit_outputs():
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", "data/adjusted"], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: adjusted pitcher data"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Committed and pushed adjusted pitcher data")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

def main():
    labels = ["home", "away"]
    for label in labels:
        weather_fp = f"data/adjusted/pitchers_{label}_weather.csv"
        park_fp = f"data/adjusted/pitchers_{label}_park.csv"
        weather_df = pd.read_csv(weather_fp)
        park_df, park_raw = load_adjusted_data(park_fp)
        combined = combine_adjustments(weather_df, park_raw)
        save_outputs(combined, label)
    commit_outputs()

if __name__ == "__main__":
    main()
