# pit1.py

import pandas as pd
from pathlib import Path

# Corrected input file paths
HWP_FILE = Path("data/end_chain/first/pit_hwp.csv")
AWP_FILE = Path("data/end_chain/first/pit_awp.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/end_chain/final/startingpitchers.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    return name.strip().lower()

def main():
    # Load home and away pitcher data
    hwp = load_csv(HWP_FILE)
    awp = load_csv(AWP_FILE)

    # Add context columns
    hwp["team_context"] = "home"
    awp["team_context"] = "away"

    # Standardize 'team' column for merge alignment
    hwp.rename(columns={"home_team": "team"}, inplace=True)
    awp.rename(columns={"away_team": "team"}, inplace=True)

    # Combine both datasets
    today_pitchers = pd.concat([hwp, awp], ignore_index=True)
    today_pitchers["name_key"] = today_pitchers["last_name, first_name"].apply(normalize_name)

    # Load xtra pitcher stats
    xtra = load_csv(XTRA_FILE)
    xtra["name_key"] = xtra["last_name, first_name"].apply(normalize_name)

    # Ensure 'team' exists in xtra
    if "team" not in xtra.columns:
        xtra["team"] = None

    # Merge stats into today's pitchers
    merged = pd.merge(
        today_pitchers,
        xtra.drop(columns=["last_name, first_name", "name"], errors="ignore"),
        on="name_key",
        how="left",
        suffixes=("", "_xtra")
    )

    # Drop helper column
    merged.drop(columns=["name_key"], inplace=True)

    # Save to output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Merged pitcher data saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
