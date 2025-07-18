import pandas as pd
from pathlib import Path

INPUT_FILE = "data/final/best_picks_raw.csv"
OUTPUT_FILE = "data/final/prop_candidates.csv"

def extract_prop_candidates(df):
    # Only keep rows with a name and a valid adj_woba_combined value
    df = df[df["name"].notnull() & df["adj_woba_combined"].notnull()]

    # Normalize team case
    df["team"] = df["team"].str.title()

    # Only keep valid columns (replacing 'stat' with 'prop_type')
    return df[["name", "team", "pick", "prop_type", "type"]]

def main():
    df = pd.read_csv(INPUT_FILE)
    props = extract_prop_candidates(df)
    props.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved prop candidates to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
