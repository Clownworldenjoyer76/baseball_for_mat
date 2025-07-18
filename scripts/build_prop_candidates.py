import pandas as pd
from pathlib import Path

INPUT_FILE = "data/final/best_picks_raw.csv"
OUTPUT_FILE = "data/final/prop_candidates.csv"

prop_keywords = ["total bases", "hits", "singles", "home runs", "strikeouts"]

def extract_pick_stat(row, search_columns):
    for col in search_columns:
        val = str(row.get(col, "")).lower()
        for kw in prop_keywords:
            if kw in val:
                return (val.strip(), kw)
    return (None, None)

def extract_prop_candidates(df):
    # Filter rows where player and adj_woba_combined exist
    df = df[df["last_name, first_name_weather"].notnull() & df["adj_woba_combined"].notnull()]

    df["name"] = df["last_name, first_name_weather"]
    df["team"] = df["team"].str.title()

    # Dynamically identify text columns to search for keywords
    search_columns = [col for col in df.columns if any(x in col.lower() for x in ["description", "type", "line", "detail"])]

    picks = df.apply(lambda row: extract_pick_stat(row, search_columns), axis=1)
    df["pick"] = picks.apply(lambda x: x[0])
    df["stat"] = picks.apply(lambda x: x[1])

    df = df[df["pick"].notnull() & df["stat"].notnull()]

    # Select only existing columns
    base_cols = ["name", "team", "pick", "stat"]
    optional_cols = [col for col in ["score", "type"] if col in df.columns]
    output_cols = base_cols + optional_cols

    return df[output_cols]

def main():
    df = pd.read_csv(INPUT_FILE)
    props = extract_prop_candidates(df)
    props.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved prop candidates to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
