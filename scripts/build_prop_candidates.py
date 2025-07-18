import pandas as pd
from pathlib import Path

INPUT_FILE = "data/final/best_picks_raw.csv"
OUTPUT_FILE = "data/final/prop_candidates.csv"

def extract_prop_candidates(df):
    # Only keep rows with a name and a valid adj_woba_combined value
    df = df[df["name"].notnull() & df["adj_woba_combined"].notnull()]

    # Rename "team" to title case to match existing convention
    df["team"] = df["team"].str.title()

    # Basic keyword match on the 'pick' or 'stat' column
    keyword_cols = [col for col in df.columns if "stat" in col or "pick" in col or "description" in col]
    prop_keywords = ["total bases", "hits", "singles", "home runs", "strikeouts"]

    def is_candidate(row):
        for col in keyword_cols:
            val = str(row.get(col, "")).lower()
            if any(kw in val for kw in prop_keywords):
                return True
        return False

    return df[df.apply(is_candidate, axis=1)]

def main():
    df = pd.read_csv(INPUT_FILE)
    props = extract_prop_candidates(df)
    props.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved prop candidates to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
