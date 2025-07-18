import pandas as pd
from pathlib import Path

INPUT_FILE = "data/final/best_picks_raw.csv"
OUTPUT_FILE = "data/final/prop_candidates.csv"

def extract_prop_candidates(df):
    # Confirm name and wOBA column
    df = df[df["last_name, first_name_weather"].notnull()]
    df = df[df["adj_woba_combined"].notnull()]

    # Build "name" and format team title-case
    df["name"] = df["last_name, first_name_weather"]
    df["team"] = df["team"].str.title()

    # Columns to scan
    search_columns = [col for col in df.columns if any(x in col.lower() for x in ["description", "type", "line", "detail"])]

    prop_keywords = ["total bases", "hits", "singles", "home runs", "strikeouts"]

    def extract_pick_stat(row):
        for col in search_columns:
            val = str(row.get(col, "")).lower()
            for kw in prop_keywords:
                if kw in val:
                    return (val.strip(), kw)
        return (None, None)

    picks = df.apply(extract_pick_stat, axis=1)
    df["pick"] = picks.apply(lambda x: x[0])
    df["stat"] = picks.apply(lambda x: x[1])

    df = df[df["pick"].notnull()]
    df["type"] = "prop"

    return df[["name", "team", "pick", "stat", "score", "type"]]

def main():
    df = pd.read_csv(INPUT_FILE)
    props = extract_prop_candidates(df)
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    props.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved prop candidates to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
