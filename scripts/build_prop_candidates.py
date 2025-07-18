import pandas as pd
from pathlib import Path

INPUT_FILE = "data/final/best_picks_raw.csv"
OUTPUT_FILE = "data/final/prop_candidates.csv"

def extract_prop_candidates(df):
    # Use last_name, first_name_weather as the player name
    if "last_name, first_name_weather" not in df.columns:
        raise ValueError("Missing column: 'last_name, first_name_weather'")

    df = df[df["last_name, first_name_weather"].notnull()]
    df = df[df["adj_woba_combined"].notnull()]

    df["name"] = df["last_name, first_name_weather"]
    df["team"] = df["team"].str.title()

    # Match against relevant prop keywords
    prop_keywords = ["total bases", "hits", "singles", "home runs", "strikeouts"]
    def is_prop(val):
        val = str(val).lower()
        return any(kw in val for kw in prop_keywords)

    df = df[df["pick"].apply(is_prop)]

    # Infer stat from pick
    def infer_stat(pick):
        pick = str(pick).lower()
        for kw in prop_keywords:
            if kw in pick:
                return kw
        return "other"

    df["stat"] = df["pick"].apply(infer_stat)
    df["type"] = "prop"

    # Final columns for output
    output_cols = ["name", "team", "pick", "stat", "score", "type"]
    df = df[output_cols]

    return df

def main():
    df = pd.read_csv(INPUT_FILE)
    props = extract_prop_candidates(df)
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    props.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved prop candidates to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
