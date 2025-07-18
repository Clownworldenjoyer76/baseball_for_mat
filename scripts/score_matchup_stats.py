import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

REQUIRED_COLUMNS = ["name", "team", "stat"]

def drop_invalid_rows(df):
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            print(f"Missing required column: {col}. Skipping drop.")
            return df  # Skip drop if any column is missing
    return df[df["name"].notnull() & df["team"].notnull() & df["stat"].notnull()]

def filter_and_score_props(df):
    df["type"] = "prop"
    df["pick"] = df["name"] + " over " + df["stat"]
    return df

def main():
    df = pd.read_csv(INPUT_FILE)

    df = drop_invalid_rows(df)
    props = filter_and_score_props(df)
    props.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
