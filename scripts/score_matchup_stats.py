import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

# Required columns that actually exist
REQUIRED_COLUMNS = [
    "type",
    "pick",
    "adj_woba_combined",
    "player_id_weather",
    "player_id_park",
    "name",
    "team"
]

def inject_missing_columns(df, required_columns):
    for col in required_columns:
        if col not in df.columns:
            print(f"Skipping missing column: {col}")
            df[col] = pd.NA
    return df

def filter_and_score_props(df):
    props = df[df["name"].notnull() & df["team"].notnull() & df["adj_woba_combined"].notnull()].copy()
    props["type"] = "prop"
    props["pick"] = props["name"] + " over"
    return props

def main():
    df = pd.read_csv(INPUT_FILE)
    df = inject_missing_columns(df, REQUIRED_COLUMNS)

    # Only output valid props
    prop_picks = filter_and_score_props(df)

    # Output only valid prop rows
    prop_picks.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Output written to {OUTPUT_FILE} with {len(prop_picks)} valid props")

if __name__ == "__main__":
    main()
