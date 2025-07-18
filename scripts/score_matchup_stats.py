import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

# Required columns (confirmed to exist)
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
            print(f"Injecting missing column: {col}")
            if col == "type":
                df[col] = "undecided"
            elif col == "pick":
                df[col] = "TBD"
            elif col == "adj_woba_combined":
                df[col] = None
            else:
                df[col] = "unknown"
    return df

def filter_and_score_props(df):
    df = df[df["name"].notnull() & df["team"].notnull()].copy()
    df["type"] = "prop"
    df["pick"] = df["name"] + " over TBD"
    return df

def main():
    df = pd.read_csv(INPUT_FILE)
    df = inject_missing_columns(df, REQUIRED_COLUMNS)
    prop_picks = filter_and_score_props(df)
    prop_picks.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
