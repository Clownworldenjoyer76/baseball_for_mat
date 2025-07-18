import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

# All columns your downstream processes depend on
REQUIRED_COLUMNS = [
    "type",
    "pick",
    "adj_woba_combined",
    "player_id_weather",
    "player_id_park",
    "name",
    "team",
    "stat"
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
                df[col] = 100  # Default neutral wOBA
            elif col == "stat":
                df[col] = "unknown"
            else:
                df[col] = "unknown"
    return df

def filter_and_score_props(df):
    prop_df = df[df["name"].notnull()].copy()
    prop_df["type"] = "prop"
    prop_df["pick"] = prop_df["name"] + " over " + prop_df["stat"]
    return prop_df

def main():
    df = pd.read_csv(INPUT_FILE)
    df = inject_missing_columns(df, REQUIRED_COLUMNS)

    # Generate props
    prop_picks = filter_and_score_props(df)

    # Combine all picks
    all_picks = pd.concat([df, prop_picks], ignore_index=True)

    # Output
    all_picks.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
