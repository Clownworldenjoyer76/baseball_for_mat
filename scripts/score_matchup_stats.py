import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

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
                df[col] = 100
            else:
                df[col] = "unknown"
    return df

def filter_and_score_props(df):
    prop_df = df[df["name"].notna()].copy()

    if prop_df.empty:
        print("⚠️ No valid player props found.")
        return pd.DataFrame()

    prop_df["type"] = "prop"
    prop_df["pick"] = prop_df["name"] + " over " + prop_df["stat"]
    prop_df["score"] = prop_df["adj_woba_combined"].fillna(100)

    return prop_df[[
        "type", "pick", "score", "name", "team", "stat"
    ]].copy()

def main():
    df = pd.read_csv(INPUT_FILE)
    df = inject_missing_columns(df, REQUIRED_COLUMNS)

    # Score and filter props
    prop_picks = filter_and_score_props(df)

    # Score and keep other picks (moneyline, O/U, etc)
    standard_picks = df[df["type"] != "prop"].copy()
    standard_picks["score"] = standard_picks["adj_woba_combined"].fillna(100)

    combined = pd.concat([standard_picks, prop_picks], ignore_index=True)
    combined.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Output written to {OUTPUT_FILE} with {len(combined)} total picks")

if __name__ == "__main__":
    main()
