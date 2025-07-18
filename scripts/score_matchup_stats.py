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
    "team",
    "stat"
]

def drop_invalid_rows(df):
    valid_df = df.copy()
    for col in ["name", "team", "stat"]:
        valid_df = valid_df[valid_df[col].notnull()]
        valid_df = valid_df[valid_df[col].astype(str).str.strip() != ""]
    return valid_df

def filter_and_score_props(df):
    prop_df = df.copy()
    prop_df["type"] = "prop"
    prop_df["pick"] = prop_df["name"] + " over " + prop_df["stat"]
    return prop_df[REQUIRED_COLUMNS]

def main():
    df = pd.read_csv(INPUT_FILE)

    # Drop any rows missing required core fields
    df = drop_invalid_rows(df)

    # If no usable rows remain, write headers and exit
    if df.empty:
        pd.DataFrame(columns=REQUIRED_COLUMNS).to_csv(OUTPUT_FILE, index=False)
        print("⚠️ No valid rows found. Output file created with headers only.")
        return

    # Score props and export
    prop_picks = filter_and_score_props(df)
    prop_picks.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
