import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

REQUIRED_COLUMNS = ["name", "team", "adj_woba_combined"]

# Keywords to identify prop types
PROP_KEYWORDS = ["total bases", "hits", "singles", "home runs", "strikeouts"]

def label_prop_type(text):
    text = str(text).lower()
    for keyword in PROP_KEYWORDS:
        if keyword in text:
            return keyword
    return None

def main():
    df = pd.read_csv(INPUT_FILE)

    # Drop any rows missing required fields
    df = df.dropna(subset=REQUIRED_COLUMNS)

    # Apply keyword detection on any relevant fields (fallback to adj_woba_combined for now)
    df["prop_type"] = df["adj_woba_combined"].apply(label_prop_type)

    # Drop rows with no matched prop type
    prop_df = df[df["prop_type"].notnull()].copy()

    if prop_df.empty:
        print("❌ No valid props found.")
        return

    # Build pick and set type
    prop_df["type"] = "prop"
    prop_df["pick"] = prop_df["name"] + " over " + prop_df["prop_type"]

    # Output
    prop_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(prop_df)} props to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
