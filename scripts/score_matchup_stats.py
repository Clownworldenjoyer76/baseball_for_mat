import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

REQUIRED_COLUMNS = ["name", "team", "pick", "adj_woba_combined"]

# Keywords to identify prop types
PROP_KEYWORDS = ["total bases", "hits", "singles", "home runs", "strikeouts"]

def label_prop_type(pick_text):
    pick_lower = str(pick_text).lower()
    for keyword in PROP_KEYWORDS:
        if keyword in pick_lower:
            return keyword
    return None

def main():
    df = pd.read_csv(INPUT_FILE)

    # Drop any rows missing core prop fields
    df = df[df["name"].notnull() & df["team"].notnull() & df["pick"].notnull()]

    # Extract valid props
    df["prop_type"] = df["pick"].apply(label_prop_type)
    prop_df = df[df["prop_type"].notnull()].copy()

    if prop_df.empty:
        print("❌ No valid props found.")
        return

    # Tag as props and create clean pick label
    prop_df["type"] = "prop"
    prop_df["pick"] = prop_df["name"] + " over " + prop_df["prop_type"]

    # Output
    prop_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(prop_df)} props to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
