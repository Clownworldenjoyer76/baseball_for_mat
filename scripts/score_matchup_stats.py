import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

REQUIRED_COLUMNS = ["name", "team", "adj_woba_combined"]

def main():
    df = pd.read_csv(INPUT_FILE)

    # Drop any rows missing required fields
    df = df.dropna(subset=REQUIRED_COLUMNS)

    if df.empty:
        print("❌ No valid rows after filtering required fields.")
        return

    # Assign default prop type (must exist for downstream scripts)
    df["prop_type"] = "total bases"

    # Build pick and tag as prop
    df["type"] = "prop"
    df["pick"] = df["name"] + " over " + df["prop_type"]

    # Output
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(df)} props to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
