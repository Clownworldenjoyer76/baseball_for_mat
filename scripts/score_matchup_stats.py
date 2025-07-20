import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

REQUIRED_COLUMNS = ["name", "team", "adj_woba_combined", "type"]

def assign_prop_type(row):
    if row["type"] == "batter":
        woba = row["adj_woba_combined"]
        if woba >= 0.400:
            return "home run"
        elif woba >= 0.330:
            return "total bases"
        else:
            return "hits"
    elif row["type"] == "pitcher":
        return "strikeouts" if "strikeout" in row and pd.notna(row["strikeout"]) else "outs"
    else:
        return "unknown"

def build_pick(row):
    return f"{row['name']} over {row['prop_type']}"

def main():
    df = pd.read_csv(INPUT_FILE)

    # Drop if missing required data
    df = df.dropna(subset=REQUIRED_COLUMNS)

    if df.empty:
        print("❌ No valid rows after filtering required fields.")
        return

    # Assign prop type and pick
    df["prop_type"] = df.apply(assign_prop_type, axis=1)
    df["type"] = "prop"
    df["pick"] = df.apply(build_pick, axis=1)

    # Output
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(df)} props to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
