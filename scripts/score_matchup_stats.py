import pandas as pd
import os
import time

INPUT_FILE = "data/final/matchup.csv"
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
    if not os.path.exists(INPUT_FILE):
        print("❌ Input file does not exist:", INPUT_FILE)
        return

    mod_time = os.path.getmtime(INPUT_FILE)
    print("[SCORE SCRIPT START]")
    print("Input file:", INPUT_FILE)
    print("Last modified:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(mod_time)))

    df = pd.read_csv(INPUT_FILE)
    df = df.dropna(subset=REQUIRED_COLUMNS)

    if df.empty:
        print("❌ No valid rows after filtering required fields.")
        return

    df["prop_type"] = df.apply(assign_prop_type, axis=1)
    df["type"] = "prop"
    df["pick"] = df.apply(build_pick, axis=1)

    df.to_csv(OUTPUT_FILE, index=False)

    print("[SCORE SCRIPT COMPLETE]")
    print(f"✅ Saved {len(df)} props to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
