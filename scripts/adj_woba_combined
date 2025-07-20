import pandas as pd
import os

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/matchup_stats_woba.csv"
LOG_FILE = "data/logs/fix_woba_combined.log"

def main():
    df = pd.read_csv(INPUT_FILE)

    if "adj_woba_weather" not in df.columns or "adj_woba_park" not in df.columns:
        raise ValueError("Missing required columns: adj_woba_weather and/or adj_woba_park")

    df["adj_woba_combined"] = (df["adj_woba_weather"] + df["adj_woba_park"]) / 2
    os.makedirs("data/final", exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    with open(LOG_FILE, "w") as f:
        f.write("[WOBA FIX MODULE COMPLETE]\n")
        f.write(f"Input: {INPUT_FILE}\n")
        f.write(f"Output: {OUTPUT_FILE}\n")
        f.write(f"Rows processed: {len(df)}\n")
        f.write(f"Sample adj_woba_combined: {df['adj_woba_combined'].head(5).tolist()}\n")

    print("[WOBA FIX COMPLETE]")
    print(f"Saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
