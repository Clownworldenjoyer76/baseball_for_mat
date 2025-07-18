import pandas as pd
import os

# Input
MATCHUP_FILE = "data/final/matchup_stats.csv"

# Output
OUTPUT_FILE = "data/final/best_picks_raw.csv"

# Scoring weights
WEIGHTS = {
    "adj_woba_combined": 0.4,
    "adj_woba_park": 0.3,
    "park_factor": 0.1,
    "weather_factor": 0.1,
    "sharp_tag": 0.1
}

def normalize(val, min_val, max_val):
    if pd.isna(val): return 0
    if max_val == min_val: return 0
    return (val - min_val) / (max_val - min_val)

def score_row(row, mins, maxs):
    score = 0
    score += WEIGHTS["adj_woba_combined"] * normalize(row.get("adj_woba_combined", 0), mins["adj_woba_combined"], maxs["adj_woba_combined"])
    score += WEIGHTS["adj_woba_park"] * normalize(row.get("adj_woba_park", 0), mins["adj_woba_park"], maxs["adj_woba_park"])
    score += WEIGHTS["park_factor"] * normalize(row.get("park_factor", 0), mins["park_factor"], maxs["park_factor"])
    score += WEIGHTS["weather_factor"] * normalize(row.get("weather_factor", 0), mins["weather_factor"], maxs["weather_factor"])
    score += WEIGHTS["sharp_tag"] * (1 if row.get("sharp_tag", "") == "🟢" else 0)
    return round(score, 4)

def main():
    df = pd.read_csv(MATCHUP_FILE)

    # Ensure key cols exist
    required_cols = ["adj_woba_combined", "adj_woba_park", "park_factor", "weather_factor", "sharp_tag"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0

    # Min/max for normalization
    mins = {col: df[col].min() for col in WEIGHTS if col in df.columns}
    maxs = {col: df[col].max() for col in WEIGHTS if col in df.columns}

    # Apply scoring
    df["score"] = df.apply(lambda row: score_row(row, mins, maxs), axis=1)

    # Sort by game, score descending
    df_sorted = df.sort_values(by=["home_team", "score"], ascending=[True, False])

    # Save output
    os.makedirs("data/final", exist_ok=True)
    df_sorted.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved: {OUTPUT_FILE} with {len(df_sorted)} rows.")

if __name__ == "__main__":
    main()
