import pandas as pd
import os

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/scored/best_picks_raw.csv"
REQUIRED_COLUMNS = ["pick", "type"]

def calculate_score(row):
    score = 0

    # Pitcher edge
    if "adj_woba_combined" in row and not pd.isna(row["adj_woba_combined"]):
        score += (100 - row["adj_woba_combined"]) * 0.4

    # Sharp/public indicator
    if "public_flag" in row:
        if row["public_flag"] == "🟢":
            score += 15
        elif row["public_flag"] == "🟡":
            score += 5
        elif row["public_flag"] == "🔴":
            score -= 10

    # Weather & park influence
    if "weather_factor" in row and not pd.isna(row["weather_factor"]):
        score += row["weather_factor"] * 0.1
    if "park_factor" in row and not pd.isna(row["park_factor"]):
        score += row["park_factor"] * 0.1

    return round(score, 2)

def main():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            if col == "type":
                df[col] = "undecided"
            else:
                raise ValueError(f"Missing required column: {col}")

    df["score"] = df.apply(calculate_score, axis=1)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Scored output saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
