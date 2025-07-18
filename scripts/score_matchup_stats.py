import pandas as pd

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

def score_row(row):
    score = 0
    if row.get("adj_woba_combined"):
        score += row["adj_woba_combined"]
    if row.get("public_money_percent") and row.get("public_bet_percent"):
        diff = row["public_money_percent"] - row["public_bet_percent"]
        score += diff * 0.01
    return score

def main():
    df = pd.read_csv(INPUT_FILE)
    required = ["name", "type", "adj_woba_combined"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df["score"] = df.apply(score_row, axis=1)
    df["pick"] = df["name"] + " to " + df["type"]

    best = df.sort_values("score", ascending=False).head(3)
    best.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
