import pandas as pd
import subprocess

# Input file
MATCHUP_FILE = "data/final/matchup_stats.csv"

# Output file
OUTPUT_FILE = "data/final/best_picks_raw.csv"

def score(row):
    score = 0
    if "adj_woba_combined" in row and not pd.isna(row["adj_woba_combined"]):
        score += row["adj_woba_combined"] * 100
    return round(score, 1)

def main():
    df = pd.read_csv(MATCHUP_FILE)
    df["score"] = df.apply(score, axis=1)
    df_sorted = df.sort_values(by="score", ascending=False)
    df_sorted.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Saved: {OUTPUT_FILE} with {len(df_sorted)} rows")

    # Commit and push the file
    subprocess.run(["git", "add", OUTPUT_FILE])
    subprocess.run(["git", "commit", "-m", "Add best_picks_raw.csv scored output"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
