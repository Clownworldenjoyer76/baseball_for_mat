import pandas as pd
import json
import os

# Input and output paths
INPUT_FILE = "data/final/best_picks_raw.csv"
OUTPUT_FILE = "data/final/top_picks.json"

def main():
    # Read input
    df = pd.read_csv(INPUT_FILE)

    # Ensure required columns exist
    required_cols = ["game_id", "score", "type", "description"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Convert score to numeric in case it's not
    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    # Drop rows without valid scores
    df = df.dropna(subset=["score"])

    # Group by game and get top 5 picks per game
    top_5_per_game = (
        df.sort_values("score", ascending=False)
        .groupby("game_id")
        .head(5)
    )

    # Get top 3 overall
    top_3_overall = df.sort_values("score", ascending=False).head(3)

    # Build JSON structure
    result = {
        "top_3_overall": top_3_overall[["game_id", "type", "description", "score"]].to_dict(orient="records"),
        "top_5_per_game": {}
    }

    for game_id, group in top_5_per_game.groupby("game_id"):
        result["top_5_per_game"][game_id] = group[["type", "description", "score"]].to_dict(orient="records")

    # Write to file
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ Saved top picks to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

import subprocess

subprocess.run(["git", "add", OUTPUT_FILE])
subprocess.run(["git", "commit", "-m", "Add top picks JSON output"])
subprocess.run(["git", "push"])
