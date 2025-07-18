import pandas as pd
import json
import os

INPUT_CSV = "data/final/best_picks_raw.csv"
OUTPUT_JSON = "data/json/top_picks.json"
os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

def generate_game_id(row):
    return f"{row['away_team']}@{row['home_team']}".replace(" ", "_").lower()

def main():
    df = pd.read_csv(INPUT_CSV)

    required_columns = ["away_team", "home_team", "type", "pick", "score"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Inject derived game_id
    df["game_id"] = df.apply(generate_game_id, axis=1)

    # Group by game_id and rank scores
    grouped = df.groupby("game_id")
    top_picks = {}

    for game_id, group in grouped:
        group_sorted = group.sort_values(by="score", ascending=False).head(5)
        top_picks[game_id] = group_sorted[["type", "pick", "score"]].to_dict(orient="records")

    with open(OUTPUT_JSON, "w") as f:
        json.dump(top_picks, f, indent=2)

    print(f"✅ Saved: {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
