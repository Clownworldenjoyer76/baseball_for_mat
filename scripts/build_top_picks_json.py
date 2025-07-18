import pandas as pd
import json
from pathlib import Path

INPUT_CSV = "data/final/best_picks_raw.csv"
OUTPUT_JSON = "data/output/top_picks.json"
REQUIRED_COLUMNS = ["home_team", "away_team", "type", "pick"]

def main():
    # Load data
    df = pd.read_csv(INPUT_CSV)
    
    # Validate required columns
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Derive game_id
    df["game_id"] = df["away_team"] + "_at_" + df["home_team"]

    # Build picks per game
    games = {}
    for _, row in df.iterrows():
        gid = row["game_id"]
        if gid not in games:
            games[gid] = {
                "away_team": row["away_team"],
                "home_team": row["home_team"],
                "picks": []
            }
        games[gid]["picks"].append({
            "type": row["type"],
            "pick": row["pick"]
        })

    # Determine top 3 overall picks
    top_3 = df.head(3).to_dict(orient="records")

    # Final structure
    output = {
        "top_3_picks": top_3,
        "games": list(games.values())
    }

    # Ensure directory exists
    Path(OUTPUT_JSON).parent.mkdir(parents=True, exist_ok=True)

    # Write JSON
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

if __name__ == "__main__":
    main()
