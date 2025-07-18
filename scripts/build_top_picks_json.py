import pandas as pd
import json
import subprocess
from pathlib import Path

INPUT_CSV = "data/final/best_picks_raw.csv"
OUTPUT_JSON = "data/output/top_picks.json"
REQUIRED_COLUMNS = ["home_team", "away_team", "type", "pick"]

def main():
    df = pd.read_csv(INPUT_CSV)

    # Validate required columns
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df["game_id"] = df["away_team"] + "_at_" + df["home_team"]

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

    top_3 = df.head(3).to_dict(orient="records")

    output = {
        "top_3_picks": top_3,
        "games": list(games.values())
    }

    Path(OUTPUT_JSON).parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    print(f"✅ top_picks.json saved to {OUTPUT_JSON}")

    subprocess.run(["git", "add", OUTPUT_JSON])
    subprocess.run(["git", "commit", "-m", "📦 Add top_picks.json"])
    subprocess.run(["git", "push"])

if __name__ == "__main__":
    main()
