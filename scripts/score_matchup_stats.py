import pandas as pd
import os

INPUT_FILE = "data/final/matchup_stats.csv"
OUTPUT_FILE = "data/final/best_picks_raw.csv"

def determine_type(pick):
    if "ML" in pick:
        return "moneyline"
    elif " O" in pick or " U" in pick:
        return "total"
    elif "Over" in pick or "Under" in pick:
        return "prop"
    else:
        return "spread"

def main():
    df = pd.read_csv(INPUT_FILE)

    best_picks = []

    for _, row in df.iterrows():
        try:
            pick = row["pick"]
            score = row.get("score", 0)
            tag = row.get("tag", "")
            game_id = f"{row['away_team']}@{row['home_team']}".replace(" ", "")

            best_picks.append({
                "game_id": game_id,
                "pick": pick,
                "score": score,
                "tag": tag,
                "type": determine_type(pick)
            })

        except KeyError as e:
            continue

    out_df = pd.DataFrame(best_picks)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved: {OUTPUT_FILE} with {len(out_df)} rows")

if __name__ == "__main__":
    main()
