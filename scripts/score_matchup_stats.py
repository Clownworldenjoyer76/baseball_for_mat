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
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"📄 Columns found: {list(df.columns)}")

    required_cols = ["away_team", "home_team", "pick"]
    for col in required_cols:
        if col not in df.columns:
            print(f"❌ Missing required column: {col}")
            return

    best_picks = []
    for i, row in df.iterrows():
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
        except Exception as e:
            print(f"⚠️ Skipped row {i}: {e}")
            continue

    if not best_picks:
        print("⚠️ No picks processed. Check column names or data.")
    else:
        out_df = pd.DataFrame(best_picks)
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Saved: {OUTPUT_FILE} with {len(out_df)} rows")

if __name__ == "__main__":
    main()
