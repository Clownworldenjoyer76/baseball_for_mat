import pandas as pd
from pathlib import Path

# ---------- File paths ----------
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")
SCHED_FILE = Path("data/bets/mlb_sched.csv")
OUTPUT_FILE = Path("data/bets/player_props_history.csv")

# ---------- Columns in output ----------
OUTPUT_COLUMNS = [
    "player_id", "name", "team", "prop", "line", "value",
    "over_probability", "date", "game_id", "prop_correct", "prop_sort"
]

def main():
    # Load data
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    sched = pd.read_csv(SCHED_FILE)

    # Ensure consistent column names
    for df in [batters, pitchers, sched]:
        df.columns = df.columns.str.strip().str.lower()

    # Merge all props into one DF
    all_props = pd.concat([batters, pitchers], ignore_index=True)

    # If date or game_id missing, fill from mlb_sched.csv
    if "date" not in all_props.columns:
        all_props["date"] = None
    if "game_id" not in all_props.columns:
        all_props["game_id"] = None

    # Fill missing date/game_id using team match to schedule
    sched_map = sched[["team", "date", "game_id"]].drop_duplicates()
    all_props = all_props.merge(
        sched_map,
        on="team",
        how="left",
        suffixes=("", "_sched")
    )
    all_props["date"] = all_props["date"].fillna(all_props["date_sched"])
    all_props["game_id"] = all_props["game_id"].fillna(all_props["game_id_sched"])
    all_props = all_props.drop(columns=["date_sched", "game_id_sched"])

    # Sort by game_id then over_probability
    all_props = all_props.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # Select top 5 per game_id
    top_props = all_props.groupby("game_id").head(5).copy()

    # Assign prop_sort
    top_props["prop_sort"] = "game"
    top_props.loc[top_props.groupby("game_id")["over_probability"].rank(method="first", ascending=False) <= 3, "prop_sort"] = "Best Prop"

    # Add prop_correct column (blank)
    top_props["prop_correct"] = ""

    # Ensure output columns exist
    for col in OUTPUT_COLUMNS:
        if col not in top_props.columns:
            top_props[col] = ""

    # Reorder columns
    top_props = top_props[OUTPUT_COLUMNS]

    # Save output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    top_props.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(top_props)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
