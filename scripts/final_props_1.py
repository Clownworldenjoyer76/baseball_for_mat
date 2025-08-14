# scripts/final_props_1.py

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

# ---------- Helpers ----------
def _std_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df

def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def main():
    # Load inputs and normalize headers
    batters = _std_cols(pd.read_csv(BATTER_FILE))
    pitchers = _std_cols(pd.read_csv(PITCHER_FILE))
    sched = _std_cols(pd.read_csv(SCHED_FILE))

    # Ensure schedule has required columns (create missing date/game_id if absent)
    if "team" not in sched.columns:
        raise ValueError("Schedule file must include a 'team' column.")
    for col in ("date", "game_id"):
        if col not in sched.columns:
            sched[col] = pd.NA

    # Combine props
    all_props = pd.concat([batters, pitchers], ignore_index=True)

    # Normalize join key
    all_props["team"] = all_props.get("team", pd.Series(pd.NA, index=all_props.index)).astype(str).str.strip()
    sched["team"] = sched["team"].astype(str).str.strip()

    # Ensure keys exist before enrichment
    for c in ("date", "game_id"):
        if c not in all_props.columns:
            all_props[c] = pd.NA

    # Build schedule map (no explicit triple-column literal)
    cols_for_map = ["team", "date", "game_id"]
    sched_map = sched.loc[:, [c for c in cols_for_map if c in sched.columns]].drop_duplicates()

    # Enrich missing date/game_id via team match
    merged = all_props.merge(sched_map, on="team", how="left", suffixes=("", "_sched"))
    for c in ("date", "game_id"):
        sched_col = f"{c}_sched"
        if sched_col in merged.columns:
            merged[c] = merged[c].fillna(merged[sched_col])

    # Drop helper columns if present
    drop_cols = [c for c in ("date_sched", "game_id_sched") if c in merged.columns]
    if drop_cols:
        merged = merged.drop(columns=drop_cols)

    # Types / sorting
    merged = _coerce_numeric(merged, ["over_probability", "value", "line"])
    merged = merged.sort_values(
        ["game_id", "over_probability"],
        ascending=[True, False],
        na_position="last"
    )

    # Keep top 5 per game_id (exclude missing game_id)
    top = (
        merged.dropna(subset=["game_id"])
        .groupby("game_id", as_index=False, sort=False)
        .head(5)
        .copy()
    )

    # prop_sort labeling
    ranks = top.groupby("game_id")["over_probability"].rank(method="first", ascending=False)
    top["prop_sort"] = "game"
    top.loc[ranks <= 3, "prop_sort"] = "Best Prop"

    # prop_correct blank
    top["prop_correct"] = ""

    # Ensure all output columns exist
    for col in OUTPUT_COLUMNS:
        if col not in top.columns:
            top[col] = ""

    # Reorder and persist
    top = top[OUTPUT_COLUMNS]
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    top.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(top)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
