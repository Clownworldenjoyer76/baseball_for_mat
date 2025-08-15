#!/usr/bin/env python3
# scripts/final_props_1.py
#
# Purpose: Select top 5 player props per game with a prop-mix rule and
#          write data/bets/player_props_history.csv (players only).

import pandas as pd
from pathlib import Path

# ---------- File paths ----------
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
SCHED_FILE  = Path("data/bets/mlb_sched.csv")

PLAYER_OUT = Path("data/bets/player_props_history.csv")

# ---------- Columns in player output ----------
PLAYER_COLUMNS = [
    "player_id", "name", "team", "prop", "line", "value",
    "over_probability", "date", "game_id", "prop_correct", "prop_sort"
]

def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df

def _pick_top5_with_mix(df_game: pd.DataFrame) -> pd.DataFrame:
    """
    Per-game selection for player history:
      • Try to include 1 'hits' and 1 'home_runs' if present.
      • Cap 'total_bases' at most 3 in the final 5.
      • Fill remaining slots by highest over_probability.
    """
    df = df_game.sort_values("over_probability", ascending=False).copy()

    # If fewer than 5 rows, take what we have and still mark "Best Prop"
    if len(df) <= 5:
        selected = df.copy()
        selected["prop_sort"] = "game"
        selected.loc[selected.index[: min(3, len(selected))], "prop_sort"] = "Best Prop"
        return selected

    picks: list[int] = []

    def _take_best(prop_name: str, k: int = 1):
        nonlocal picks
        mask = (df["prop"].str.lower() == prop_name) & (~df.index.isin(picks))
        pool = df[mask]
        if not pool.empty:
            picks.extend(list(pool.head(k).index))

    # Reserve 1 Hits, 1 HR if available
    _take_best("hits", k=1)
    _take_best("home_runs", k=1)

    # Fill remaining, respecting TB cap
    MAX_TB = 3
    for idx, row in df.iterrows():
        if len(picks) >= 5:
            break
        if idx in picks:
            continue
        if str(row.get("prop", "")).lower() == "total_bases":
            tb_count = int((df.loc[picks, "prop"].str.lower() == "total_bases").sum()) if picks else 0
            if tb_count >= MAX_TB:
                continue
        picks.append(idx)

    selected = df.loc[picks].copy().sort_values("over_probability", ascending=False)
    selected["prop_sort"] = "game"
    selected.loc[selected.index[: min(3, len(selected))], "prop_sort"] = "Best Prop"
    return selected

def main():
    # ----- Load inputs -----
    batters = _std(pd.read_csv(BATTER_FILE))
    sched   = _std(pd.read_csv(SCHED_FILE))

    # Required columns (players)
    for col in ["prop", "over_probability", "team"]:
        if col not in batters.columns:
            raise SystemExit(f"❌ {BATTER_FILE} missing column '{col}'")
    batters["over_probability"] = pd.to_numeric(batters["over_probability"], errors="coerce")

    # Required columns (schedule)
    need_sched = [c for c in ("home_team", "away_team", "date", "game_id") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    # Map team → (date, game_id) from schedule (both home and away roles)
    team_map = pd.concat([
        sched[["home_team", "date", "game_id"]].rename(columns={"home_team": "team"}),
        sched[["away_team", "date", "game_id"]].rename(columns={"away_team": "team"}),
    ], ignore_index=True).drop_duplicates()
    team_map["team"] = team_map["team"].astype(str).str.strip().str.lower()

    batters["team"] = batters["team"].astype(str).str.strip().str.lower()
    merged = batters.merge(team_map, on="team", how="left")

    merged = merged.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # Group by game and select
    top_chunks = []
    for gid, df_game in merged.groupby("game_id", dropna=False):
        top_chunks.append(_pick_top5_with_mix(df_game))
    top_props = pd.concat(top_chunks, ignore_index=True) if top_chunks else merged.head(0).copy()

    # Prepare player output
    top_props["prop_correct"] = ""
    for col in PLAYER_COLUMNS:
        if col not in top_props.columns:
            top_props[col] = ""
    player_out = top_props[PLAYER_COLUMNS].copy()

    PLAYER_OUT.parent.mkdir(parents=True, exist_ok=True)
    player_out.to_csv(PLAYER_OUT, index=False)

    print(f"✅ Wrote players → {PLAYER_OUT} (rows={len(player_out)})")

if __name__ == "__main__":
    main()
