# scripts/final_props_1.py

import pandas as pd
from pathlib import Path

# ---------- File paths ----------
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")   # unchanged; included if you want to merge later
SCHED_FILE = Path("data/bets/mlb_sched.csv")
PLAYER_OUT = Path("data/bets/player_props_history.csv")
GAME_OUT   = Path("data/bets/game_props_history.csv")

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
    Enforce a per-game prop mix:
      - Try to include 1 'hits' and 1 'home_runs' if present.
      - Cap 'total_bases' at most 3 in the final 5.
      - Fill remaining slots by highest over_probability.
    """
    # sort by probability desc, stable
    df = df.sort_values("over_probability", ascending=False).copy()

    picks = []

    def _take_best(prop_name, k=1):
        nonlocal picks
        pool = df[(df["prop"].str.lower() == prop_name) & (~df.index.isin([p for p in picks]))]
        if not pool.empty:
            sel = list(pool.head(k).index)
            picks.extend(sel)

    # 1) Reserve 1 Hits, 1 HR if available
    _take_best("hits", k=1)
    _take_best("home_runs", k=1)

    # 2) Fill remaining by probability, but keep TB cap
    MAX_TB = 3
    while len(picks) < 5 and len(picks) < len(df):
        for idx, row in df.iterrows():
            if idx in picks:
                continue
            if row["prop"].lower() == "total_bases":
                tb_count = sum(df.loc[picks, "prop"].str.lower().eq("total_bases")) if picks else 0
                if tb_count >= MAX_TB:
                    continue
            picks.append(idx)
            if len(picks) == 5:
                break
        break  # dataset already iterated in this pass

    selected = df.loc[picks].copy()

    # Mark "Best Prop": top 3 by probability within the 5
    selected = selected.sort_values("over_probability", ascending=False).copy()
    selected["prop_sort"] = "game"
    selected.loc[selected.index[:3], "prop_sort"] = "Best Prop"
    return selected

def main():
    # Load data
    batters = _std(pd.read_csv(BATTER_FILE))
    sched = _std(pd.read_csv(SCHED_FILE))

    # Ensure expected columns exist in batters
    for col in ["prop","over_probability","team"]:
        if col not in batters.columns:
            raise SystemExit(f"❌ {BATTER_FILE} missing column '{col}'")

    # Bring date/game_id from schedule (non-destructive)
    # Try both home/away mapping and direct team mapping
    sched_basic = sched.copy()
    sched_basic = sched_basic.rename(columns={"home_team":"home_team", "away_team":"away_team"})
    # derive (team -> date, game_id) map using both home and away roles
    home_map = sched_basic[["home_team","date","game_id"]].rename(columns={"home_team":"team"})
    away_map = sched_basic[["away_team","date","game_id"]].rename(columns={"away_team":"team"})
    sched_map = pd.concat([home_map, away_map], ignore_index=True).drop_duplicates()
    sched_map["team"] = sched_map["team"].astype(str).str.strip().str.lower()

    batters["team"] = batters["team"].astype(str).str.strip().str.lower()
    merged = batters.merge(sched_map, on="team", how="left")

    # If still missing date/game_id, keep as NaN (don’t crash)
    merged = merged.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # Group and select top 5 with mix
    top5 = []
    for gid, df in merged.groupby("game_id", dropna=False):
        picked = _pick_top5_with_mix(df)
        top5.append(picked)
    top_props = pd.concat(top5, ignore_index=True) if top5 else merged.head(0).copy()

    # Add prop_correct blank
    top_props["prop_correct"] = ""

    # Ensure output columns exist
    for col in PLAYER_COLUMNS:
        if col not in top_props.columns:
            top_props[col] = ""

    # Reorder and write player history
    player_out = top_props[PLAYER_COLUMNS].copy()
    PLAYER_OUT.parent.mkdir(parents=True, exist_ok=True)
    player_out.to_csv(PLAYER_OUT, index=False)

    # --------- Build game_props_history.csv (new) ----------
    # One row per game_id with a compact summary of selected props
    def summarize_props(df):
        # df is already filtered to top-5 for the game
        df = df.sort_values("over_probability", ascending=False)
        summary = {
            "game_id": df["game_id"].iloc[0],
            "date": df["date"].iloc[0] if "date" in df.columns else "",
            "num_selected": len(df),
            "hits_selected": int((df["prop"].str.lower()=="hits").sum()),
            "hr_selected": int((df["prop"].str.lower()=="home_runs").sum()),
            "tb_selected": int((df["prop"].str.lower()=="total_bases").sum()),
            "max_over_probability": df["over_probability"].max(),
            "min_over_probability": df["over_probability"].min(),
        }
        # Include a compact string of the top 3 “Best Prop”
        best3 = df.sort_values("over_probability", ascending=False).head(3)
        summary["best_prop_summary"] = "; ".join(
            f"{r['name']} ({r['team']}) {r['prop']} {r['line']} @ {r['over_probability']:.3f}"
            for _, r in best3.iterrows()
        )
        return pd.Series(summary)

    game_summary = (
        top_props.groupby("game_id", dropna=False, as_index=False)
        .apply(summarize_props)
        .reset_index(drop=True)
    )

    GAME_OUT.parent.mkdir(parents=True, exist_ok=True)
    game_summary.to_csv(GAME_OUT, index=False)

    print(f"✅ Saved {len(player_out)} player rows → {PLAYER_OUT}")
    print(f"✅ Saved {len(game_summary)} game rows   → {GAME_OUT}")

if __name__ == "__main__":
    main()
