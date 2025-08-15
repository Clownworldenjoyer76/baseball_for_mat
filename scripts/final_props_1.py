#!/usr/bin/env python3
# scripts/final_props_1.py

import pandas as pd
from pathlib import Path

# ---------- File paths ----------
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")   # not used directly yet
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
    Per-game selection:
      • Try to include 1 'hits' and 1 'home_runs' if present.
      • Cap 'total_bases' at most 3 in the final 5.
      • Fill remaining slots by highest over_probability (stable).
    """
    # Sort by probability desc, stable
    df = df_game.sort_values("over_probability", ascending=False).copy()

    # Guard: if fewer than 5 rows exist, just take what's there (still mark Best Prop)
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
            sel = list(pool.head(k).index)
            picks.extend(sel)

    # 1) Reserve 1 Hits, 1 HR if available
    _take_best("hits", k=1)
    _take_best("home_runs", k=1)

    # 2) Fill remaining by probability, but keep TB cap
    MAX_TB = 3
    for idx, row in df.iterrows():
        if len(picks) >= 5:
            break
        if idx in picks:
            continue
        if str(row.get("prop", "")).lower() == "total_bases":
            tb_count = 0
            if picks:
                tb_count = int((df.loc[picks, "prop"].str.lower() == "total_bases").sum())
            if tb_count >= MAX_TB:
                continue
        picks.append(idx)

    # Build selected set and mark “Best Prop” = top 3 by prob among selected
    selected = df.loc[picks].copy()
    selected = selected.sort_values("over_probability", ascending=False)
    selected["prop_sort"] = "game"
    if len(selected) > 0:
        selected.loc[selected.index[: min(3, len(selected))], "prop_sort"] = "Best Prop"
    return selected

def main():
    # Load data
    batters = _std(pd.read_csv(BATTER_FILE))
    sched = _std(pd.read_csv(SCHED_FILE))

    # Basic checks
    for col in ["prop", "over_probability", "team"]:
        if col not in batters.columns:
            raise SystemExit(f"❌ {BATTER_FILE} missing column '{col}'")

    # Ensure over_probability is numeric
    batters["over_probability"] = pd.to_numeric(batters["over_probability"], errors="coerce")

    # ---- Bring date/game_id from schedule (non-destructive) ----
    need = [c for c in ("home_team", "away_team", "date", "game_id") if c not in sched.columns]
    if need:
        raise SystemExit(f"❌ schedule missing columns: {need}")

    # Build (team -> date, game_id) map from both home and away perspectives
    home_map = sched[["home_team", "date", "game_id"]].rename(columns={"home_team": "team"})
    away_map = sched[["away_team", "date", "game_id"]].rename(columns={"away_team": "team"})
    sched_map = pd.concat([home_map, away_map], ignore_index=True).drop_duplicates()
    sched_map["team"] = sched_map["team"].astype(str).str.strip().str.lower()

    batters["team"] = batters["team"].astype(str).str.strip().str.lower()
    merged = batters.merge(sched_map, on="team", how="left")

    # Sort primarily by game_id then probability
    merged = merged.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # ---- Group by game and pick with mix ----
    top5_chunks = []
    for gid, df_game in merged.groupby("game_id", dropna=False):
        picked = _pick_top5_with_mix(df_game)
        top5_chunks.append(picked)

    top_props = pd.concat(top5_chunks, ignore_index=True) if top5_chunks else merged.head(0).copy()

    # Add prop_correct column (blank)
    top_props["prop_correct"] = ""

    # Ensure all expected player output columns exist
    for col in PLAYER_COLUMNS:
        if col not in top_props.columns:
            top_props[col] = ""

    # Reorder and persist player history
    player_out = top_props[PLAYER_COLUMNS].copy()
    PLAYER_OUT.parent.mkdir(parents=True, exist_ok=True)
    player_out.to_csv(PLAYER_OUT, index=False)

    # ---- Build game_props_history.csv (one row per game) ----
    def summarize_props(df_sel: pd.DataFrame) -> pd.Series:
        df_sel = df_sel.sort_values("over_probability", ascending=False)
        summary = {
            "game_id": df_sel["game_id"].iloc[0],
            "date": df_sel["date"].iloc[0] if "date" in df_sel.columns else "",
            "num_selected": int(len(df_sel)),
            "hits_selected": int((df_sel["prop"].str.lower() == "hits").sum()),
            "hr_selected": int((df_sel["prop"].str.lower() == "home_runs").sum()),
            "tb_selected": int((df_sel["prop"].str.lower() == "total_bases").sum()),
            "max_over_probability": float(pd.to_numeric(df_sel["over_probability"], errors="coerce").max()),
            "min_over_probability": float(pd.to_numeric(df_sel["over_probability"], errors="coerce").min()),
        }
        best3 = df_sel.head(3)
        summary["best_prop_summary"] = "; ".join(
            f"{r.get('name','')} ({r.get('team','')}) {r.get('prop','')} {r.get('line','')} @ {float(r.get('over_probability', float('nan'))):.3f}"
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
