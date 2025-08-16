#!/usr/bin/env python3
# scripts/final_props_1.py
#
# Purpose: Select top 5 player props per game (batters only) with a prop-mix rule
#          and write ONLY today's games to data/bets/player_props_history.csv.

import pandas as pd
from pathlib import Path
from datetime import datetime
try:
    # Python 3.9+ standard library
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # fallback handled below

# ---------- File paths ----------
BATTER_FILE = Path("data/bets/prep/batter_props_final.csv")
SCHED_FILE  = Path("data/bets/mlb_sched.csv")
PLAYER_OUT  = Path("data/bets/player_props_history.csv")

# ---------- Config ----------
TZ_NAME = "America/New_York"

# ---------- Columns in player output ----------
PLAYER_COLUMNS = [
    "player_id", "name", "team", "prop", "line", "value",
    "over_probability", "date", "game_id", "prop_correct", "prop_sort"
]

def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    return df

def _today_str() -> str:
    if ZoneInfo is not None:
        now_local = datetime.now(ZoneInfo(TZ_NAME))
    else:
        # Fallback: naive local time; still format as YYYY-MM-DD
        now_local = datetime.now()
    return now_local.strftime("%Y-%m-%d")

def _pick_top5_with_mix(df_game: pd.DataFrame) -> pd.DataFrame:
    """
    Per-game selection:
      • Try to include 1 'hits' and 1 'home_runs' if present.
      • Cap 'total_bases' at most 3 in the final 5.
      • Fill remaining slots by highest over_probability.
      • Mark top 3 as 'Best Prop'.
    """
    df = df_game.sort_values("over_probability", ascending=False).copy()

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

    # Required columns
    for col in ["prop", "over_probability", "team"]:
        if col not in batters.columns:
            raise SystemExit(f"❌ {BATTER_FILE} missing column '{col}'")
    need_sched = [c for c in ("home_team", "away_team", "date", "game_id") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    # Normalize types
    batters["over_probability"] = pd.to_numeric(batters["over_probability"], errors="coerce")
    batters["team"] = batters["team"].astype(str).str.strip().str.lower()

    # ----- Choose "today" from schedule -----
    sched["date"] = pd.to_datetime(sched["date"], errors="coerce")
    if sched["date"].isna().all():
        raise SystemExit("❌ schedule 'date' column could not be parsed")

    today_str = _today_str()
    today_dt  = pd.to_datetime(today_str)

    sched_today = sched[sched["date"] == today_dt].copy()
    if sched_today.empty:
        # Fallback: use the latest available date in schedule
        latest = sched["date"].max()
        sched_today = sched[sched["date"] == latest].copy()
        print(f"⚠️ No rows for today ({today_str}) in schedule; using latest date {latest.date()} instead.")
    else:
        print(f"✅ Using schedule for today: {today_str}")

    # Map team → (date, game_id) for TODAY ONLY
    team_map = pd.concat([
        sched_today[["home_team", "date", "game_id"]].rename(columns={"home_team": "team"}),
        sched_today[["away_team", "date", "game_id"]].rename(columns={"away_team": "team"}),
    ], ignore_index=True).drop_duplicates()

    team_map["team"] = team_map["team"].astype(str).str.strip().str.lower()

    # ----- Join batters to today's schedule -----
    merged = batters.merge(team_map, on="team", how="left")

    # Strictly keep ONLY scheduled teams (drop NaN game_id)
    before = len(merged)
    merged = merged[merged["game_id"].notna()].copy()
    after = len(merged)
    if after < before:
        print(f"🧹 Dropped {before - after} props not on today's schedule.")

    # Optional hygiene: drop rows with missing/invalid over_probability
    merged = merged[pd.to_numeric(merged["over_probability"], errors="coerce").notna()].copy()

    # Sort for stable grouping
    merged = merged.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # ----- Group by game and select top 5 with mix -----
    top_chunks = []
    for gid, df_game in merged.groupby("game_id", dropna=False):
        top_chunks.append(_pick_top5_with_mix(df_game))

    top_props = pd.concat(top_chunks, ignore_index=True) if top_chunks else merged.head(0).copy()

    # Prepare output schema
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
