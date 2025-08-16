#!/usr/bin/env python3
# scripts/final_props_select.py
#
# Purpose:
#   For each game_id on today's MLB schedule:
#     - pick highest-prob prop for: Home run, Hits, Total Bases, Pitcher prop
#     - then pick the highest remaining prop from either dataset
#   All selected rows get prop_sort="game".
#   Then, across ALL selected rows, the global top 3 by over_probability
#   are re-labeled prop_sort="Best Prop" (only 3 total).
#
# Inputs:
#   data/bets/prep/batter_props_final.csv   (batters)
#   data/bets/prep/pitcher_props_bets.csv   (pitchers)
#   data/bets/mlb_sched.csv                 (schedule with date & game_id)
#
# Output:
#   data/bets/player_props_history.csv

from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None

# ---------- File paths ----------
BATTER_FILE  = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE = Path("data/bets/prep/pitcher_props_bets.csv")
SCHED_FILE   = Path("data/bets/mlb_sched.csv")
PLAYER_OUT   = Path("data/bets/player_props_history.csv")

TZ_NAME = "America/New_York"

# Output schema
PLAYER_COLUMNS = [
    "player_id", "name", "team", "prop", "line", "value",
    "over_probability", "date", "game_id", "prop_correct", "prop_sort"
]

# Prop name aliases (lower-case compare)
HR_ALIASES  = {"home_runs", "home run", "hr"}
H_ALIASES   = {"hits"}
TB_ALIASES  = {"total_bases", "total bases"}

def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    # keep original casing in values, but compare via lower() when needed
    return df

def _today_str() -> str:
    if ZoneInfo is not None:
        now_local = datetime.now(ZoneInfo(TZ_NAME))
    else:
        now_local = datetime.now()
    return now_local.strftime("%Y-%m-%d")

def _ensure_numeric(df: pd.DataFrame, cols) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def _normalize_team(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()

def _first_best(df: pd.DataFrame, mask: pd.Series) -> list[int]:
    """Return index of the row with max over_probability within mask; [] if none."""
    pool = df[mask].dropna(subset=["over_probability"])
    if pool.empty:
        return []
    idx = pool["over_probability"].idxmax()
    return [idx] if pd.notna(idx) else []

def _select_for_game(df_game: pd.DataFrame) -> pd.DataFrame:
    """
    Per-game selection:
      1) best HR
      2) best Hits
      3) best Total Bases
      4) best Pitcher prop (is_pitcher==True)
      5) best remaining (not already picked)
    Some categories may be missing; we only pick what's available.
    """
    df = df_game.copy()
    df["_prop_lc"] = df["prop"].astype(str).str.strip().str.lower()

    picks: list[int] = []

    # 1) Home run
    picks += _first_best(df, df["_prop_lc"].isin(HR_ALIASES))

    # 2) Hits
    picks += _first_best(df, df["_prop_lc"].isin(H_ALIASES) & (~df.index.isin(picks)))

    # 3) Total Bases
    picks += _first_best(df, df["_prop_lc"].isin(TB_ALIASES) & (~df.index.isin(picks)))

    # 4) Pitcher prop
    if "is_pitcher" in df.columns:
        picks += _first_best(df, (df["is_pitcher"] == True) & (~df.index.isin(picks)))

    # 5) Highest remaining
    remain = df[~df.index.isin(picks)].dropna(subset=["over_probability"])
    if not remain.empty:
        picks.append(remain["over_probability"].idxmax())

    if not picks:
        return df.head(0).copy()

    selected = df.loc[picks].copy()
    selected = selected.sort_values("over_probability", ascending=False)

    # Mark all as "game" initially
    selected["prop_sort"] = "game"
    return selected

def main():
    # ----- Load inputs -----
    if not SCHED_FILE.exists():
        raise SystemExit(f"❌ Missing schedule: {SCHED_FILE}")
    sched = _std(pd.read_csv(SCHED_FILE))

    if not BATTER_FILE.exists():
        raise SystemExit(f"❌ Missing batter props: {BATTER_FILE}")
    bat = _std(pd.read_csv(BATTER_FILE))

    if not PITCHER_FILE.exists():
        raise SystemExit(f"❌ Missing pitcher props: {PITCHER_FILE}")
    pit = _std(pd.read_csv(PITCHER_FILE))

    # ---- Validate schedule
    need_sched = [c for c in ("home_team", "away_team", "date", "game_id") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    # Normalize schedule dates and pick today's slate (fallback to latest)
    sched["date"] = pd.to_datetime(sched["date"], errors="coerce")
    if sched["date"].isna().all():
        raise SystemExit("❌ schedule 'date' column unparseable")

    today = pd.to_datetime(_today_str())
    sched_today = sched[sched["date"] == today].copy()
    if sched_today.empty:
        latest = sched["date"].max()
        sched_today = sched[sched["date"] == latest].copy()
        print(f"⚠️ No schedule for today ({today.date()}); using latest {latest.date()} instead.")
    else:
        print(f"✅ Using schedule for {today.date()}")

    # Build team → (date, game_id) map for the chosen date only
    team_map = pd.concat([
        sched_today[["home_team", "date", "game_id"]].rename(columns={"home_team": "team"}),
        sched_today[["away_team", "date", "game_id"]].rename(columns={"away_team": "team"}),
    ], ignore_index=True).drop_duplicates()
    team_map["team_norm"] = _normalize_team(team_map["team"])

    # ---- Prepare batter props
    for col in ["prop", "team", "over_probability"]:
        if col not in bat.columns:
            raise SystemExit(f"❌ batter file missing '{col}'")
    _ensure_numeric(bat, ["over_probability", "line", "value"])
    bat["team_norm"] = _normalize_team(bat["team"])
    bat["is_pitcher"] = False

    # ---- Prepare pitcher props
    for col in ["prop", "team", "over_probability"]:
        if col not in pit.columns:
            raise SystemExit(f"❌ pitcher file missing '{col}'")
    _ensure_numeric(pit, ["over_probability", "line", "value"])
    pit["team_norm"] = _normalize_team(pit["team"])
    # infer pitcher flag
    if "player_pos" in pit.columns:
        pit["is_pitcher"] = pit["player_pos"].astype(str).str.lower().eq("pitcher")
    else:
        pit["is_pitcher"] = True  # assume everything in this file is a pitcher prop

    # ---- Combine, then attach (date, game_id) via team_map, strict filter to scheduled games
    both = pd.concat([bat, pit], ignore_index=True, sort=False)
    both = both.merge(team_map[["team_norm", "date", "game_id"]], on="team_norm", how="left")

    before = len(both)
    both = both[both["game_id"].notna()].copy()
    after = len(both)
    if after < before:
        print(f"🧹 Dropped {before - after} off-schedule props (no game_id match).")

    # ---- Clean & sort
    both["over_probability"] = pd.to_numeric(both["over_probability"], errors="coerce")
    both = both.dropna(subset=["over_probability"])
    both = both.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # ---- Per-game selection
    chunks = []
    for gid, df_game in both.groupby("game_id", dropna=False):
        sel = _select_for_game(df_game)
        if not sel.empty:
            chunks.append(sel)

    selected = pd.concat(chunks, ignore_index=True) if chunks else both.head(0).copy()

    # ---- All get "game", then globally re-label top 3 as "Best Prop"
    if not selected.empty:
        # Ensure only 3 in total get "Best Prop"
        top3_idx = selected["over_probability"].nlargest(3).index
        selected.loc[:, "prop_sort"] = "game"
        selected.loc[top3_idx, "prop_sort"] = "Best Prop"

    # ---- Construct output schema
    selected["prop_correct"] = ""
    # If 'date' not present from join, copy from schedule selected date
    if "date" not in selected.columns:
        selected["date"] = sched_today["date"].iloc[0]
    # Ensure date is str YYYY-MM-DD
    selected["date"] = pd.to_datetime(selected["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Normalize expected columns; fill missing with ""
    out = selected.copy()
    for col in PLAYER_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    out = out[PLAYER_COLUMNS].copy()

    PLAYER_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(PLAYER_OUT, index=False)

    print(f"✅ Wrote {len(out)} rows → {PLAYER_OUT}")
    # Optional: quick summary per game
    try:
        summary = (
            out.groupby("game_id")["prop"]
              .apply(lambda s: ", ".join(s.astype(str).tolist()))
              .head(10)
              .to_dict()
        )
        print(f"🧾 Example picks by game (first 10): {summary}")
    except Exception:
        pass

if __name__ == "__main__":
    main()
