#!/usr/bin/env python3
# scripts/final_props_1.py
#
# Output: data/bets/player_props_history.csv
#
# Per game_id, select:
#   1) highest-prob Home run
#   2) highest-prob Hits
#   3) highest-prob Total Bases
#   4) highest-prob Pitcher prop
#   5) highest remaining (either file; excluding the 4 above)
# All selected rows start with prop_sort="game".
# Then the global top 3 by over_probability are re-labeled prop_sort="Best Prop".

from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None

# ---------- Paths ----------
BATTER_FILE   = Path("data/bets/prep/batter_props_final.csv")
PITCHER_FILE  = Path("data/bets/prep/pitcher_props_bets.csv")
SCHED_FILE    = Path("data/bets/mlb_sched.csv")
TEAMMAP_FILE  = Path("data/Data/team_name_master.csv")
PLAYER_OUT    = Path("data/bets/player_props_history.csv")

TZ_NAME = "America/New_York"

# Output schema
PLAYER_COLUMNS = [
    "player_id", "name", "team", "prop", "line", "value",
    "over_probability", "date", "game_id", "prop_correct", "prop_sort"
]

# Prop labels (lowercased compare)
HR_ALIASES = {"home_runs", "home run", "hr"}
H_ALIASES  = {"hits"}
TB_ALIASES = {"total_bases", "total bases"}

# ---------- Helpers ----------
def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df

def _today_str() -> str:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")

def _ensure_num(df: pd.DataFrame, cols) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def _build_team_normalizer(team_map_df: pd.DataFrame):
    # Map any alias to canonical team_name (per team_name_master.csv)
    req = {"team_code", "team_name", "abbreviation", "clean_team_name"}
    miss = [c for c in req if c not in team_map_df.columns]
    if miss:
        raise SystemExit(f"❌ team_name_master.csv missing columns: {miss}")

    alias_to_team = {}

    def _add(key_val, team_name):
        if pd.isna(key_val):
            return
        k = str(key_val).strip().lower()
        if k:
            alias_to_team[k] = team_name

    for _, r in team_map_df.iterrows():
        canon = str(r["team_name"]).strip()
        _add(r["team_code"], canon)
        _add(r["abbreviation"], canon)
        _add(r["clean_team_name"], canon)
        _add(r["team_name"], canon)
        _add(str(r["team_name"]).lower(), canon)

    def normalize_series(s: pd.Series) -> pd.Series:
        return s.astype(str).map(lambda x: alias_to_team.get(str(x).strip().lower(), str(x).strip()))

    return normalize_series

def _first_best(df: pd.DataFrame, mask: pd.Series) -> list[int]:
    pool = df[mask].dropna(subset=["over_probability"])
    if pool.empty:
        return []
    idx = pool["over_probability"].idxmax()
    return [idx] if pd.notna(idx) else []

def _select_for_game(df_game: pd.DataFrame) -> pd.DataFrame:
    df = df_game.copy()
    df["_prop_lc"] = df["prop"].astype(str).str.strip().str.lower()

    picks: list[int] = []
    picks += _first_best(df, df["_prop_lc"].isin(HR_ALIASES))                                   # HR
    picks += _first_best(df, df["_prop_lc"].isin(H_ALIASES) & (~df.index.isin(picks)))          # Hits
    picks += _first_best(df, df["_prop_lc"].isin(TB_ALIASES) & (~df.index.isin(picks)))         # TB
    if "is_pitcher" in df.columns:
        picks += _first_best(df, (df["is_pitcher"] == True) & (~df.index.isin(picks)))          # Pitcher

    remain = df[~df.index.isin(picks)].dropna(subset=["over_probability"])                      # Highest remaining
    if not remain.empty:
        picks.append(remain["over_probability"].idxmax())

    if not picks:
        return df.head(0).copy()

    sel = df.loc[picks].copy().sort_values("over_probability", ascending=False)
    sel["prop_sort"] = "game"
    return sel

# ---------- Main ----------
def main():
    # Team map & normalizer
    if not TEAMMAP_FILE.exists():
        raise SystemExit(f"❌ Missing team map: {TEAMMAP_FILE}")
    teammap = _std(pd.read_csv(TEAMMAP_FILE))
    normalize_series = _build_team_normalizer(teammap)

    # Schedule
    if not SCHED_FILE.exists():
        raise SystemExit(f"❌ Missing schedule: {SCHED_FILE}")
    sched = _std(pd.read_csv(SCHED_FILE))
    need_sched = [c for c in ("home_team", "away_team", "date", "game_id") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    sched["home_team"] = normalize_series(sched["home_team"])
    sched["away_team"] = normalize_series(sched["away_team"])
    sched["date"] = pd.to_datetime(sched["date"], errors="coerce")
    if sched["date"].isna().all():
        raise SystemExit("❌ schedule 'date' column is not parseable")

    today = pd.to_datetime(_today_str())
    sched_today = sched[sched["date"] == today].copy()
    if sched_today.empty:
        latest = sched["date"].max()
        sched_today = sched[sched["date"] == latest].copy()
        print(f"⚠️ No schedule for today ({today.date()}); using latest {latest.date()} instead.")
    else:
        print(f"✅ Using schedule for {today.date()}")

    team_map_sched = pd.concat([
        sched_today[["home_team", "date", "game_id"]].rename(columns={"home_team": "team"}),
        sched_today[["away_team", "date", "game_id"]].rename(columns={"away_team": "team"}),
    ], ignore_index=True).drop_duplicates()

    # Props
    if not BATTER_FILE.exists():
        raise SystemExit(f"❌ Missing batter props: {BATTER_FILE}")
    if not PITCHER_FILE.exists():
        raise SystemExit(f"❌ Missing pitcher props: {PITCHER_FILE}")

    bat = _std(pd.read_csv(BATTER_FILE))
    pit = _std(pd.read_csv(PITCHER_FILE))

    for col in ["prop", "team", "over_probability"]:
        if col not in bat.columns:
            raise SystemExit(f"❌ batter file missing '{col}'")
        if col not in pit.columns:
            raise SystemExit(f"❌ pitcher file missing '{col}'")

    _ensure_num(bat, ["over_probability", "line", "value"])
    _ensure_num(pit, ["over_probability", "line", "value"])

    # Mark pitchers
    if "player_pos" in pit.columns:
        pit["is_pitcher"] = pit["player_pos"].astype(str).str.lower().eq("pitcher")
    else:
        pit["is_pitcher"] = True
    bat["is_pitcher"] = False

    # Canonicalize props’ team to team_name
    bat["team"] = normalize_series(bat["team"])
    pit["team"] = normalize_series(pit["team"])

    # Combine props
    both = pd.concat([bat, pit], ignore_index=True, sort=False)

    # CRITICAL: drop pre-existing game_id/date from props to avoid suffixing on merge
    for c in ("game_id", "date"):
        if c in both.columns:
            both = both.drop(columns=[c])

    # Merge schedule (brings in schedule's game_id/date)
    both = both.merge(team_map_sched, on="team", how="left")

    # Guard & diagnostics
    if "game_id" not in both.columns:
        raise SystemExit("❌ Merge failed: schedule did not contribute 'game_id' column.")

    off_mask = both["game_id"].isna()
    if int(off_mask.sum()):
        sample = (both.loc[off_mask, "team"].value_counts().head(12).to_dict()
                  if "team" in both.columns else {})
        raise SystemExit(f"❌ No schedule match for {int(off_mask.sum())} props. Example teams: {sample}")

    # Clean & sort
    both = both.dropna(subset=["over_probability"])
    both = both.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # Per-game selection
    chunks = []
    for gid, df_game in both.groupby("game_id", dropna=False):
        sel = _select_for_game(df_game)
        if not sel.empty:
            chunks.append(sel)

    selected = pd.concat(chunks, ignore_index=True) if chunks else both.head(0).copy()

    # Global top-3 → "Best Prop"
    if not selected.empty:
        selected["prop_sort"] = "game"
        top3_idx = selected["over_probability"].nlargest(3).index
        selected.loc[top3_idx, "prop_sort"] = "Best Prop"

    # Output schema
    selected["prop_correct"] = ""
    selected["date"] = pd.to_datetime(selected["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    out = selected.copy()
    for c in PLAYER_COLUMNS:
        if c not in out.columns:
            out[c] = ""
    out = out[PLAYER_COLUMNS].copy()

    PLAYER_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(PLAYER_OUT, index=False)
    print(f"✅ Wrote {len(out)} rows → {PLAYER_OUT}")

if __name__ == "__main__":
    main()
