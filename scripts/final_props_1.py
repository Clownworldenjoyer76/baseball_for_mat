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
    """
    Map any alias to canonical team_name (per team_name_master.csv).
    STRICT: unknown aliases become NaN (no fallback/guessing).
    """
    req = {"team_code", "team_name", "abbreviation", "clean_team_name"}
    miss = [c for c in req if c not in team_map_df.columns]
    if miss:
        raise SystemExit(f"❌ team_name_master.csv missing columns: {miss}")

    alias_to_team = {}
    def _add(key_val, team_name):
        if pd.isna(key_val): return
        k = str(key_val).strip().lower()
        if k: alias_to_team[k] = team_name

    for _, r in team_map_df.iterrows():
        canon = str(r["team_name"]).strip()
        _add(r["team_code"], canon)
        _add(r["abbreviation"], canon)
        _add(r["clean_team_name"], canon)
        _add(r["team_name"], canon)
        _add(str(r["team_name"]).lower(), canon)

    def normalize_series_strict(s: pd.Series) -> pd.Series:
        return s.astype(str).map(lambda x: alias_to_team.get(str(x).strip().lower(), pd.NA))

    return normalize_series_strict

def _first_best(df: pd.DataFrame, mask: pd.Series) -> list[int]:
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
      5) best remaining (highest over_probability not yet picked)
    """
    df = df_game.copy()
    df["_prop_lc"] = df["prop"].astype(str).str.strip().str.lower()

    picks: list[int] = []
    picks += _first_best(df, df["_prop_lc"].isin(HR_ALIASES))
    picks += _first_best(df, df["_prop_lc"].isin(H_ALIASES) & (~df.index.isin(picks)))
    picks += _first_best(df, df["_prop_lc"].isin(TB_ALIASES) & (~df.index.isin(picks)))
    if "is_pitcher" in df.columns:
        picks += _first_best(df, (df["is_pitcher"] == True) & (~df.index.isin(picks)))

    remain = df[~df.index.isin(picks)].dropna(subset=["over_probability"])
    if not remain.empty:
        picks.append(remain["over_probability"].idxmax())

    if not picks:
        return df.head(0).copy()

    sel = df.loc[picks].copy().sort_values("over_probability", ascending=False)
    sel["prop_sort"] = "game"
    return sel

# ---------- Main ----------
def main():
    # Team map & normalizer (STRICT)
    teammap = _std(pd.read_csv(TEAMMAP_FILE))
    normalize_series = _build_team_normalizer(teammap)

    # Schedule
    sched = _std(pd.read_csv(SCHED_FILE))
    need_sched = [c for c in ("home_team", "away_team", "date", "game_id") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    # Normalize schedule teams (fail hard on unknown aliases)
    for col in ["home_team", "away_team"]:
        orig = sched[col].copy()
        sched[col] = normalize_series(sched[col])
        unknown = orig[pd.isna(sched[col])].dropna().unique().tolist()
        if unknown:
            raise SystemExit(f"❌ Unknown team alias(es) in schedule '{col}': {unknown}")

    sched["date"] = pd.to_datetime(sched["date"], errors="coerce")
    # drop timezone if present for stable date-only comparisons
    try:
        sched["date"] = sched["date"].dt.tz_localize(None)
    except Exception:
        pass
    if sched["date"].isna().all():
        raise SystemExit("❌ schedule 'date' column is not parseable")

    # Select today's slate (fallback to latest in schedule) using date-only
    today_date = pd.to_datetime(_today_str()).date()
    sched_today = sched[sched["date"].dt.date == today_date].copy()
    if sched_today.empty:
        latest = sched["date"].max()
        sched_today = sched[sched["date"] == latest].copy()
        print(f"⚠️ No schedule for today ({today_date}); using latest {latest.date()} instead.")
    else:
        print(f"✅ Using schedule for {today_date}")

    # Long map: team -> (date, game_id) for the selected date only
    team_map = pd.concat([
        sched_today[["home_team", "date", "game_id"]].rename(columns={"home_team": "team"}),
        sched_today[["away_team", "date", "game_id"]].rename(columns={"away_team": "team"}),
    ], ignore_index=True).drop_duplicates()

    # Props
    bat = _std(pd.read_csv(BATTER_FILE))
    pit = _std(pd.read_csv(PITCHER_FILE))

    for col in ["prop", "team", "over_probability"]:
        if col not in bat.columns: raise SystemExit(f"❌ batter file missing '{col}'")
        if col not in pit.columns: raise SystemExit(f"❌ pitcher file missing '{col}'")

    _ensure_num(bat, ["over_probability", "line", "value"])
    _ensure_num(pit, ["over_probability", "line", "value"])

    # Mark pitchers
    pit["is_pitcher"] = pit["player_pos"].astype(str).str.lower().eq("pitcher") if "player_pos" in pit.columns else True
    bat["is_pitcher"] = False

    # Normalize teams in props (fail hard on unknowns)
    for df, name in [(bat, "batter"), (pit, "pitcher")]:
        for col in ["team", "opp_team"]:
            if col in df.columns:
                orig = df[col].copy()
                df[col] = normalize_series(df[col])
                unknown = orig[pd.isna(df[col])].dropna().unique().tolist()
                if unknown:
                    raise SystemExit(f"❌ Unknown team alias(es) in {name} file '{col}': {unknown}")

    # Preserve original ids from pitcher file (some rows already have correct ids)
    pit["_game_id_orig"] = pit["game_id"] if "game_id" in pit.columns else np.nan
    pit["_date_orig"]    = pit["date"]    if "date"    in pit.columns else np.nan

    both = pd.concat([bat, pit], ignore_index=True, sort=False)

    # Remove any preexisting date/game_id to avoid merge suffixing
    both = both.drop(columns=[c for c in ("game_id", "date") if c in both.columns], errors="ignore")

    # Primary join: props.team → schedule team_map (brings schedule date/game_id)
    both = both.merge(team_map, on="team", how="left")

    # Fallback A: use opp_team for batters where primary join missed
    if "opp_team" in both.columns:
        opp_map = team_map.rename(columns={"team": "opp_team"})
        both = both.merge(opp_map, on="opp_team", how="left", suffixes=("", "_opp"))
        miss = both["game_id"].isna() & both["game_id_opp"].notna()
        both.loc[miss, "game_id"] = both.loc[miss, "game_id_opp"]
        both.loc[miss, "date"]    = both.loc[miss, "date_opp"]
        both = both.drop(columns=[c for c in ["game_id_opp", "date_opp"] if c in both.columns])

    # Fallback B: reuse original pitcher ids where still missing
    if "_game_id_orig" in both.columns:
        miss = both["game_id"].isna() & both["_game_id_orig"].notna()
        both.loc[miss, "game_id"] = both.loc[miss, "_game_id_orig"]
    if "_date_orig" in both.columns:
        miss = both["date"].isna() & both["_date_orig"].notna()
        both.loc[miss, "date"] = both.loc[miss, "_date_orig"]

    both = both.drop(columns=[c for c in ["_game_id_orig","_date_orig"] if c in both.columns], errors="ignore")

    # Log & DROP any remaining unmatched (do NOT exit)
    if "game_id" not in both.columns:
        print("⚠️ Schedule merge produced no 'game_id' column; continuing with empty selection.")
        both = both.head(0)
    else:
        miss_ct = int(both["game_id"].isna().sum())
        if miss_ct:
            sample = both.loc[both["game_id"].isna(), "team"].value_counts().head(10).to_dict()
            print(f"⚠️ No schedule match for {miss_ct} props. Example teams: {sample}")
            both = both[both["game_id"].notna()].copy()

    # Clean & sort
    if "over_probability" not in both.columns or both.empty:
        # nothing to select; write empty with correct columns
        out = pd.DataFrame(columns=PLAYER_COLUMNS)
        PLAYER_OUT.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(PLAYER_OUT, index=False)
        print(f"✅ Wrote 0 rows → {PLAYER_OUT}")
        return

    both = both.dropna(subset=["over_probability"])
    both = both.sort_values(["game_id", "over_probability"], ascending=[True, False])

    # Per-game selection
    chunks = []
    for gid, df_game in both.groupby("game_id", dropna=False):
        sel = _select_for_game(df_game)
        if not sel.empty:
            chunks.append(sel)

    selected = pd.concat(chunks, ignore_index=True) if chunks else both.head(0).copy()

    # Global top-3 -> "Best Prop"
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
