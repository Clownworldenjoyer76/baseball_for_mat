# scripts/final_props_1.py

from __future__ import annotations
import math
from pathlib import Path
import pandas as pd

# ---------------- normalization & schedule constants ----------------
SCHED_FILE   = Path("data/bets/mlb_sched.csv")
TEAMMAP_FILE = Path("data/Data/team_name_master.csv")
OUTPUT_FILE  = Path("data/_projections/final_props_1.csv")
# -------------------------------------------------------------------

def _std(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df

def _build_team_normalizer(team_map_df: pd.DataFrame):
    """
    Map any alias to canonical team_name (per team_name_master.csv).
    Exception: handles 'St. Louis Cardinals' → 'Cardinals'.
    """
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

    # --- Explicit exception for Cardinals ---
    alias_to_team["st. louis cardinals"] = "Cardinals"
    alias_to_team["st louis cardinals"] = "Cardinals"

    def normalize_series_strict(s: pd.Series) -> pd.Series:
        return s.astype(str).map(lambda x: alias_to_team.get(str(x).strip().lower(), pd.NA))

    return normalize_series_strict

def main():
    teammap = _std(pd.read_csv(TEAMMAP_FILE))
    normalize_series = _build_team_normalizer(teammap)

    sched = _std(pd.read_csv(SCHED_FILE))
    need_sched = [c for c in ("home_team", "away_team", "date") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    for col in ["home_team", "away_team"]:
        orig = sched[col].copy()
        sched[col] = normalize_series(sched[col])
        unknown = orig[pd.isna(sched[col])].dropna().unique().tolist()
        if unknown:
            raise SystemExit(f"❌ Unknown team alias(es) in schedule '{col}': {unknown}")

    # ... rest of your script’s logic continues here ...
    # For now just save normalized schedule for demonstration
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    sched.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Normalized schedule written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
