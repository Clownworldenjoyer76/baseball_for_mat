# scripts/final_props_1.py

from __future__ import annotations
import math
from pathlib import Path
import pandas as pd

# ---------------- normalization & schedule constants ----------------
SCHED_FILE    = Path("data/bets/mlb_sched.csv")
TEAMMAP_FILE  = Path("data/Data/team_name_master.csv")
OUTPUT_FILE   = Path("data/_projections/final_props_1.csv")          # keeps your prior output
GAME_PROPS_IN = [
    Path("data/bets/game_props_today.csv"),
    Path("data/bets/prep/game_props_bets.csv"),
    Path("data/bets/game_props_bets.csv"),
]
GAME_PROPS_OUT = Path("data/bets/game_props_history.csv")
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

def _load_first_existing(paths: list[Path]) -> pd.DataFrame | None:
    for p in paths:
        if p.exists():
            df = pd.read_csv(p)
            return _std(df)
    return None

def main():
    # --- Build normalizer and normalize schedule ---
    teammap = _std(pd.read_csv(TEAMMAP_FILE))
    normalize_series = _build_team_normalizer(teammap)

    sched = _std(pd.read_csv(SCHED_FILE))
    need_sched = [c for c in ("game_id", "home_team", "away_team", "date") if c not in sched.columns]
    if need_sched:
        raise SystemExit(f"❌ schedule missing columns: {need_sched}")

    for col in ["home_team", "away_team"]:
        orig = sched[col].copy()
        sched[col] = normalize_series(sched[col])
        unknown = orig[pd.isna(sched[col])].dropna().unique().tolist()
        if unknown:
            raise SystemExit(f"❌ Unknown team alias(es) in schedule '{col}': {unknown}")

    # Keep a compact schedule view for joins
    sched_join = sched[["game_id", "date", "home_team", "away_team"]].drop_duplicates("game_id")

    # Persist normalized schedule snapshot (keeps your prior behavior)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    sched.to_csv(OUTPUT_FILE, index=False)

    # --- Build/append game props HISTORY by game_id (not team/side) ---
    props = _load_first_existing(GAME_PROPS_IN)
    if props is not None:
        # Normalize obvious game_id casing if needed
        if "gameId" in props.columns and "game_id" not in props.columns:
            props = props.rename(columns={"gameId": "game_id"})

        if "game_id" not in props.columns:
            raise SystemExit("❌ game props input is missing 'game_id' — cannot join by game_id.")

        # Strict one-to-one merge on game_id only
        merged = props.merge(sched_join, on="game_id", how="left", validate="m:1")

        # Append to history and de-dup by game_id (+ finer keys if present)
        if GAME_PROPS_OUT.exists():
            hist = pd.read_csv(GAME_PROPS_OUT)
            hist = _std(hist)
            combined = pd.concat([hist, merged], ignore_index=True)
        else:
            combined = merged

        # Preferred de-dup key hierarchy
        dedup_keys = [k for k in ["game_id", "market", "selection", "book", "asof"] if k in combined.columns]
        if not dedup_keys:
            dedup_keys = ["game_id"]

        combined = combined.sort_values(by=[c for c in combined.columns if c in ("asof", "date")]).drop_duplicates(
            subset=dedup_keys, keep="last"
        )

        GAME_PROPS_OUT.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(GAME_PROPS_OUT, index=False)

if __name__ == "__main__":
    main()
