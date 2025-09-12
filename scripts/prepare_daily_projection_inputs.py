#!/usr/bin/env python3
"""
Prepare daily inputs for event projection scripts.

- Ensures batter files have team_id and game_id.
- Uses today's normalized schedule (home/away names + ids + game_id).
- Writes the enriched CSVs in place.
- Emits diagnostics into summaries/07_final/.

Inputs it tries (first existing wins):
  schedule: data/raw/todaysgames_normalized.csv
            data/_projections/todaysgames_normalized_fixed.csv
  batters:  data/_projections/batter_props_projected_final.csv
            data/_projections/batter_props_expanded_final.csv
"""

from pathlib import Path
import pandas as pd

DAILY_DIR = Path("data/_projections")
RAW_DIR   = Path("data/raw")
SUM_DIR   = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_PROJ = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP  = DAILY_DIR / "batter_props_expanded_final.csv"

SCHED_CANDIDATES = [
    RAW_DIR / "todaysgames_normalized.csv",
    DAILY_DIR / "todaysgames_normalized_fixed.csv",
]

def _read_schedule() -> pd.DataFrame:
    for p in SCHED_CANDIDATES:
        if p.exists():
            df = pd.read_csv(p)
            df["__source"] = str(p)
            return df
    raise FileNotFoundError(
        "Could not find today's schedule CSV. Looked for: "
        + ", ".join(str(p) for p in SCHED_CANDIDATES)
    )

def _normalize_name(s: pd.Series) -> pd.Series:
    # Gentle normalizer to improve joins on team text
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
         .str.lower()
    )

def _schedule_long(df: pd.DataFrame) -> pd.DataFrame:
    # Accept a few common column spellings
    # Expect at least: game_id, home_team_name/home_team_id, away_team_name/away_team_id
    col = {c.lower(): c for c in df.columns}
    def pick(*opts):
        for o in opts:
            if o in col: return col[o]
        return None

    gcol = pick("game_id")
    h_name = pick("home_team_name","home_team","home")
    a_name = pick("away_team_name","away_team","away")
    h_id   = pick("home_team_id","home_id","home_teamid","home_team_id_x")
    a_id   = pick("away_team_id","away_id","away_teamid","away_team_id_x")

    need = [gcol, h_name, a_name, h_id, a_id]
    if any(x is None for x in need):
        raise RuntimeError(
            "Schedule file missing required columns. "
            f"Have: {list(df.columns)}"
        )

    skel = df[[gcol, h_name, a_name, h_id, a_id]].copy()
    skel.columns = ["game_id", "home_name", "away_name", "home_id", "away_id"]

    # two rows per game: home team + away team
    home = skel.assign(team_name=skel["home_name"], team_id=skel["home_id"])[["game_id","team_name","team_id"]]
    away = skel.assign(team_name=skel["away_name"], team_id=skel["away_id"])[["game_id","team_name","team_id"]]
    long = pd.concat([home, away], ignore_index=True)
    long["team_name_norm"] = _normalize_name(long["team_name"])
    long["team_id"] = pd.to_numeric(long["team_id"], errors="coerce")
    long["game_id"] = pd.to_numeric(long["game_id"], errors="coerce")
    return long

def _enrich_batters(bat_path: Path, sched_long: pd.DataFrame) -> pd.DataFrame:
    bat = pd.read_csv(bat_path)
    # If already present and non-null, keep; otherwise fill via join
    have_team_id = "team_id" in bat.columns and bat["team_id"].notna().any()
    have_game_id = "game_id" in bat.columns and bat["game_id"].notna().any()

    # Build join key from 'team' if present; otherwise try existing team_id->game_id map
    if "team" in bat.columns:
        bat["__team_name_norm"] = _normalize_name(bat["team"])
        joined = bat.merge(
            sched_long[["team_name_norm","team_id","game_id"]],
            left_on="__team_name_norm",
            right_on="team_name_norm",
            how="left",
        )
        # prefer existing ids if present, else use schedule
        if "team_id" in joined.columns:
            joined["team_id"] = pd.to_numeric(joined["team_id"], errors="coerce")
        joined["team_id"] = joined.get("team_id", pd.Series([pd.NA]*len(joined))).fillna(joined["team_id_y"] if "team_id_y" in joined else joined["team_id"])
        joined["game_id"] = pd.to_numeric(joined.get("game_id", pd.Series([pd.NA]*len(joined))), errors="coerce")
        joined["game_id"] = joined["game_id"].fillna(joined["game_id_y"] if "game_id_y" in joined else joined["game_id"])

        # Clean columns
        drop_cols = [c for c in joined.columns if c.endswith("_x") or c.endswith("_y") or c in ("team_name_norm","__team_name_norm")]
        joined = joined.drop(columns=drop_cols, errors="ignore")
        bat = joined
    else:
        # No team name — try to backfill game_id from a separate map if team_id exists
        if "team_id" in bat.columns:
            bat["team_id"] = pd.to_numeric(bat["team_id"], errors="coerce")
            m = sched_long.drop_duplicates(["game_id","team_id"])
            bat = bat.merge(m[["team_id","game_id"]], on="team_id", how="left")
        else:
            raise RuntimeError(f"{bat_path} has no 'team' or 'team_id' to resolve game_id from schedule.")

    # Final coercions
    bat["team_id"] = pd.to_numeric(bat["team_id"], errors="coerce")
    bat["game_id"] = pd.to_numeric(bat["game_id"], errors="coerce")

    # Diagnostics
    miss = bat[bat["game_id"].isna() | bat["team_id"].isna()].copy()
    if not miss.empty:
        out = SUM_DIR / f"prep_missing_team_or_game_id__{bat_path.name}.csv"
        miss.to_csv(out, index=False)

    bat.to_csv(bat_path, index=False)
    return bat

def main():
    sched = _read_schedule()
    sched_long = _schedule_long(sched)

    # Enrich batter projected (this is the one used by downstream scripts)
    if not BATTERS_PROJ.exists():
        raise FileNotFoundError(f"{BATTERS_PROJ} not found")
    bp = _enrich_batters(BATTERS_PROJ, sched_long)

    # Enrich batter expanded so it can inner-join on (player_id, game_id) later
    if BATTERS_EXP.exists():
        be = pd.read_csv(BATTERS_EXP)
        # If missing game_id, map from bp by player_id when same team/day
        if "game_id" not in be.columns or be["game_id"].isna().any():
            cols = ["player_id","game_id"]
            map_df = bp[cols].drop_duplicates()
            be = be.merge(map_df, on="player_id", how="left", suffixes=("","_from_proj"))
            # prefer existing if present
            if "game_id_from_proj" in be.columns:
                be["game_id"] = pd.to_numeric(be.get("game_id", pd.Series([pd.NA]*len(be))), errors="coerce")
                be["game_id"] = be["game_id"].fillna(pd.to_numeric(be["game_id_from_proj"], errors="coerce"))
                be = be.drop(columns=["game_id_from_proj"], errors="ignore")

            # diag
            miss = be[be["game_id"].isna()].copy()
            if not miss.empty:
                miss.to_csv(SUM_DIR / "prep_missing_game_id__batter_props_expanded_final.csv", index=False)

            be.to_csv(BATTERS_EXP, index=False)

    (SUM_DIR / "prep_status.txt").write_text("OK prepare_daily_projection_inputs.py\n", encoding="utf-8")

if __name__ == "__main__":
    main()
