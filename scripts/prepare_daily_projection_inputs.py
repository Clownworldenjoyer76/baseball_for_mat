#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Inject team_id and game_id into batter *_final.csv using:
#   - data/raw/lineups.csv         (player_id -> team_id)
#   - data/raw/todaysgames_normalized.csv (team_id -> game_id via home/away)
#
# Design:
#   * Treat keys as strings throughout (player_id, team_id, game_id)
#   * Never cast Series to int (avoids TypeError)
#   * Write warnings + diagnostic CSVs; do not fail the run

import pandas as pd
from pathlib import Path

DAILY_DIR   = Path("data/_projections")
RAW_DIR     = Path("data/raw")
SUM_DIR     = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

# Inputs
BATTERS_PROJ = DAILY_DIR / "batter_props_projected_final.csv"
BATTERS_EXP  = DAILY_DIR / "batter_props_expanded_final.csv"
LINEUPS      = RAW_DIR / "lineups.csv"
TGN          = RAW_DIR / "todaysgames_normalized.csv"

def read_csv_str(path: Path) -> pd.DataFrame:
    """Read CSV with all object (string-like) dtypes to avoid numeric coercion."""
    df = pd.read_csv(path, dtype=str)
    # Normalize typical key columns to string explicitly (strip spaces)
    for c in ("player_id","team_id","game_id","home_team_id","away_team_id"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def write_warn_list(rows_df: pd.DataFrame, out_path: Path, label: str):
    if rows_df.empty:
        return 0
    rows_df.to_csv(out_path, index=False)
    print(f"[WARN] {label} ({out_path})")
    return len(rows_df)

def inject_team_and_game(bat_df: pd.DataFrame,
                         lineups_df: pd.DataFrame,
                         tgn_df: pd.DataFrame,
                         label: str) -> pd.DataFrame:
    """
    1) Ensure player_id is present; merge to inject team_id from lineups.
    2) Build (team_id -> game_id) long map from todaysgames_normalized (home/away).
    3) Merge to inject game_id by (team_id).
    All keys are strings; no int casts.
    """
    df = bat_df.copy()

    # --- 1) Inject team_id from lineups by player_id ---
    # Keep only necessary columns from lineups
    need_cols = [c for c in ("player_id","team_id") if c in lineups_df.columns]
    lx = lineups_df[need_cols].dropna().copy()

    # If duplicates of player_id exist with conflicting team_id, prefer the last row
    # (lineups often append over the day). This avoids multi-row merges.
    if "player_id" in lx.columns:
        lx = lx.sort_index().drop_duplicates(subset=["player_id"], keep="last")

    # Merge
    if "player_id" in df.columns:
        df = df.merge(lx, on="player_id", how="left", suffixes=("", "_from_lineups"))
        # If original df unexpectedly had a team_id, prefer the non-null one
        if "team_id_from_lineups" in df.columns:
            df["team_id"] = df.get("team_id", pd.Series([None]*len(df), dtype=object))
            df["team_id"] = df["team_id"].where(df["team_id"].notna(), df["team_id_from_lineups"])
            df.drop(columns=[c for c in df.columns if c.endswith("_from_lineups")], inplace=True)

    # --- 2) Build (team_id -> game_id) map from TGN (home/away) ---
    # Expect columns: game_id, home_team_id, away_team_id
    tgn_cols = [c for c in ("game_id","home_team_id","away_team_id") if c in tgn_df.columns]
    tg = tgn_df[tgn_cols].copy()

    # Melt to long: one row per (game_id, team_id)
    long_map = []
    if {"game_id", "home_team_id"}.issubset(tg.columns):
        tmp = tg[["game_id","home_team_id"]].rename(columns={"home_team_id":"team_id"})
        long_map.append(tmp)
    if {"game_id", "away_team_id"}.issubset(tg.columns):
        tmp = tg[["game_id","away_team_id"]].rename(columns={"away_team_id":"team_id"})
        long_map.append(tmp)

    if long_map:
        team_game_map = pd.concat(long_map, ignore_index=True)
        # Clean
        team_game_map = team_game_map.dropna(subset=["team_id", "game_id"]).copy()
        team_game_map["team_id"] = team_game_map["team_id"].astype(str).str.strip()
        team_game_map["game_id"] = team_game_map["game_id"].astype(str).str.strip()
        # If a team appears twice (rare doubleheader), prefer the last occurrence
        team_game_map = team_game_map.drop_duplicates(subset=["team_id"], keep="last")
    else:
        # No mapping possible; keep empty map to avoid KeyError
        team_game_map = pd.DataFrame(columns=["team_id","game_id"])

    # --- 3) Inject game_id by team_id ---
    if "team_id" in df.columns and not team_game_map.empty:
        df = df.merge(team_game_map, on="team_id", how="left", suffixes=("", "_mapped"))
        if "game_id_mapped" in df.columns:
            # Respect any existing non-null game_id; else take mapped
            df["game_id"] = df.get("game_id", pd.Series([None]*len(df), dtype=object))
            df["game_id"] = df["game_id"].where(df["game_id"].notna(), df["game_id_mapped"])
            df.drop(columns=[c for c in df.columns if c.endswith("_mapped")], inplace=True)

    # --- Diagnostics (no exceptions) ---
    miss_team = df.loc[df.get("team_id").isna(), ["player_id"]].drop_duplicates() if "team_id" in df.columns else df[["player_id"]]
    miss_game = df.loc[df.get("game_id").isna(), ["player_id","team_id"]].drop_duplicates() if "game_id" in df.columns else df[["player_id","team_id"]]

    n_miss_team = write_warn_list(
        miss_team,
        SUM_DIR / f"missing_team_id_in_{label}.csv",
        f"{label}: {len(miss_team)} rows missing team_id"
    )
    n_miss_game = write_warn_list(
        miss_game,
        SUM_DIR / f"missing_game_id_in_{label}.csv",
        f"{label}: {len(miss_game)} rows missing game_id"
    )

    print(f"[INFO] {label}: missing team_id={n_miss_team}, missing game_id={n_miss_game}")
    return df

def main():
    print("PREP: injecting team_id and game_id into batter *_final.csv")

    # Load inputs as strings
    bat_proj = read_csv_str(BATTERS_PROJ)
    bat_exp  = read_csv_str(BATTERS_EXP)
    lineups  = read_csv_str(LINEUPS)
    tgn      = read_csv_str(TGN)

    # Inject (team_id, game_id)
    bat_proj_out = inject_team_and_game(bat_proj, lineups, tgn, "batter_props_projected_final")
    bat_exp_out  = inject_team_and_game(bat_exp,  lineups, tgn, "batter_props_expanded_final")

    # Write back (preserve CSVs even if there are unresolved rows)
    bat_proj_out.to_csv(BATTERS_PROJ, index=False)
    bat_exp_out.to_csv(BATTERS_EXP,  index=False)

    print(f"OK: wrote {BATTERS_PROJ} and {BATTERS_EXP}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Fail-safe: never emit noisy traceback lines that get grepped across runs.
        # We print a concise FAIL marker here so the summary can capture *only this run's* error.
        (SUM_DIR / "prep_injection_log.txt").write_text(f"[FAIL] {repr(e)}\n", encoding="utf-8")
        raise
