#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Strengthened:
# - Fallback to map team ABBREV -> team_id using todaysgames_normalized.csv
#   when lineups.csv is missing a player's team_id.
# - Hard-fail if any team_id or game_id remain blank.
# - Explicit diagnostics under summaries/07_final/.
#
from __future__ import annotations

import pandas as pd
from pathlib import Path

PROJ_DIR = Path("data/_projections")
RAW_DIR  = Path("data/raw")
SUM_DIR  = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_PROJECTED = PROJ_DIR / "batter_props_projected_final.csv"
BATTERS_EXPANDED  = PROJ_DIR / "batter_props_expanded_final.csv"
LINEUPS_CSV       = RAW_DIR / "lineups.csv"
TGN_CSV           = RAW_DIR / "todaysgames_normalized.csv"

LOG_FILE = SUM_DIR / "prep_injection_log.txt"

def log(msg: str) -> None:
    print(msg, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")

def read_csv_force_str(path: Path) -> pd.DataFrame:
    """Read CSV with all columns coerced to string and whitespace trimmed."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
        df[c] = df[c].replace({"None": "", "nan": "", "NaN": ""})
    return df

def coalesce_series(a: pd.Series | None, b: pd.Series | None) -> pd.Series:
    """Return first non-empty (not NA/empty-string) between a and b (both may be None)."""
    if a is None and b is None:
        return pd.Series([], dtype="object")
    if a is None:
        a = pd.Series([""] * len(b), index=b.index, dtype="object")
    if b is None:
        b = pd.Series([""] * len(a), index=a.index, dtype="object")
    a = a.astype(str)
    b = b.astype(str)
    out = a.where(a.str.len() > 0, b)
    return out.fillna("").astype(str)

def build_team_maps_from_tgn(tgn: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build:
      - team_game: mapping (team_id -> game_id) exploded for home/away
      - abbrev_to_id: mapping (team abbrev -> team_id) from home/away columns
    Validates that each game_id has exactly two unique team_ids.
    """
    need = {"game_id", "home_team_id", "away_team_id", "home_team", "away_team"}
    miss = sorted(list(need - set(tgn.columns)))
    if miss:
        raise RuntimeError(f"{TGN_CSV} missing columns: {miss}")

    cols = ["game_id", "home_team_id", "away_team_id", "home_team", "away_team"]
    tgn = tgn[cols].copy()
    for c in tgn.columns:
        tgn[c] = tgn[c].astype(str).str.strip()

    # team_id -> game_id (exploded)
    home = tgn.rename(columns={"home_team_id": "team_id"})[["game_id", "team_id"]]
    away = tgn.rename(columns={"away_team_id": "team_id"})[["game_id", "team_id"]]
    team_game = pd.concat([home, away], ignore_index=True).drop_duplicates()
    team_game["team_id"] = team_game["team_id"].replace({"None": "", "nan": "", "NaN": ""})
    team_game = team_game[team_game["team_id"].str.len() > 0]

    # ABBREV -> team_id
    a_home = tgn.rename(columns={"home_team": "team", "home_team_id": "team_id"})[["team", "team_id"]]
    a_away = tgn.rename(columns={"away_team": "team", "away_team_id": "team_id"})[["team", "team_id"]]
    abbrev_to_id = pd.concat([a_home, a_away], ignore_index=True).dropna().drop_duplicates()

    # Validate: exactly two unique team_ids per game
    per_game = team_game.groupby("game_id")["team_id"].nunique()
    bad = per_game[per_game != 2]
    if not bad.empty:
        raise RuntimeError(f"{TGN_CSV} has games without exactly two teams: {bad.to_dict()}")

    return team_game, abbrev_to_id

def inject_team_and_game(df: pd.DataFrame, name_for_logs: str,
                         lineups: pd.DataFrame,
                         team_game_map: pd.DataFrame,
                         abbrev_to_id: pd.DataFrame) -> pd.DataFrame:
    """
    - Coalesce/attach 'team_id' using: existing -> lineups -> team abbrev map
    - Attach 'game_id' via team_id using the team_game_map (home/away exploded)
    - Emit diagnostics for any still-missing team_id/game_id
    """
    if "player_id" not in df.columns:
        raise RuntimeError(f"{name_for_logs} missing required column: player_id")

    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    # Bring team_id from lineups
    li = lineups.rename(columns={"team_id": "team_id_lineups"})[["player_id", "team_id_lineups"]].copy()
    merged = df.merge(li, on="player_id", how="left")

    # Fallback: map abbrev in df['team'] -> team_id when lineups is missing
    if "team" in merged.columns:
        abbrev = merged["team"].astype(str).str.strip()
        merged = merged.merge(abbrev_to_id, how="left", left_on="team", right_on="team")
        merged.rename(columns={"team_id": "team_id_from_abbrev"}, inplace=True)
    else:
        merged["team_id_from_abbrev"] = ""

    # Coalesce team_id: existing -> lineups -> abbrev map
    existing_team = merged["team_id"] if "team_id" in merged.columns else None
    from_lineups  = merged["team_id_lineups"] if "team_id_lineups" in merged.columns else None
    from_abbrev   = merged["team_id_from_abbrev"]
    merged["team_id"] = coalesce_series(coalesce_series(existing_team, from_lineups), from_abbrev)

    # Attach game_id via canonical mapping (coalesce with any pre-existing)
    merged = merged.merge(team_game_map, on="team_id", how="left", suffixes=("", "_from_map"))
    existing_gid = merged["game_id"] if "game_id" in merged.columns else None
    from_map     = merged["game_id_from_map"] if "game_id_from_map" in merged.columns else None
    merged["game_id"] = coalesce_series(existing_gid, from_map)

    # Drop helper columns if present
    drop_cols = ["team_id_lineups", "team_id_from_abbrev", "game_id_from_map", "team_y"]
    merged.drop(columns=[c for c in drop_cols if c in merged.columns], inplace=True)

    # Diagnostics
    miss_team = merged.loc[merged["team_id"].astype(str).str.len() == 0,
                           ["player_id"] + (["team"] if "team" in merged.columns else [])].drop_duplicates()
    miss_gid  = merged.loc[merged["game_id"].astype(str).str.len() == 0,
                           ["player_id", "team_id"]].drop_duplicates()

    if len(miss_team) > 0:
        out = SUM_DIR / f"missing_team_id_in_{name_for_logs}.csv"
        miss_team.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_team)} rows still missing team_id ({out})")

    if len(miss_gid) > 0:
        out = SUM_DIR / f"missing_game_id_in_{name_for_logs}.csv"
        miss_gid.to_csv(out, index=False)
        log(f"[WARN] {name_for_logs}: {len(miss_gid)} rows still missing game_id ({out})")

    log(f"[INFO] {name_for_logs}: missing team_id={len(miss_team)}, missing game_id={len(miss_gid)}")
    return merged

def write_back(df_before: pd.DataFrame, df_after: pd.DataFrame, path: Path) -> None:
    """Preserve original column order; append team_id/game_id at end if new."""
    cols = list(df_before.columns)
    for add_col in ["team_id", "game_id"]:
        if add_col not in cols:
            cols.append(add_col)
    cols_final = [c for c in cols if c in df_after.columns]
    df_after[cols_final].to_csv(path, index=False)

def main() -> None:
    LOG_FILE.write_text("", encoding="utf-8")
    log("PREP: injecting team_id and game_id into batter *_final.csv")

    bat_proj = read_csv_force_str(BATTERS_PROJECTED)
    bat_exp  = read_csv_force_str(BATTERS_EXPANDED)
    lineups  = read_csv_force_str(LINEUPS_CSV)
    tgn      = read_csv_force_str(TGN_CSV)

    team_game_map, abbrev_to_id = build_team_maps_from_tgn(tgn)

    bat_proj_out = inject_team_and_game(
        bat_proj, "batter_props_projected_final.csv", lineups, team_game_map, abbrev_to_id
    )
    bat_exp_out  = inject_team_and_game(
        bat_exp,  "batter_props_expanded_final.csv",  lineups, team_game_map, abbrev_to_id
    )

    # Hard stop if any keys remain blank
    if (bat_proj_out["team_id"].astype(str).str.len() == 0).any():
        raise RuntimeError("prepare_daily_projection_inputs: projected file has missing team_id after mapping.")
    if (bat_proj_out["game_id"].astype(str).str.len() == 0).any():
        raise RuntimeError("prepare_daily_projection_inputs: projected file has missing game_id after mapping.")
    if (bat_exp_out["team_id"].astype(str).str.len() == 0).any():
        raise RuntimeError("prepare_daily_projection_inputs: expanded file has missing team_id after mapping.")
    if (bat_exp_out["game_id"].astype(str).str.len() == 0).any():
        raise RuntimeError("prepare_daily_projection_inputs: expanded file has missing game_id after mapping.")

    write_back(bat_proj, bat_proj_out, BATTERS_PROJECTED)
    write_back(bat_exp,  bat_exp_out,  BATTERS_EXPANDED)

    log(f"OK: wrote {BATTERS_PROJECTED} and {BATTERS_EXPANDED}")

if __name__ == "__main__":
    main()
