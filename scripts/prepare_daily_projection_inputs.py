#!/usr/bin/env python3
# scripts/prepare_daily_projection_inputs.py
#
# Purpose:
#   - Ensure batter *_final daily CSVs have consistent string IDs
#   - Inject team_id from data/raw/lineups.csv (by player_id)
#   - Inject game_id by mapping team_id to game_id using data/raw/todaysgames_normalized.csv
#   - Write back to the same *_final files (projected & expanded)
#   - Emit concise diagnostics in summaries/07_final (no benign "error" words)

from pathlib import Path
import pandas as pd

# ---- Paths ----
PROJ_DIR = Path("data/_projections")
RAW_DIR  = Path("data/raw")
SUM_DIR  = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATS_PROJECTED = PROJ_DIR / "batter_props_projected_final.csv"
BATS_EXPANDED  = PROJ_DIR / "batter_props_expanded_final.csv"
LINEUPS        = RAW_DIR   / "lineups.csv"
TGN            = RAW_DIR   / "todaysgames_normalized.csv"

# ---- Helpers ----
def norm_id_series(s: pd.Series) -> pd.Series:
    """
    Normalize an ID-like series to strings, removing trailing '.0',
    trimming spaces, and preserving non-numeric tokens like 'UNKNOWN'.
    Empty/NaN -> pd.NA
    """
    if s is None:
        return pd.Series(dtype="string")

    s = s.astype("string")
    s = s.str.strip()

    # Treat common empties as NA
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NaN": pd.NA})

    # If numeric-looking, strip trailing .0 (e.g., "700249.0" -> "700249")
    s = s.str.replace(r"\.0$", "", regex=True)

    return s

def load_csv(path: Path, cols_as_string: list[str] | None = None) -> pd.DataFrame:
    """
    Read CSV with all columns as string by default, then coerce specific ones as string cleanly.
    (We keep everything as string to avoid Series->int issues.)
    """
    df = pd.read_csv(path, dtype="string")
    if cols_as_string:
        for c in cols_as_string:
            if c in df.columns:
                df[c] = norm_id_series(df[c])
    else:
        # Normalize common ID columns if present
        for c in ("player_id", "team_id", "game_id", "home_team_id", "away_team_id"):
            if c in df.columns:
                df[c] = norm_id_series(df[c])
    return df

def write_diag(path: Path, df: pd.DataFrame):
    """Write a small diagnostic CSV (overwrites)."""
    df.to_csv(path, index=False)

def inject_team_and_game_ids(bats_path: Path, team_map: pd.DataFrame, gid_map: pd.DataFrame, label: str):
    """
    - Load batter file
    - Inject/repair team_id via player_id -> team_id (team_map)
    - Inject/repair game_id via team_id -> game_id (gid_map)
    - Write warnings & diagnostics
    - Save back in place
    """
    df = load_csv(bats_path)

    # Ensure key columns exist even if missing
    if "player_id" not in df.columns:
        raise RuntimeError(f"{bats_path} is missing required column 'player_id'")

    # Normalize any existing IDs
    for c in ("player_id", "team_id", "game_id"):
        if c in df.columns:
            df[c] = norm_id_series(df[c])

    # --- Inject team_id from lineups if missing/NA ---
    if "team_id" not in df.columns:
        df["team_id"] = pd.NA
    df_before_team = df["team_id"].copy()

    df = df.merge(
        team_map[["player_id", "team_id"]].drop_duplicates(),
        on="player_id",
        how="left",
        suffixes=("", "_from_lineups"),
    )

    # Prefer existing team_id if present; otherwise use from lineups
    df["team_id"] = df["team_id"].fillna(df["team_id_from_lineups"])
    df.drop(columns=[c for c in df.columns if c.endswith("_from_lineups")], inplace=True)

    missing_team = df["team_id"].isna()
    if missing_team.any():
        out = df.loc[missing_team, ["player_id"]].drop_duplicates().reset_index(drop=True)
        write_diag(SUM_DIR / f"missing_team_id_in_{label}.csv", out)
        print(f"[WARN] {label}: {len(out)} rows missing team_id (summaries/07_final/missing_team_id_in_{label}.csv)")
    else:
        # Remove old diagnostic if it exists to reduce stale noise
        (SUM_DIR / f"missing_team_id_in_{label}.csv").unlink(missing_ok=True)

    # --- Inject game_id from todaysgames_normalized based on team_id ---
    if "game_id" not in df.columns:
        df["game_id"] = pd.NA
    df["team_id"] = norm_id_series(df["team_id"])  # re-normalize after fill

    df = df.merge(gid_map, on="team_id", how="left", suffixes=("", "_from_map"))
    # Prefer existing game_id; otherwise use mapped one
    df["game_id"] = df["game_id"].fillna(df["game_id_from_map"])
    df.drop(columns=[c for c in df.columns if c.endswith("_from_map")], inplace=True)

    missing_gid = df["game_id"].isna()
    if missing_gid.any():
        out = df.loc[missing_gid, ["player_id", "team_id"]].drop_duplicates().reset_index(drop=True)
        write_diag(SUM_DIR / f"missing_game_id_in_{label}.csv", out)
        print(f"[WARN] {label}: {len(out)} rows missing game_id (summaries/07_final/missing_game_id_in_{label}.csv)")
    else:
        (SUM_DIR / f"missing_game_id_in_{label}.csv").unlink(missing_ok=True)

    # Final tidy: ensure IDs are strings & de-dupe
    for c in ("player_id", "team_id", "game_id"):
        if c in df.columns:
            df[c] = norm_id_series(df[c])

    df = df.drop_duplicates()
    df.to_csv(bats_path, index=False)

def main():
    print("PREP: injecting team_id and game_id into batter *_final.csv")

    # Load auxiliary inputs
    # lineups: needs (player_id, team_id)
    lineups = load_csv(LINEUPS)
    need_lineups = {"player_id", "team_id"}
    if not need_lineups.issubset(lineups.columns):
        raise RuntimeError(f"{LINEUPS} missing columns: {sorted(need_lineups - set(lineups.columns))}")
    lineups["player_id"] = norm_id_series(lineups["player_id"])
    lineups["team_id"]   = norm_id_series(lineups["team_id"])

    # todaysgames_normalized: needs (game_id, home_team_id, away_team_id)
    tgn = load_csv(TGN)
    need_tgn = {"game_id", "home_team_id", "away_team_id"}
    if not need_tgn.issubset(tgn.columns):
        raise RuntimeError(f"{TGN} missing columns: {sorted(need_tgn - set(tgn.columns))}")
    tgn["game_id"]       = norm_id_series(tgn["game_id"])
    tgn["home_team_id"]  = norm_id_series(tgn["home_team_id"])
    tgn["away_team_id"]  = norm_id_series(tgn["away_team_id"])

    # Build (team_id -> game_id) map by melting home/away into rows
    gid_map = pd.melt(
        tgn[["game_id", "home_team_id", "away_team_id"]].rename(
            columns={"home_team_id": "HOME", "away_team_id": "AWAY"}
        ),
        id_vars=["game_id"],
        value_vars=["HOME", "AWAY"],
        var_name="side",
        value_name="team_id",
    )[["team_id", "game_id"]].dropna().drop_duplicates()
    gid_map["team_id"] = norm_id_series(gid_map["team_id"])
    gid_map["game_id"] = norm_id_series(gid_map["game_id"])

    # Process both batter files
    inject_team_and_game_ids(BATS_PROJECTED, lineups, gid_map, "batter_props_projected_final")
    inject_team_and_game_ids(BATS_EXPANDED,  lineups, gid_map, "batter_props_expanded_final")

    print("OK: wrote data/_projections/batter_props_projected_final.csv and data/_projections/batter_props_expanded_final.csv")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Only write a single concise log line; do not spam "error" unless truly failing.
        (SUM_DIR / "prep_injection_log.txt").write_text(repr(e), encoding="utf-8")
        raise
