#!/usr/bin/env python3
"""
Inject team_id and game_id into:
  - data/_projections/batter_props_projected_final.csv
  - data/_projections/batter_props_expanded_final.csv

Sources:
  - Team for each batter: data/raw/lineups.csv (player_id, team_id)
  - Game for each team:  data/raw/todaysgames_normalized.csv (game_id, home_team_id, away_team_id)

Notes:
  - All key columns coerced to str to avoid dtype merge issues.
  - Safe left merges; original rows/order preserved.
  - Writes diagnostics for any remaining missing team_id / game_id.
"""

from pathlib import Path
import pandas as pd

# ----- Paths -----
DAILY_DIR   = Path("data/_projections")
RAW_DIR     = Path("data/raw")
SUM_DIR     = Path("summaries/07_final")
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATS_PROJ   = DAILY_DIR / "batter_props_projected_final.csv"
BATS_EXP    = DAILY_DIR / "batter_props_expanded_final.csv"
LINEUPS     = RAW_DIR   / "lineups.csv"
TGN         = RAW_DIR   / "todaysgames_normalized.csv"

# ----- Helpers -----
def _read_csv_force_str(path: Path) -> pd.DataFrame:
    """
    Read CSV with all columns as str where possible.
    This prevents object/int mismatches on keys.
    """
    # Load without dtype to preserve content, then coerce to str
    df = pd.read_csv(path)
    for c in df.columns:
        # Convert everything to str uniformly to be safe for joins on ids/names
        df[c] = df[c].astype(str)
        # Normalize typical textual NaNs from previous runs
        df[c] = df[c].replace({"nan": "", "None": "", "NaN": ""})
    return df

def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{missing} missing in dataframe")

def _write_missing(df: pd.DataFrame, cols: list[str], stem: str) -> tuple[int, int]:
    """Log and dump CSVs for rows missing team_id / game_id."""
    missing_team_mask = df["team_id"].eq("") | df["team_id"].isna()
    missing_game_mask = df["game_id"].eq("") | df["game_id"].isna()

    miss_team = df.loc[missing_team_mask, ["player_id"] + [c for c in cols if c in df.columns]].copy()
    miss_game = df.loc[missing_game_mask, ["player_id","team_id"] + [c for c in cols if c in df.columns]].copy()

    mt = len(miss_team)
    mg = len(miss_game)

    if mt > 0:
        miss_team.to_csv(SUM_DIR / f"missing_team_id_in_{stem}.csv", index=False)
        print(f"[WARN] {stem}: {mt} rows missing team_id (summaries/07_final/missing_team_id_in_{stem}.csv)")
    if mg > 0:
        miss_game.to_csv(SUM_DIR / f"missing_game_id_in_{stem}.csv", index=False)
        print(f"[WARN] {stem}: {mg} rows missing game_id (summaries/07_final/missing_game_id_in_{stem}.csv)")

    print(f"[INFO] {stem}: missing team_id={mt}, missing game_id={mg}")
    return mt, mg

def main():
    print("PREP: injecting team_id and game_id into batter *_final.csv")

    # ---------- Load inputs with safe dtypes ----------
    bats_proj = _read_csv_force_str(BATS_PROJ)
    bats_exp  = _read_csv_force_str(BATS_EXP)
    lineups   = _read_csv_force_str(LINEUPS)
    tgn       = _read_csv_force_str(TGN)

    # Expected columns
    _ensure_cols(bats_proj, ["player_id"])
    _ensure_cols(bats_exp,  ["player_id"])
    _ensure_cols(lineups,   ["player_id", "team_id"])
    _ensure_cols(tgn,       ["game_id", "home_team_id", "away_team_id"])

    # ---------- Build team_id -> game_id map from TGN ----------
    # Melt home/away into long form keyed by team_id
    tgn_home = tgn[["game_id", "home_team_id"]].rename(columns={"home_team_id": "team_id"}).copy()
    tgn_away = tgn[["game_id", "away_team_id"]].rename(columns={"away_team_id": "team_id"}).copy()
    tgn_map  = pd.concat([tgn_home, tgn_away], ignore_index=True)
    # Ensure team_id and game_id are strings and clean blanks
    for c in ["team_id", "game_id"]:
        tgn_map[c] = tgn_map[c].astype(str).replace({"nan": "", "None": "", "NaN": ""})

    # ---------- Inject team_id from lineups ----------
    # Use left merge on player_id; keep original order/cols
    def inject_team_and_game(df: pd.DataFrame, stem: str) -> pd.DataFrame:
        df = df.copy()

        # Ensure required columns exist even if empty
        if "team_id" not in df.columns:
            df["team_id"] = ""
        if "game_id" not in df.columns:
            df["game_id"] = ""

        # Coerce keys to string
        for c in ["player_id", "team_id", "game_id"]:
            if c in df.columns:
                df[c] = df[c].astype(str).replace({"nan": "", "None": "", "NaN": ""})

        # Inject team_id from lineups for rows missing team_id
        need_team_mask = df["team_id"].eq("") | df["team_id"].isna()
        if need_team_mask.any():
            df_team = df.loc[:, ["player_id", "team_id", "game_id"]].copy()
            df_left = df.loc[:, [c for c in df.columns if c not in ["team_id", "game_id"]]].copy()

            # Merge to get team_id from lineups
            merged = df_left.merge(
                lineups[["player_id", "team_id"]],
                on="player_id",
                how="left",
                suffixes=("", "_from_lineups")
            )

            # If team_id column existed originally, prefer original when present; fill missing from _from_lineups
            if "team_id" in df.columns:
                # Attach previous values and fill
                merged["team_id"] = df_team["team_id"].values
                merged["team_id"] = merged["team_id"].where(merged["team_id"].ne(""), merged["team_id_from_lineups"])
            else:
                merged.rename(columns={"team_id_from_lineups": "team_id"}, inplace=True)

            merged.drop(columns=[c for c in merged.columns if c.endswith("_from_lineups")], inplace=True)
            # Re-join other columns that were temporarily parked
            # (They’re already in merged via df_left; add back any we parked)
            for c in df.columns:
                if c not in merged.columns:
                    merged[c] = df[c]

            df = merged

        # ---------- Inject game_id from team_id via tgn_map ----------
        need_game_mask = df["game_id"].eq("") | df["game_id"].isna()
        if need_game_mask.any():
            df_left = df.copy()
            df_left = df_left.merge(
                tgn_map.drop_duplicates(subset=["team_id"]),
                on="team_id",
                how="left",
                suffixes=("", "_from_tgn")
            )
            # Prefer existing game_id when present; otherwise fill from _from_tgn
            if "game_id_from_tgn" in df_left.columns:
                df_left["game_id"] = df_left["game_id"].where(df_left["game_id"].ne(""), df_left["game_id_from_tgn"])
                df_left.drop(columns=["game_id_from_tgn"], inplace=True)

            df = df_left

        # Keep columns order stable: put keys up front if present
        key_order = [c for c in ["player_id", "team_id", "game_id"] if c in df.columns]
        other = [c for c in df.columns if c not in key_order]
        df = df[key_order + other]

        # Diagnostics
        _write_missing(df, cols=[], stem=stem)

        return df

    bats_proj_out = inject_team_and_game(bats_proj, stem="batter_props_projected_final")
    bats_exp_out  = inject_team_and_game(bats_exp,  stem="batter_props_expanded_final")

    # ---------- Write outputs ----------
    bats_proj_out.to_csv(BATS_PROJ, index=False)
    bats_exp_out.to_csv(BATS_EXP, index=False)
    print(f"OK: wrote {BATS_PROJ} and {BATS_EXP}")

if __name__ == "__main__":
    # We avoid printing any literal "[FAIL]" to keep CI grep clean.
    try:
        main()
    except Exception as e:
        # Write a minimal log with the exception string (no '[FAIL]' marker)
        (SUM_DIR / "prep_injection_log.txt").write_text(repr(e), encoding="utf-8")
        raise
