#!/usr/bin/env python3
"""
Prepare daily inputs for 07_final_projections:
- Inject team_id into batter *_final.csv files using data/raw/lineups.csv
- Inject game_id by matching team_id to home/away in data/raw/todaysgames_normalized.csv
- Write the updated CSVs back in-place
- Emit diagnostics but DO NOT fail the job
"""

from pathlib import Path
import pandas as pd
import numpy as np

# Paths
ROOT = Path(".")
SUM_DIR = ROOT / "summaries" / "07_final"
SUM_DIR.mkdir(parents=True, exist_ok=True)

BATTERS_PROJ  = ROOT / "data" / "_projections" / "batter_props_projected_final.csv"
BATTERS_EXP   = ROOT / "data" / "_projections" / "batter_props_expanded_final.csv"
LINEUPS_CSV   = ROOT / "data" / "raw" / "lineups.csv"
TGN_CSV       = ROOT / "data" / "raw" / "todaysgames_normalized.csv"

def _to_int_str(s):
    """Normalize ids to string integers where possible (preserves 'UNKNOWN')."""
    s = s.astype(str).str.strip()
    s = s.replace({"nan": np.nan})
    # Keep 'UNKNOWN' as-is
    s = s.where(s.eq("UNKNOWN") | s.isna(), s.str.replace(r"\.0$", "", regex=True))
    return s

def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required input: {path}")
    return pd.read_csv(path)

def inject_team_and_game(df: pd.DataFrame, who: str) -> pd.DataFrame:
    out = df.copy()

    # --- team_id via lineups ---
    lu = load_csv(LINEUPS_CSV)
    need_lu = {"player_id", "team_id"}
    miss_lu = sorted(list(need_lu - set(lu.columns)))
    if miss_lu:
        raise RuntimeError(f"{LINEUPS_CSV} missing columns: {miss_lu}")

    # normalize ids as strings
    out["player_id"] = _to_int_str(out["player_id"])
    lu["player_id"]  = _to_int_str(lu["player_id"])
    lu["team_id"]    = _to_int_str(lu["team_id"])

    if "team_id" not in out.columns:
        out = out.merge(lu[["player_id","team_id"]], on="player_id", how="left")
    else:
        # backfill only where null
        out = out.merge(lu[["player_id","team_id"]].rename(columns={"team_id":"team_id_from_lu"}),
                        on="player_id", how="left")
        out["team_id"] = out["team_id"].astype(str)
        out["team_id"] = out["team_id"].where(out["team_id"].notna() & out["team_id"].ne("nan"),
                                              out["team_id_from_lu"])
        out.drop(columns=["team_id_from_lu"], inplace=True)

    # --- game_id via todaysgames_normalized ---
    tgn = load_csv(TGN_CSV)
    need_tgn = {"game_id","home_team_id","away_team_id"}
    miss_tgn = sorted(list(need_tgn - set(tgn.columns)))
    if miss_tgn:
        raise RuntimeError(f"{TGN_CSV} missing columns: {miss_tgn}")

    # normalize
    for col in ["home_team_id","away_team_id","game_id"]:
        tgn[col] = _to_int_str(tgn[col])
    out["team_id"] = _to_int_str(out["team_id"])

    # match team_id to home
    home = tgn.rename(columns={"home_team_id":"team_id", "game_id":"game_id_home"})
    away = tgn.rename(columns={"away_team_id":"team_id", "game_id":"game_id_away"})
    out = out.merge(home[["team_id","game_id_home"]], on="team_id", how="left")
    out = out.merge(away[["team_id","game_id_away"]], on="team_id", how="left")

    # final game_id: prefer the one that matched
    if "game_id" in out.columns:
        # keep existing where valid, else coalesce
        base = _to_int_str(out["game_id"])
    else:
        base = pd.Series([np.nan]*len(out))

    out["game_id"] = base.where(base.notna() & base.ne("nan"),
                                _to_int_str(out["game_id_home"]).where(
                                    _to_int_str(out["game_id_home"]).notna()
                                ).fillna(_to_int_str(out["game_id_away"])))

    out.drop(columns=[c for c in ["game_id_home","game_id_away"] if c in out.columns], inplace=True)

    # diagnostics
    missing_team = out["team_id"].isna() | out["team_id"].eq("nan")
    missing_game = out["game_id"].isna() | out["game_id"].eq("nan")

    if missing_team.any():
        out.loc[missing_team, ["player_id"]].to_csv(
            SUM_DIR / f"missing_team_id_in_{who}.csv", index=False
        )
        print(f"[WARN] {who}: {missing_team.sum()} rows missing team_id "
              f"(summaries/07_final/missing_team_id_in_{who}.csv)")

    if missing_game.any():
        out.loc[missing_game, ["player_id","team_id"]].to_csv(
            SUM_DIR / f"missing_game_id_in_{who}.csv", index=False
        )
        print(f"[WARN] {who}: {missing_game.sum()} rows missing game_id "
              f"(summaries/07_final/missing_game_id_in_{who}.csv)")

    return out

def main():
    print("PREP: injecting team_id and game_id into batter *_final.csv")

    bat_proj = load_csv(BATTERS_PROJ)
    bat_exp  = load_csv(BATTERS_EXP)

    # ensure player_id exists
    for name, df in [("batter_props_projected_final", bat_proj),
                     ("batter_props_expanded_final", bat_exp)]:
        if "player_id" not in df.columns:
            raise RuntimeError(f"{name} missing column 'player_id'")

    bat_proj2 = inject_team_and_game(bat_proj, "batter_props_projected_final")
    bat_exp2  = inject_team_and_game(bat_exp,  "batter_props_expanded_final")

    # write back in-place (preserve column order where possible)
    bat_proj2.to_csv(BATTERS_PROJ, index=False)
    bat_exp2.to_csv(BATTERS_EXP, index=False)

    print(f"OK: wrote {BATTERS_PROJ} and {BATTERS_EXP}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # still fail loudly here so the step shows what to fix
        # (Your workflow summarizes logs and artifacts.)
        raise
