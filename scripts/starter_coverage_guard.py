#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

# Paths (relative to repo root)
P_STARTERS     = Path("data/end_chain/final/startingpitchers.csv")
P_MEGA_Z       = Path("data/_projections/pitcher_mega_z_final.csv")

# Summary outputs
OUT_DIR        = Path("summaries/projections")
OUT_DIR.mkdir(parents=True, exist_ok=True)
P_COVERAGE     = OUT_DIR / "mega_z_starter_coverage.csv"
P_MISSING      = OUT_DIR / "mega_z_starter_missing.csv"

def read_csv_safe(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"Missing required input: {p}")
    return pd.read_csv(p)

def coerce_id_series(s: pd.Series) -> pd.Series:
    """
    Force player_id to a comparable dtype:
    - strip whitespace
    - coerce to numeric (Int64); preserve NA if truly missing
    """
    s = s.astype(str).str.strip()
    # empty strings -> NA before numeric
    s = s.mask(s.eq("") | s.eq("nan") | s.eq("None"))
    s = pd.to_numeric(s, errors="coerce").astype("Int64")
    return s

def main():
    starters = read_csv_safe(P_STARTERS).copy()
    mega     = read_csv_safe(P_MEGA_Z).copy()

    # Normalize column names we need
    if "player_id" not in starters.columns:
        # allow alternate casing just in case
        alt = [c for c in starters.columns if c.lower() == "player_id"]
        if alt:
            starters = starters.rename(columns={alt[0]: "player_id"})
        else:
            raise KeyError(f"'player_id' column not found in {P_STARTERS}")

    if "player_id" not in mega.columns:
        alt = [c for c in mega.columns if c.lower() == "player_id"]
        if alt:
            mega = mega.rename(columns={alt[0]: "player_id"})
        else:
            raise KeyError(f"'player_id' column not found in {P_MEGA_Z}")

    # Coerce ids
    starters["player_id"] = coerce_id_series(starters["player_id"])
    mega["player_id"]     = coerce_id_series(mega["player_id"])

    # Keep only rows with a valid starter id
    starters_valid = starters.dropna(subset=["player_id"]).copy()

    # Build sets for coverage
    starter_ids = set(starters_valid["player_id"].dropna().tolist())
    mega_ids    = set(mega["player_id"].dropna().tolist())

    missing_ids = sorted(starter_ids - mega_ids)

    # Enrich a small coverage table to inspect easily
    coverage = (
        starters_valid[["player_id", "game_id", "team_id"]]
        .drop_duplicates()
        .assign(in_mega_z=lambda df: df["player_id"].isin(mega_ids))
        .sort_values(["in_mega_z", "game_id", "team_id", "player_id"], ascending=[True, True, True, True])
    )
    coverage.to_csv(P_COVERAGE, index=False)

    # If anything missing, export details and fail
    if missing_ids:
        # Try to add names if present in starters
        name_cols = [c for c in starters_valid.columns if c.lower() in ("pitcher_home","pitcher_away","pitcher","name","name_norm")]
        base = starters_valid[["player_id","game_id","team_id"] + name_cols].drop_duplicates()
        missing_df = base[base["player_id"].isin(missing_ids)].sort_values(["game_id","team_id","player_id"])
        # If no name columns, at least output the ids
        if missing_df.empty:
            missing_df = pd.DataFrame({"player_id": missing_ids})
        missing_df.to_csv(P_MISSING, index=False)

        raise RuntimeError(
            f"Starter coverage failure: {len(missing_ids)} starter(s) absent in pitcher_mega_z. "
            f"See {P_COVERAGE} and {P_MISSING}."
        )
    else:
        # Still write an empty missing file for consistency
        pd.DataFrame(columns=["player_id","game_id","team_id"]).to_csv(P_MISSING, index=False)
        print(f"starter_coverage_guard: OK — all {len(starter_ids)} starters covered in mega_z.")

if __name__ == "__main__":
    try:
        pd.set_option("display.width", 200)
        main()
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)
