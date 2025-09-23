#!/usr/bin/env python3
import os, sys, pandas as pd

OUT = "data/_projections/pitcher_props_projected_final.csv"
REQUIRED = ["player_id","game_id","team_id","opponent_team_id","pa"]

def main():
    if not os.path.exists(OUT):
        print(f"[ensure_pitcher_props_schema] missing file: {OUT}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(OUT)

    # Add required columns if missing
    if "pa" not in df.columns:
        df["pa"] = 0

    # Coerce types (best-effort)
    for c in ["game_id","team_id","opponent_team_id","pa"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # player_id stays string; trim/normalize
    if "player_id" in df.columns:
        df["player_id"] = df["player_id"].astype(str).str.strip()

    # Reorder columns exactly
    df = df[[c for c in REQUIRED if c in df.columns]]

    # Drop dup rows if any
    df = df.drop_duplicates()

    df.to_csv(OUT, index=False)
    print(f"[ensure_pitcher_props_schema] enforced columns on {OUT} -> {list(df.columns)}; rows={len(df)}")

if __name__ == "__main__":
    main()
