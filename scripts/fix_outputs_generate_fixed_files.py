#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import sys

RAW_TODAY = Path("data/raw/todaysgames_normalized.csv")
MASTER    = Path("data/processed/player_team_master.csv")
SEASON_P  = Path("data/Data/pitchers.csv")
SUM_DIR   = Path("summaries/projections")
OUT_DIR   = Path("data/_projections")

SUM_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    return name.strip().lower().replace(".", "").replace(",", "")

def main():
    # Load raw today’s games
    df = pd.read_csv(RAW_TODAY)

    # Load master and season-level fallback
    master = pd.read_csv(MASTER) if MASTER.exists() else pd.DataFrame()
    season = pd.read_csv(SEASON_P) if SEASON_P.exists() else pd.DataFrame()

    if not master.empty and "player_name" in master.columns and "player_id" in master.columns:
        master["name_norm"] = master["player_name"].map(normalize_name)
    if not season.empty and "player_name" in season.columns and "player_id" in season.columns:
        season["name_norm"] = season["player_name"].map(normalize_name)

    # Track missing
    missing_rows = []

    for side in ["home", "away"]:
        pid_col = f"pitcher_{side}_id"
        name_col = f"pitcher_{side}"

        # Where ID is null, try to resolve
        mask = df[pid_col].isna()
        for idx in df[mask].index:
            pname = df.at[idx, name_col]
            norm = normalize_name(pname)

            resolved = None
            # Try master
            if not master.empty:
                hit = master.loc[master["name_norm"] == norm]
                if not hit.empty:
                    resolved = hit["player_id"].iloc[0]

            # Fallback: season pitchers
            if resolved is None and not season.empty:
                hit = season.loc[season["name_norm"] == norm]
                if not hit.empty:
                    resolved = hit["player_id"].iloc[0]

            if resolved is not None:
                df.at[idx, pid_col] = resolved
            else:
                missing_rows.append({
                    "game_id": df.at[idx, "game_id"],
                    "team_id": df.at[idx, f"{side}_team_id"],
                    "pitcher_name": pname
                })

    if missing_rows:
        miss_df = pd.DataFrame(missing_rows)
        miss_path = SUM_DIR / "missing_pitcher_ids.csv"
        miss_df.to_csv(miss_path, index=False)
        sys.stderr.write(f"❌ Missing pitcher IDs, see {miss_path}\n")
        sys.exit(1)

    # Write a fixed version for downstream scripts
    out_path = OUT_DIR / "todaysgames_normalized_fixed.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Fixed todaysgames_normalized written: {out_path}")

if __name__ == "__main__":
    main()
