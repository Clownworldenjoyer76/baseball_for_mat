#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

# --- Filepaths ---
SRC = Path("data/raw/todaysgames.csv")
OUT = Path("data/end_chain/cleaned/games_today_cleaned.csv")

REQUIRED = [
    "game_id",
    "home_team",
    "away_team",
    "game_time",
    "pitcher_home",
    "pitcher_away",
]

def main():
    if not SRC.exists():
        raise SystemExit(f"❌ missing input: {SRC}")

    df = pd.read_csv(SRC)

    # normalize header whitespace/casing gently
    df.columns = [c.strip() for c in df.columns]

    # verify required columns exist exactly as provided
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise SystemExit(f"❌ todaysgames.csv missing columns: {missing}")

    # ensure game_id is string to preserve leading zeros / avoid float formatting
    try:
        df["game_id"] = df["game_id"].astype("Int64").astype("string")
    except Exception:
        df["game_id"] = df["game_id"].astype("string")

    # write out unchanged columns in original order
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"✅ Saved cleaned games file → {OUT} (rows={len(df)})")

if __name__ == "__main__":
    main()
