#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import datetime

def main():
    prep_path = Path("data/bets/prep/batter_props_final.csv")
    hist_path = Path("data/bets/player_props_history.csv")

    if not prep_path.exists():
        print(f"Prep file missing: {prep_path}")
        return

    # Load prep file
    df = pd.read_csv(prep_path)

    # Ensure required columns exist
    required = ["player_id","name","team","prop","line","value","over_probability","date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"Missing columns in prep: {missing}")
        return

    # Normalize numeric columns (coerce errors, replace pd.NA with None)
    for col in ["line", "value", "over_probability"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            df[col] = df[col].where(pd.notnull(df[col]), None)

    # Ensure date is datetime
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Add missing history columns if not present
    for col in ["game_id","prop_correct","prop_sort"]:
        if col not in df.columns:
            df[col] = None

    # Reorder to match history schema
    out_cols = ["player_id","name","team","prop","line","value",
                "over_probability","date","game_id","prop_correct","prop_sort"]
    df_out = df[out_cols]

    # Load or create history file
    if hist_path.exists():
        hist = pd.read_csv(hist_path)
    else:
        hist = pd.DataFrame(columns=out_cols)

    # Append new rows
    hist = pd.concat([hist, df_out], ignore_index=True)

    # Drop duplicates (same player_id, prop, date)
    hist.drop_duplicates(subset=["player_id","prop","date"], keep="last", inplace=True)

    # Save
    hist.to_csv(hist_path, index=False)
    print(f"Appended {len(df_out)} rows into {hist_path}, now {len(hist)} total.")

if __name__ == "__main__":
    main()
