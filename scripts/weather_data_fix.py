#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/weather_data_fix.py

import pandas as pd
from pathlib import Path

WEATHER_FILE = Path("data/weather_adjustments.csv")
TEAM_DIR     = Path("data/manual/team_directory.csv")

def _require(df: pd.DataFrame, cols: list[str], where: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{where}: missing columns {miss}")

def calculate_weather_factor(df: pd.DataFrame) -> pd.Series:
    factor = pd.Series(1.0, index=df.index)
    notes = df.get("notes", pd.Series("", index=df.index)).fillna("")
    factor[notes.eq("Roof closed")] = 1.00

    temp = pd.to_numeric(df.get("temperature"), errors="coerce")
    factor += (temp.fillna(70) - 70) * 0.005
    factor = factor.clip(lower=0.85, upper=1.15)

    wind_dir = df.get("wind_direction", pd.Series("", index=df.index)).astype(str).str.lower()
    wind_spd = pd.to_numeric(df.get("wind_speed"), errors="coerce").fillna(0)
    factor[wind_dir.eq("out")] += wind_spd[wind_dir.eq("out")] * 0.01
    factor[wind_dir.eq("in")]  -= wind_spd[wind_dir.eq("in")]  * 0.01

    return factor.clip(lower=0.80, upper=1.20).round(3)

def main():
    print("🔄 Load weather_adjustments.csv...")
    df = pd.read_csv(WEATHER_FILE, dtype=str)

    # Ensure team IDs exist and are clean
    team_dir = pd.read_csv(TEAM_DIR, dtype=str)
    _require(team_dir, ["team_id","team_code","canonical_team"], str(TEAM_DIR))

    # Coerce IDs to Int64 for clean comparisons
    for col in ["home_team_id","away_team_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Calculate weather_factor if missing
    if "weather_factor" not in df.columns:
        print("🧮 Calculate weather_factor...")
        df["weather_factor"] = calculate_weather_factor(df)
    else:
        print("✅ weather_factor exists; skip calc.")

    # Write back
    Path(WEATHER_FILE).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(WEATHER_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()
