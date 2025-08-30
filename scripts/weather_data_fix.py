#!/usr/bin/env python3
# /home/runner/work/baseball_for_mat/baseball_for_mat/scripts/weather_data_fix.py

import pandas as pd
from pathlib import Path

WEATHER_FILE = Path("data/weather_adjustments.csv")
MAP_FILE     = Path("data/Data/team_name_map.csv")

def _require(df: pd.DataFrame, cols: list[str], where: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{where}: missing columns {miss}")

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _build_team_map(path: Path) -> dict:
    m = pd.read_csv(path)
    _require(m, ["name", "team"], str(path))
    return { _norm(n): str(t).strip() for n, t in zip(m["name"], m["team"]) }

def _canon(series: pd.Series, name_map: dict) -> pd.Series:
    return series.map(lambda s: name_map.get(_norm(s), str(s).strip()))

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
    df = pd.read_csv(WEATHER_FILE)

    # Preserve game_id/date and canonicalize names
    try:
        name_map = _build_team_map(MAP_FILE)
        if "home_team" in df.columns:
            df["home_team"] = _canon(df["home_team"], name_map)
        if "away_team" in df.columns:
            df["away_team"] = _canon(df["away_team"], name_map)
    except Exception as e:
        print(f"⚠️ Name map skipped: {e}")

    if "weather_factor" not in df.columns:
        print("🧮 Calculate weather_factor...")
        df["weather_factor"] = calculate_weather_factor(df)
    else:
        print("✅ weather_factor exists; skip calc.")

    Path(WEATHER_FILE).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(WEATHER_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()
