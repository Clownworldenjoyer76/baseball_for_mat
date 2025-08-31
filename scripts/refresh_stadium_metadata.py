#!/usr/bin/env python3
"""
Refresh stadium metadata for today's home teams by merging manual park factors.

Inputs
- data/raw/todaysgames_normalized.csv (needs home_team_id, home_team_canonical)
- data/Data/stadium_metadata.csv      (previous metadata with venue/city/lat/lon/roof if present; optional)
- data/manual/park_factors_day.csv
- data/manual/park_factors_night.csv
- data/manual/park_factors_roof_closed.csv   (optional override)

Output
- data/Data/stadium_metadata.csv  (only today's home teams, enriched)
"""
import pandas as pd
from pathlib import Path

GAMES = Path("data/raw/todaysgames_normalized.csv")
BASE  = Path("data/Data/stadium_metadata.csv")  # used to retain venue/lat/lon/roof if present
PF_DAY   = Path("data/manual/park_factors_day.csv")
PF_NIGHT = Path("data/manual/park_factors_night.csv")
PF_ROOF  = Path("data/manual/park_factors_roof_closed.csv")

OUT = BASE  # write back

def _load_csv(p: Path) -> pd.DataFrame:
    if p.exists():
        df = pd.read_csv(p)
        df.columns = [c.strip() for c in df.columns]
        return df
    return pd.DataFrame()

def main():
    games = _load_csv(GAMES)
    if games.empty:
        raise SystemExit("todaysgames_normalized.csv is empty or missing")

    # Keep only today's HOME teams (unique)
    homes = games[["home_team_id","home_team_canonical"]].drop_duplicates().rename(
        columns={"home_team_canonical":"home_team"}
    )

    # Park factors (expect columns: team_id / home_team / Park Factor or PF value)
    day = _load_csv(PF_DAY)
    night = _load_csv(PF_NIGHT)
    roof = _load_csv(PF_ROOF)

    for df in (day, night, roof):
        if not df.empty and "home_team_id" not in df.columns and "team_id" in df.columns:
            df.rename(columns={"team_id":"home_team_id"}, inplace=True)
        if "Park Factor" in df.columns and "park_factor" not in df.columns:
            df.rename(columns={"Park Factor":"park_factor"}, inplace=True)

    day.rename(columns={"park_factor":"park_factor_day"}, inplace=True)
    night.rename(columns={"park_factor":"park_factor_night"}, inplace=True)
    roof.rename(columns={"park_factor":"park_factor_roof_closed"}, inplace=True)

    meta_prev = _load_csv(BASE)

    # Prefer previous metadata for venue/city/lat/lon/roof_type if present
    keep_cols = ["home_team","venue","city","state","latitude","longitude","roof_type","time_of_day"]
    meta_prev = meta_prev[keep_cols] if not meta_prev.empty else pd.DataFrame(columns=keep_cols)

    meta = homes.merge(meta_prev, on="home_team", how="left")

    # Attach numeric team id for joins
    if "home_team_id" not in meta.columns and "team_id" in homes.columns:
        meta["home_team_id"] = homes["team_id"]

    # Merge manual PFs
    if not day.empty:
        meta = meta.merge(day[["home_team_id","park_factor_day"]], on="home_team_id", how="left")
    if not night.empty:
        meta = meta.merge(night[["home_team_id","park_factor_night"]], on="home_team_id", how="left")
    if not roof.empty:
        meta = meta.merge(roof[["home_team_id","park_factor_roof_closed"]], on="home_team_id", how="left")

    # Compute selected "Park Factor" using time_of_day and roof_type
    def _pick_pf(row):
        # If roof closed and override present, use it
        if str(row.get("roof_type","")).strip().lower() in {"closed","dome","fixed","roof closed"}:
            val = row.get("park_factor_roof_closed")
            if pd.notna(val):
                return float(val)
        # Else day/night
        tod = str(row.get("time_of_day","")).strip().lower()
        if tod == "day" and pd.notna(row.get("park_factor_day")):
            return float(row["park_factor_day"])
        if tod == "night" and pd.notna(row.get("park_factor_night")):
            return float(row["park_factor_night"])
        # Fallback: average of available
        vals = [row.get("park_factor_day"), row.get("park_factor_night")]
        vals = [float(v) for v in vals if pd.notna(v)]
        return sum(vals)/len(vals) if vals else None

    meta["Park Factor"] = meta.apply(_pick_pf, axis=1)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    meta.to_csv(OUT, index=False)

if __name__ == "__main__":
    main()
