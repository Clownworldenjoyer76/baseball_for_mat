#!/usr/bin/env python3
# Purpose: Attach park factors to data/raw/todaysgames_normalized.csv
# Key change: join by home_team_id (stable MLB numeric ID), not abbreviations.
# Sources (exact paths):
#   - data/manual/park_factors_day.csv
#   - data/manual/park_factors_night.csv
#   - data/manual/park_factors_roof_closed.csv
#
# Selection logic:
#   - If a column named "roof_status" exists and equals "closed" (case-insensitive),
#     use roof-closed park factor.
#   - Else, determine day/night from game_time (ET string like "1:35 PM"):
#       night if hour >= 6 PM, else day.
#
# Output:
#   - Overwrites data/raw/todaysgames_normalized.csv with a new/updated column:
#       park_factor  (float)

from __future__ import annotations
import sys
from pathlib import Path
from typing import Optional
import pandas as pd

GAMES_CSV = Path("data/raw/todaysgames_normalized.csv")
PF_DAY_CSV = Path("data/manual/park_factors_day.csv")
PF_NIGHT_CSV = Path("data/manual/park_factors_night.csv")
PF_ROOF_CSV = Path("data/manual/park_factors_roof_closed.csv")

REQUIRED_GAME_COLS = {"home_team_id", "game_time"}  # must exist

def _load_pf(path: Path, label: str) -> pd.DataFrame:
    """
    Read a park-factor file with exact headers:
      - team_id
      - park_factor
    Returns DataFrame with columns: team_id (Int64), park_factor_<label> (float)
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_csv(path)
    cols = [c.strip() for c in df.columns]
    if set(map(str.lower, cols)) != {"team_id", "park_factor"}:
        raise ValueError(f"{path} must have exact headers: team_id, park_factor")
    df.columns = ["team_id", "park_factor"]
    # types
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")
    df["park_factor"] = pd.to_numeric(df["park_factor"], errors="coerce")
    return df.rename(columns={"park_factor": f"park_factor_{label}"})


def _parse_hour_et(timestr: Optional[str]) -> Optional[int]:
    """
    Parse "H:MM AM/PM" or "HH:MM AM/PM" to hour in 24h.
    Returns None if parse fails.
    """
    if not isinstance(timestr, str):
        return None
    s = timestr.strip().upper()
    # expected: "12:10 PM", "1:35 PM", etc.
    try:
        part_time, part_ampm = s.split()
        hour_str, _ = part_time.split(":")
        hour = int(hour_str)
        if part_ampm == "AM":
            return 0 if hour == 12 else hour
        if part_ampm == "PM":
            return 12 if hour == 12 else hour + 12
        return None
    except Exception:
        return None


def _choose_pf(row: pd.Series) -> float:
    """
    Choose final park_factor using row-level context:
      - If roof_status == 'closed' -> park_factor_roof
      - Else if night (hour >= 18) -> park_factor_night
      - Else -> park_factor_day
    Missing values propagate as NaN.
    """
    roof = str(row.get("roof_status", "")).strip().lower()
    if roof == "closed":
        return row.get("park_factor_roof", float("nan"))
    hour = row.get("_hour24")
    if isinstance(hour, (int, float)) and pd.notna(hour) and hour >= 18:
        return row.get("park_factor_night", float("nan"))
    return row.get("park_factor_day", float("nan"))


def main() -> None:
    # Load games
    if not GAMES_CSV.exists():
        print(f"ERROR: {GAMES_CSV} not found", file=sys.stderr)
        sys.exit(1)
    games = pd.read_csv(GAMES_CSV)

    # Validate required columns
    missing = [c for c in REQUIRED_GAME_COLS if c not in games.columns]
    if missing:
        print(f"ERROR: {GAMES_CSV} missing required columns: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # Types
    games["home_team_id"] = pd.to_numeric(games["home_team_id"], errors="coerce").astype("Int64")

    # Derive hour for day/night selection
    games["_hour24"] = games["game_time"].apply(_parse_hour_et)

    # Load park factors (exact headers enforced)
    pf_day = _load_pf(PF_DAY_CSV, "day")
    pf_night = _load_pf(PF_NIGHT_CSV, "night")
    pf_roof = _load_pf(PF_ROOF_CSV, "roof")

    # Merge by numeric team ID
    merged = games.merge(pf_day, how="left", left_on="home_team_id", right_on="team_id")
    merged = merged.drop(columns=["team_id"])
    merged = merged.merge(pf_night, how="left", left_on="home_team_id", right_on="team_id")
    merged = merged.drop(columns=["team_id"])
    merged = merged.merge(pf_roof, how="left", left_on="home_team_id", right_on="team_id")
    merged = merged.drop(columns=["team_id"])

    # Compute final park_factor
    merged["park_factor"] = merged.apply(_choose_pf, axis=1)

    # Clean and write back
    merged = merged.drop(columns=["_hour24"], errors="ignore")
    GAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(GAMES_CSV, index=False)

    print("✅ Updated park factors in data/raw/todaysgames_normalized.csv (joined by home_team_id)")

if __name__ == "__main__":
    main()
