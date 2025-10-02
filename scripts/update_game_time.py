#!/usr/bin/env python3
# scripts/update_game_time.py
#
# Purpose: Attach park factors to data/raw/todaysgames_normalized.csv
# Sources:
#   - data/raw/todaysgames_normalized.csv          (must have: home_team_id, game_time)
#   - data/manual/stadium_master.csv               (must have: team_id AND a roof/dome column)
#   - data/manual/park_factors_day.csv             (must have: team_id, park_factor or 'Park Factor')
#   - data/manual/park_factors_night.csv           (must have: team_id, park_factor or 'Park Factor')
#   - data/manual/park_factors_roof_closed.csv     (must have: team_id, park_factor or 'Park Factor')
#
# Behavior:
#   - Computes hour-of-day from game_time (AM/PM accepted)
#   - Determines roof-closed/dome from stadium_master roof column (accepts: roof, roof_status,
#     roof_closed, dome, is_dome)
#   - Chooses park factor: roof_closed → roof table; else day/night tables (6pm threshold)
#   - Writes park_factor back to todaysgames_normalized.csv
#
# Fails loudly if required files/columns are missing.

from pathlib import Path
import pandas as pd
import sys

GAMES_CSV     = Path("data/raw/todaysgames_normalized.csv")
STADIUM_CSV   = Path("data/manual/stadium_master.csv")
PF_DAY_CSV    = Path("data/manual/park_factors_day.csv")
PF_NIGHT_CSV  = Path("data/manual/park_factors_night.csv")
PF_ROOF_CSV   = Path("data/manual/park_factors_roof_closed.csv")

REQUIRED_GAME_COLS = {"home_team_id", "game_time"}

def _die(msg: str, code: int = 1):
    print(f"INSUFFICIENT INFORMATION\n{msg}", file=sys.stderr)
    sys.exit(code)

def _to_int64(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def _norm_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def _pick_column(df: pd.DataFrame, candidates) -> str | None:
    cmap = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cmap:
            return cmap[cand.lower()]
    return None

def _load_games() -> pd.DataFrame:
    if not GAMES_CSV.exists():
        _die(f"Missing file: {GAMES_CSV}")
    df = _norm_columns(pd.read_csv(GAMES_CSV))
    missing = REQUIRED_GAME_COLS - set(df.columns)
    if missing:
        _die(f"{GAMES_CSV} missing required columns: {', '.join(sorted(missing))}")
    df["home_team_id"] = _to_int64(df["home_team_id"])
    return df

def _normalize_pf_columns(df: pd.DataFrame, label: str) -> pd.DataFrame:
    df = _norm_columns(df)
    team_col = _pick_column(df, ["team_id"])
    if not team_col:
        _die(f"{label} must include team_id")
    pf_col = _pick_column(df, ["park_factor", "Park Factor"])
    if not pf_col:
        _die(f"{label} must include 'park_factor' (any case) or 'Park Factor'")
    out = df.rename(columns={team_col: "team_id", pf_col: "park_factor"})
    out["team_id"] = _to_int64(out["team_id"])
    out["park_factor"] = pd.to_numeric(out["park_factor"], errors="coerce")
    return out[["team_id", "park_factor"]]

def _load_pf(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        _die(f"Missing file: {path}")
    raw = pd.read_csv(path)
    return _normalize_pf_columns(raw, f"{path.name} ({label})")

def _hour24(et):
    try:
        t = str(et).strip()
        if not t or t.lower() == "nan":
            return pd.NA
        # Accept "H:MM AM/PM"
        if "AM" in t.upper() or "PM" in t.upper():
            hh, rest = t.split(":", 1)
            mm, ampm = rest.split(" ")
            h = int(hh) % 12
            if ampm.upper() == "PM":
                h += 12
            return h
        # Accept 24h "HH:MM"
        hh, _ = t.split(":")
        return int(hh) % 24
    except Exception:
        return pd.NA

def _roof_closed_flag(roof_val: object) -> bool:
    """
    Normalize a variety of roof flags to a boolean "closed/dome" indicator.
    Accepts True/False, strings like 'closed', 'dome', 'indoor', 'roof closed', 'yes', 'y', '1'.
    """
    # Handle direct booleans
    if isinstance(roof_val, bool):
        return roof_val
    v = str(roof_val).strip().lower()
    if v in {"1", "true", "yes", "y"}:
        return True
    if any(token in v for token in ["closed", "dome", "indoor", "roof closed"]):
        return True
    return False

def _load_stadium_master() -> pd.DataFrame:
    if not STADIUM_CSV.exists():
        _die(f"Missing file: {STADIUM_CSV}")
    sm = _norm_columns(pd.read_csv(STADIUM_CSV))

    team_col = _pick_column(sm, ["team_id"])
    if not team_col:
        _die(f"{STADIUM_CSV} must include 'team_id'")

    # Accept any of these as the roof indicator (now includes 'is_dome')
    roof_col = _pick_column(sm, ["roof", "roof_status", "roof_closed", "dome", "is_dome"])
    if not roof_col:
        _die(f"{STADIUM_CSV} must include a roof indicator column: one of roof, roof_status, roof_closed, dome, is_dome")

    out = sm[[team_col, roof_col]].copy()
    out.columns = ["team_id", "roof_flag_raw"]
    out["team_id"] = _to_int64(out["team_id"])
    out["roof_closed"] = out["roof_flag_raw"].map(_roof_closed_flag)
    return out[["team_id", "roof_closed"]]

def _choose_pf(row):
    # roof closed first
    if bool(row.get("__roof_closed", False)) and pd.notna(row.get("pf_roof")):
        return row["pf_roof"]
    h = row.get("__hour24")
    if pd.notna(h) and h >= 18 and pd.notna(row.get("pf_night")):
        return row["pf_night"]
    return row.get("pf_day")

def main():
    games = _load_games()
    games["__hour24"] = games["game_time"].map(_hour24)

    # Load stadium master for roof flag
    stad = _load_stadium_master()

    # Load PF tables
    pf_day   = _load_pf(PF_DAY_CSV,   "day").rename(columns={"park_factor": "pf_day"})
    pf_night = _load_pf(PF_NIGHT_CSV, "night").rename(columns={"park_factor": "pf_night"})
    pf_roof  = _load_pf(PF_ROOF_CSV,  "roof").rename(columns={"park_factor": "pf_roof"})

    # Merge: games + roof flag (by home_team_id)
    merged = games.merge(stad, how="left", left_on="home_team_id", right_on="team_id")
    merged = merged.drop(columns=["team_id"])

    if merged["roof_closed"].isna().any():
        _die("Stadium roof mapping failed for one or more games (no roof flag for home_team_id in stadium_master.csv)")

    # Attach park factors by home_team_id
    merged = merged.merge(pf_day,   how="left", left_on="home_team_id", right_on="team_id").drop(columns=["team_id"])
    merged = merged.merge(pf_night, how="left", left_on="home_team_id", right_on="team_id").drop(columns=["team_id"])
    merged = merged.merge(pf_roof,  how="left", left_on="home_team_id", right_on="team_id").drop(columns=["team_id"])

    # Compute final park_factor
    merged["__roof_closed"] = merged["roof_closed"].astype(bool)
    merged["park_factor"] = merged.apply(_choose_pf, axis=1)

    # Cleanup helper columns
    merged = merged.drop(columns=["__hour24", "__roof_closed"], errors="ignore")

    # Save
    GAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(GAMES_CSV, index=False)
    filled = int(merged["park_factor"].notna().sum()) if "park_factor" in merged.columns else 0
    print(f"✅ Updated {GAMES_CSV} (park_factor filled {filled}/{len(merged)})")

if __name__ == "__main__":
    main()
