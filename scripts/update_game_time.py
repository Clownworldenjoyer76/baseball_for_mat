#!/usr/bin/env python3
# Purpose: Attach park factors to data/raw/todaysgames_normalized.csv
# Robust to header case/variants in manual PF files:
#   accepts either 'park_factor' or 'Park Factor'
# Joins on home_team_id (nullable Int64)

from pathlib import Path
import pandas as pd
import sys

GAMES_CSV = Path("data/raw/todaysgames_normalized.csv")
PF_DAY_CSV  = Path("data/manual/park_factors_day.csv")
PF_NIGHT_CSV= Path("data/manual/park_factors_night.csv")
PF_ROOF_CSV = Path("data/manual/park_factors_roof_closed.csv")

REQUIRED_GAME_COLS = {"home_team_id", "game_time"}

def _die(msg):
    print(f"INSUFFICIENT INFORMATION\n{msg}", file=sys.stderr)
    sys.exit(1)

def _to_int64(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def _load_games() -> pd.DataFrame:
    if not GAMES_CSV.exists():
        _die(f"Missing file: {GAMES_CSV}")
    df = pd.read_csv(GAMES_CSV)
    missing = REQUIRED_GAME_COLS - set(df.columns)
    if missing:
        _die(f"{GAMES_CSV} missing required columns: {', '.join(sorted(missing))}")
    df["home_team_id"] = _to_int64(df["home_team_id"])
    return df

def _normalize_pf_columns(df: pd.DataFrame, label: str) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    if "team_id" not in cols:
        _die(f"{label} file must include team_id")
    # Accept either 'park_factor' or 'Park Factor'
    pf_col = cols.get("park_factor") or cols.get("park factor")
    if not pf_col:
        _die(f"{label} file must include park_factor (any case) or 'Park Factor'")
    out = df.rename(columns={pf_col: "park_factor"})
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
        hh, rest = t.split(":", 1)
        mm, ampm = rest.split(" ")
        h = int(hh) % 12
        if ampm.upper() == "PM":
            h += 12
        return h
    except Exception:
        return pd.NA

def _choose_pf(row):
    roof = str(row.get("roof_status", "")).strip().lower()
    if roof == "closed" and pd.notna(row.get("pf_roof")):
        return row["pf_roof"]
    h = row.get("_hour24")
    if pd.notna(h) and h >= 18 and pd.notna(row.get("pf_night")):
        return row["pf_night"]
    return row.get("pf_day")

def main():
    games = _load_games()
    games["_hour24"] = games["game_time"].map(_hour24)

    pf_day  = _load_pf(PF_DAY_CSV, "day").rename(columns={"park_factor": "pf_day"})
    pf_ngt  = _load_pf(PF_NIGHT_CSV, "night").rename(columns={"park_factor": "pf_night"})
    pf_roof = _load_pf(PF_ROOF_CSV, "roof").rename(columns={"park_factor": "pf_roof"})

    merged = games.merge(pf_day,  how="left", left_on="home_team_id", right_on="team_id").drop(columns=["team_id"])
    merged = merged.merge(pf_ngt,  how="left", left_on="home_team_id", right_on="team_id").drop(columns=["team_id"])
    merged = merged.merge(pf_roof, how="left", left_on="home_team_id", right_on="team_id").drop(columns=["team_id"])

    merged["park_factor"] = merged.apply(_choose_pf, axis=1)
    merged = merged.drop(columns=["_hour24"], errors="ignore")

    GAMES_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(GAMES_CSV, index=False)
    filled = int(merged["park_factor"].notna().sum())
    print(f"✅ Updated {GAMES_CSV} (park_factor filled {filled}/{len(merged)})")

if __name__ == "__main__":
    main()
