#!/usr/bin/env python3
"""
Build data/weather_input.csv from normalized games + stadium metadata + team directory,
emitting TEAM IDS and required stadium fields.

Inputs
- data/raw/todaysgames_normalized.csv
- data/manual/stadium_master.csv
- data/manual/team_directory.csv

Output
- data/weather_input.csv  (with: home_team_id, away_team_id, venue, city, latitude, longitude,
                           roof_type, game_time, date)
"""
import pandas as pd
from pathlib import Path
from datetime import datetime

GAMES = Path("data/raw/todaysgames_normalized.csv")
STAD  = Path("data/manual/stadium_master.csv")
TEAMS = Path("data/manual/team_directory.csv")
OUT   = Path("data/weather_input.csv")

def _require(df: pd.DataFrame, cols: list[str], where: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{where}: missing required columns: {miss}")

def _norm(s: str) -> str:
    return str(s).strip().lower()

def _coerce_bool(s):
    if pd.isna(s):
        return None
    v = str(s).strip().lower()
    if v in {"1","true","t","y","yes"}:
        return True
    if v in {"0","false","f","n","no"}:
        return False
    return None

def _derive_roof_type(row) -> str:
    # Priority: explicit roof_type; else roof; else is_dome; else unknown
    for k in ("roof_type","roof"):
        if k in row and pd.notna(row[k]):
            v = str(row[k]).strip().lower()
            if any(x in v for x in ("open","outdoor")): return "open"
            if "retract" in v: return "retractable"
            if any(x in v for x in ("dome","closed","fixed","indoor","roofed")): return "dome"
            return v or "unknown"
    if "is_dome" in row:
        b = _coerce_bool(row["is_dome"])
        if b is True:  return "dome"
        if b is False: return "open"
    return "unknown"

def _pick_date(g: pd.DataFrame) -> pd.Series:
    for c in ("date","game_date","Date","GAME_DATE"):
        if c in g.columns:
            return pd.to_datetime(g[c], errors="coerce").dt.strftime("%Y-%m-%d")
    # Fallback: today (ET-ish); leave as naive ISO date
    today = datetime.now().strftime("%Y-%m-%d")
    return pd.Series([today]*len(g), index=g.index)

def main():
    g = pd.read_csv(GAMES, dtype=str)
    # Accept either ids already present or canonical names to map via team_directory
    have_ids = set(g.columns) & {"home_team_id","away_team_id"}
    need_ids = not {"home_team_id","away_team_id"}.issubset(g.columns)

    # Pull names for joining if needed
    name_cols = []
    for c in ("home_team_canonical","home_team","home","Home"):
        if c in g.columns: name_cols.append(("home", c)); break
    for c in ("away_team_canonical","away_team","away","Away"):
        if c in g.columns: name_cols.append(("away", c)); break

    # Team directory: support several header variants
    t = pd.read_csv(TEAMS, dtype=str)
    # Try to find id and name columns
    id_col = next((c for c in t.columns if _norm(c) in {"team_id","id","mlb_id"}), None)
    name_col = next((c for c in t.columns if _norm(c) in {"canonical_team","team","name","team_name"}), None)
    if name_col is None or id_col is None:
        raise RuntimeError(f"{TEAMS}: missing a team id/name column (have {list(t.columns)})")
    t[id_col] = t[id_col].astype(str).str.strip()
    t[name_col] = t[name_col].astype(str).str.strip()

    # Build stadium table keyed by team_id
    s = pd.read_csv(STAD, dtype=str)
    # Flexible, but must include team_id + location fields
    if "team_id" not in s.columns:
        # Try to map from team name to id if present
        stad_name_col = next((c for c in s.columns if _norm(c) in {"canonical_team","team","name","team_name","home_team"}), None)
        if stad_name_col:
            s = s.merge(t[[id_col, name_col]], left_on=stad_name_col, right_on=name_col, how="left")
            s.rename(columns={id_col:"team_id"}, inplace=True)
        else:
            raise RuntimeError(f"{STAD}: must have team_id or recognizable team name")
    # Standardize fields
    # Venue/city
    if "venue" not in s.columns:
        alt = next((c for c in s.columns if _norm(c) in {"park","stadium","ballpark"}), None)
        if alt: s.rename(columns={alt:"venue"}, inplace=True)
    if "city" not in s.columns:
        alt = next((c for c in s.columns if _norm(c) in {"location_city","market","metro"}), None)
        if alt: s.rename(columns={alt:"city"}, inplace=True)
    # Coordinates
    if "latitude" not in s.columns:
        alt = next((c for c in s.columns if "lat" in _norm(c)), None)
        if alt: s.rename(columns={alt:"latitude"}, inplace=True)
    if "longitude" not in s.columns:
        alt = next((c for c in s.columns if "lon" in _norm(c) or "long" in _norm(c)), None)
        if alt: s.rename(columns={alt:"longitude"}, inplace=True)

    # Derive roof_type column robustly
    if "roof_type" not in s.columns:
        s["roof_type"] = s.apply(_derive_roof_type, axis=1)
    else:
        s["roof_type"] = s["roof_type"].apply(lambda v: _derive_roof_type({"roof_type": v}))

    _require(s, ["team_id","venue","city","latitude","longitude","roof_type"], str(STAD))

    # Build the games table with IDs
    out = pd.DataFrame(index=g.index)

    # Date & time
    out["date"] = _pick_date(g)
    # Prefer an explicit game_time column; else fallback to first time-like column
    if "game_time" in g.columns:
        out["game_time"] = g["game_time"].astype(str).str.strip()
    else:
        tm_col = next((c for c in g.columns if "time" in _norm(c)), None)
        if tm_col: out["game_time"] = g[tm_col].astype(str).str.strip()
        else: out["game_time"] = ""

    # Determine IDs
    if need_ids:
        if not name_cols:
            raise RuntimeError(f"{GAMES}: need team ids or recognizable team name columns for home/away")
        # Home
        home_name = dict(name_cols).get("home")
        away_name = dict(name_cols).get("away")
        if home_name is None or away_name is None:
            raise RuntimeError(f"{GAMES}: missing home/away team name columns")
        g_home = g[[home_name]].rename(columns={home_name:"team_name"})
        g_away = g[[away_name]].rename(columns={away_name:"team_name"})
        # Normalize names for join
        t["_key"] = t[name_col].map(_norm)
        g_home["_key"] = g_home["team_name"].map(_norm)
        g_away["_key"] = g_away["team_name"].map(_norm)
        home_map = g_home.merge(t[["_key", id_col]], on="_key", how="left")[id_col].rename("home_team_id")
        away_map = g_away.merge(t[["_key", id_col]], on="_key", how="left")[id_col].rename("away_team_id")
        out["home_team_id"] = home_map.astype(str)
        out["away_team_id"] = away_map.astype(str)
    else:
        out["home_team_id"] = g["home_team_id"].astype(str)
        out["away_team_id"] = g["away_team_id"].astype(str)

    # Join stadium info twice (home team is the venue)
    s_small = s[["team_id","venue","city","latitude","longitude","roof_type"]].copy()
    s_small["team_id"] = s_small["team_id"].astype(str).str.strip()

    out = out.join(
        out.merge(s_small, left_on="home_team_id", right_on="team_id", how="left")[
            ["venue","city","latitude","longitude","roof_type"]
        ]
    )

    # Final sanity checks
    _require(out, ["home_team_id","away_team_id","venue","city","latitude","longitude","roof_type","game_time","date"], "weather_input")

    # Surface missing critical fields
    req = ["venue","city","latitude","longitude"]
    missing = out[req].isna().any(axis=1)
    if missing.any():
        print("⚠️ Warning: missing values detected in:", ", ".join(req))
        print(out.loc[missing, ["home_team_id","away_team_id"] + req].head(3).to_string(index=False))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

if __name__ == "__main__":
    main()
