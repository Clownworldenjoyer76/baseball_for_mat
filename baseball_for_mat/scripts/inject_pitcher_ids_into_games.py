#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# scripts/inject_pitcher_ids_into_games.py
# Resolves pitcher_home_id / pitcher_away_id in data/raw/todaysgames_normalized.csv
# Lookup order:
#   1) data/processed/player_team_master.csv
#   2) data/team_csvs/pitchers_*.csv
#   3) data/Data/pitchers_2017-2025.csv
#   4) tools/missing_pitcher_id.csv (treated as a mapping if player_id present)
# Any still-missing pitchers are APPENDED to tools/missing_pitcher_id.csv (never overwritten).

import csv
import glob
import unicodedata
from datetime import datetime
from pathlib import Path
import pandas as pd

# Inputs/outputs
GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")
MASTER_FILE = Path("data/processed/player_team_master.csv")
TEAM_PITCHERS_GLOB = "data/team_csvs/pitchers_*.csv"
ALL_PITCHERS_FILE = Path("data/Data/pitchers_2017-2025.csv")
MISSING_MAP_FILE = Path("tools/missing_pitcher_id.csv")   # both mapping IN and unresolved OUT

# Hard overrides (authoritative)
OVERRIDES = {
    "Richardson, Simeon Woods": 680573,
    "Gipson-Long, Sawyer": 687830,
    "Berríos, José": 621244,
}

# ----------------- helpers -----------------

def _norm_ascii(s: str) -> str:
    s = (s or "").strip()
    # strip accents -> ASCII
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _variants(name: str):
    """Yield a few matching keys for a given name."""
    n = (name or "").strip()
    if not n:
        return []
    out = {n}
    out.add(_norm_ascii(n))
    # Try flipping "Last, First" <-> "First Last"
    if "," in n:
        parts = [p.strip() for p in n.split(",", 1)]
        if len(parts) == 2:
            flipped = f"{parts[1]} {parts[0]}".strip()
            out.add(flipped)
            out.add(_norm_ascii(flipped))
    else:
        # try building "Last, First" from "First Last"
        ps = n.split()
        if len(ps) >= 2:
            last = ps[-1]
            first = " ".join(ps[:-1])
            lf = f"{last}, {first}"
            out.add(lf)
            out.add(_norm_ascii(lf))
    return list(out)

def _add_name_to_map(df: pd.DataFrame, name_col="name", id_col="player_id"):
    m = {}
    if df is None or df.empty:
        return m
    cols = {c.lower(): c for c in df.columns}
    if name_col.lower() not in cols or id_col.lower() not in cols:
        return m
    ncol = cols[name_col.lower()]
    icol = cols[id_col.lower()]
    for _, r in df[[ncol, icol]].dropna().iterrows():
        name = str(r[ncol]).strip()
        pid = pd.to_numeric(r[icol], errors="coerce")
        if pd.isna(pid):
            continue
        pid = int(pid)
        # store direct key + ascii/variant keys
        for key in _variants(name) + [name]:
            if key and key not in m:
                m[key] = pid
    return m

def _read_csv_str(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        return None

def load_games() -> pd.DataFrame:
    if not GAMES_FILE.exists():
        raise FileNotFoundError(f"{GAMES_FILE} not found")
    df = pd.read_csv(GAMES_FILE, dtype=str, keep_default_na=False)
    req = {"game_id", "home_team", "away_team", "home_team_id", "away_team_id", "pitcher_home", "pitcher_away"}
    missing = req - set(df.columns)
    if missing:
        raise RuntimeError(f"INSUFFICIENT INFORMATION: {GAMES_FILE} missing columns: {sorted(missing)}")
    # Ensure id columns exist for passthrough
    for col in ("pitcher_home_id", "pitcher_away_id"):
        if col not in df.columns:
            df[col] = pd.NA
    return df

def build_name_to_id() -> dict:
    name_to_id: dict[str, int] = {}

    # 1) master file
    mf = _read_csv_str(MASTER_FILE)
    name_to_id.update(_add_name_to_map(mf, "name", "player_id"))

    # 2) team pitchers files
    for p in glob.glob(TEAM_PITCHERS_GLOB):
        df = _read_csv_str(Path(p))
        name_to_id.update(_add_name_to_map(df, "name", "player_id"))

    # 3) static all-years pitchers
    ap = _read_csv_str(ALL_PITCHERS_FILE)
    # Accept a few common column names for player id
    if ap is not None:
        pid_col = None
        for cand in ("player_id", "mlb_id", "id"):
            if cand in {c.lower() for c in ap.columns}:
                pid_col = cand
                break
        # name column guess
        name_col = None
        for cand in ("name", "player_name", "full_name"):
            if cand in {c.lower() for c in ap.columns}:
                name_col = cand
                break
        if name_col and pid_col:
            name_to_id.update(_add_name_to_map(ap, name_col, pid_col))

    # 4) tools/missing_pitcher_id.csv as a mapping (if user filled IDs there)
    mp = _read_csv_str(MISSING_MAP_FILE)
    if mp is not None:
        # expect at least name + player_id; ignore rows without player_id
        cols_lower = {c.lower(): c for c in mp.columns}
        ncol = None
        icol = None
        for cand in ("name", "player_name"):
            if cand in cols_lower:
                ncol = cols_lower[cand]
                break
        for cand in ("player_id", "mlb_id", "id"):
            if cand in cols_lower:
                icol = cols_lower[cand]
                break
        if ncol and icol:
            name_to_id.update(_add_name_to_map(mp, ncol, icol))

    # 5) hard overrides last (authoritative)
    for k, v in OVERRIDES.items():
        for key in _variants(k) + [k]:
            name_to_id[key] = int(v)

    return name_to_id

def resolve_id(pitcher_name: str, existing_id, name_map: dict):
    # Leave "Undecided" as missing
    if isinstance(pitcher_name, str) and pitcher_name.strip().lower() == "undecided":
        return pd.NA
    # If already present, keep
    if pd.notna(existing_id):
        return existing_id
    # Try multiple keys
    for key in _variants(pitcher_name) + [pitcher_name]:
        if not key:
            continue
        pid = name_map.get(key)
        if pid is not None:
            return int(pid)
    return pd.NA

def append_unresolved(rows: list[dict]):
    """Append unresolved pitchers to tools/missing_pitcher_id.csv, creating header if absent."""
    if not rows:
        return
    MISSING_MAP_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_header = not MISSING_MAP_FILE.exists() or MISSING_MAP_FILE.stat().st_size == 0
    fieldnames = [
        "date_utc",
        "name",
        "game_id",
        "side",
        "home_team_id",
        "away_team_id",
        "home_team",
        "away_team",
        "player_id",   # leave blank for user to fill if known
    ]
    with MISSING_MAP_FILE.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        for r in rows:
            # ensure only allowed keys are written
            w.writerow({k: r.get(k, "") for k in fieldnames})

# ----------------- main -----------------

def main():
    games = load_games()
    name_map = build_name_to_id()

    unresolved: list[dict] = []
    today_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Inject IDs
    def _inject(row, side: str):
        name = row[f"pitcher_{side}"]
        existing = row.get(f"pitcher_{side}_id", pd.NA)
        pid = resolve_id(name, existing, name_map)
        if pd.isna(pid):
            unresolved.append({
                "date_utc": today_utc,
                "name": name,
                "game_id": row.get("game_id", ""),
                "side": side,
                "home_team_id": row.get("home_team_id", ""),
                "away_team_id": row.get("away_team_id", ""),
                "home_team": row.get("home_team", ""),
                "away_team": row.get("away_team", ""),
                "player_id": "",  # left blank for user backfill
            })
        return pid

    games["pitcher_home_id"] = games.apply(lambda r: _inject(r, "home"), axis=1)
    games["pitcher_away_id"] = games.apply(lambda r: _inject(r, "away"), axis=1)

    # Normalize dtype to pandas nullable Int64
    for col in ("pitcher_home_id", "pitcher_away_id"):
        games[col] = pd.to_numeric(games[col], errors="coerce").astype("Int64")

    # Persist unresolved list (append-only)
    append_unresolved(unresolved)

    # Preserve all passthrough columns and write back
    GAMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(GAMES_FILE, index=False)

if __name__ == "__main__":
    main()
