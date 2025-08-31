#!/usr/bin/env python3
# Build consolidated player → team master, inject MLB team_id/team_code via team_directory.csv,
# and attach player_id from data/Data/batters.csv and data/Data/pitchers.csv.

import os
import re
import unicodedata
import pandas as pd
from pathlib import Path

# ===== Paths =====
TEAM_CSV_DIR  = Path("data/team_csvs")
TEAM_DIR_FILE = Path("data/manual/team_directory.csv")  # requires: team_id,team_code,canonical_team,team_name,clean_team_name,all_names
BATTERS_ID_CSV = Path("data/Data/batters.csv")
PITCHERS_ID_CSV = Path("data/Data/pitchers.csv")
OUTPUT_FILE   = Path("data/processed/player_team_master.csv")

# ===== Name normalization =====
def strip_accents(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFD", text)
    return "".join(c for c in text if unicodedata.category(c) != "Mn")

def _capitalize_mc_names_in_string(text: str) -> str:
    def repl(m):
        prefix = m.group(1)
        c1 = m.group(2).upper()
        rest = m.group(3).lower()
        return prefix.capitalize() + c1 + rest
    return re.sub(r"\b(mc)([a-z])([a-z]*)\b", repl, text, flags=re.IGNORECASE)

def normalize_person_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.title()
    name = _capitalize_mc_names_in_string(name)
    return name

# ===== Team key helpers =====
def norm_key(s: str) -> str:
    """Uppercase and remove non-alphanumerics to build a merge key."""
    if s is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(s).upper())

def build_team_lookup(team_dir: pd.DataFrame) -> dict:
    """
    Build dict: normalized_key -> (team_id, team_code, canonical_team).
    Keys cover canonical, official, clean, all_names (pipe-delimited), and code.
    """
    required = ["team_id", "team_code", "canonical_team", "team_name", "clean_team_name", "all_names"]
    missing = [c for c in required if c not in team_dir.columns]
    if missing:
        raise RuntimeError(f"{TEAM_DIR_FILE}: missing required columns: {missing}")

    lut = {}
    for _, row in team_dir.iterrows():
        tid = row["team_id"]
        if pd.isna(tid):
            raise RuntimeError(f"{TEAM_DIR_FILE}: null team_id found.")
        tid = int(tid)
        tcode = str(row["team_code"]).strip()
        canon = str(row["canonical_team"]).strip()

        keys = set()
        keys.add(norm_key(canon))
        keys.add(norm_key(row["team_name"]))
        keys.add(norm_key(row["clean_team_name"]))
        keys.add(norm_key(tcode))

        all_names = row.get("all_names")
        if pd.notna(all_names):
            for alias in str(all_names).split("|"):
                alias = alias.strip()
                if alias:
                    keys.add(norm_key(alias))

        for k in keys:
            if k and k not in lut:
                lut[k] = (tid, tcode, canon)
    return lut

# ===== Load team directory =====
if not TEAM_DIR_FILE.exists():
    raise RuntimeError(f"Missing {TEAM_DIR_FILE}")
team_dir = pd.read_csv(TEAM_DIR_FILE)
team_dir["team_id"] = pd.to_numeric(team_dir["team_id"], errors="coerce").astype("Int64")
if team_dir["team_id"].isna().any():
    bad = team_dir[team_dir["team_id"].isna()]
    raise RuntimeError(f"{TEAM_DIR_FILE}: null team_id rows: {bad.to_dict(orient='records')}")
team_lut = build_team_lookup(team_dir)

# ===== Build base rows from team_csvs =====
rows = []
if not TEAM_CSV_DIR.exists():
    raise RuntimeError(f"Missing {TEAM_CSV_DIR}")

for fn in os.listdir(TEAM_CSV_DIR):
    if not fn.endswith(".csv"):
        continue
    fpath = TEAM_CSV_DIR / fn
    if fn.startswith("batters_"):
        team_token = fn.replace("batters_", "").replace(".csv", "")
        df = pd.read_csv(fpath)
        if "last_name, first_name" in df.columns:
            for name in df["last_name, first_name"].dropna():
                rows.append({"name": normalize_person_name(name), "team": team_token, "type": "batter"})
    elif fn.startswith("pitchers_"):
        team_token = fn.replace("pitchers_", "").replace(".csv", "")
        df = pd.read_csv(fpath)
        if "last_name, first_name" in df.columns:
            for name in df["last_name, first_name"].dropna():
                rows.append({"name": normalize_person_name(name), "team": team_token, "type": "pitcher"})

master = pd.DataFrame(rows)

# If empty, write empty headers and exit
if master.empty:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    out = master.assign(team_id=pd.Series(dtype="Int64"),
                        team_code=pd.Series(dtype="string"),
                        canonical_team=pd.Series(dtype="string"),
                        player_id=pd.Series(dtype="Int64"))
    out.to_csv(OUTPUT_FILE, index=False)
    raise SystemExit(0)

# ===== Inject team_id / team_code =====
master["team_key"] = master["team"].apply(norm_key)

def map_team(row):
    k = row["team_key"]
    if k in team_lut:
        tid, tcode, canon = team_lut[k]
        return pd.Series({"team_id": tid, "team_code": tcode, "canonical_team": canon})
    raise RuntimeError(
        f"Unmapped team token '{row['team']}' → key '{k}'. "
        f"Ensure it exists in {TEAM_DIR_FILE} (canonical or alias)."
    )

mapped = master.apply(map_team, axis=1)
master = pd.concat([master.drop(columns=["team_key"]), mapped], axis=1)

# ===== Build player_id maps from data/Data/*.csv =====
def _detect_cols(df: pd.DataFrame, want_id: bool) -> tuple[str, str]:
    """
    Detect (name_col, id_col?) from a source df.
    Returns (name_col, id_col_or_empty).
    Error if name col missing. If want_id and not found, error.
    """
    # Candidate name columns (in order of preference)
    name_candidates = ["last_name, first_name", "name", "full_name", "player_name", "player"]
    # Candidate id columns
    id_candidates = ["player_id", "mlb_id", "id", "person_id"]

    name_col = next((c for c in name_candidates if c in df.columns), None)
    if not name_col:
        raise RuntimeError("ID source file: missing a recognizable name column "
                           "(expected one of: last_name, first_name | name | full_name | player_name | player)")

    id_col = next((c for c in id_candidates if c in df.columns), None)
    if want_id and not id_col:
        raise RuntimeError("ID source file: missing a recognizable player id column "
                           "(expected one of: player_id | mlb_id | id | person_id)")

    return name_col, id_col or ""

def _make_id_map(path: Path) -> dict:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    name_col, id_col = _detect_cols(df, want_id=True)
    # Normalize names to the same canonical form
    df["_norm_name"] = df[name_col].astype(str).map(normalize_person_name)
    # Coerce id numeric (Int64) but keep dictionary as Python int where possible
    ids = pd.to_numeric(df[id_col], errors="coerce").astype("Int64")
    df = df.loc[ids.notna(), ["_norm_name", id_col]]
    return {n: int(pid) for n, pid in zip(df["_norm_name"], df[id_col])}

batter_id_map = _make_id_map(BATTERS_ID_CSV)
pitcher_id_map = _make_id_map(PITCHERS_ID_CSV)

def map_player_id(row) -> pd.Series:
    nm = row["name"]
    if row["type"] == "batter":
        pid = batter_id_map.get(nm)
    else:
        pid = pitcher_id_map.get(nm)
    return pd.Series({"player_id": pid if pid is not None else pd.NA})

master = pd.concat([master, master.apply(map_player_id, axis=1)], axis=1)

# ===== Validate =====
# Ensure team_id present for all
if master["team_id"].isna().any():
    bad = master[master["team_id"].isna()][["team", "name", "type"]].head(20).to_dict(orient="records")
    raise RuntimeError(f"Null team_id after mapping; sample rows: {bad}")

# Note: player_id may be missing if the name in Data files doesn't match.
# This is tolerated; downstream can use names or handle missing IDs explicitly.

# ===== Write =====
master = master[["name", "team", "type", "player_id", "team_id", "team_code", "canonical_team"]]
master = master.sort_values(["team", "type", "name"]).reset_index(drop=True)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
master.to_csv(OUTPUT_FILE, index=False)
