#!/usr/bin/env python3
# Build consolidated player → team master and inject MLB team_id/team_code via team_directory.csv

import os
import re
import unicodedata
import pandas as pd
from pathlib import Path

# ---------- Config ----------
TEAM_CSV_DIR   = Path("data/team_csvs")
TEAM_DIR_FILE  = Path("data/manual/team_directory.csv")  # requires: team_id,team_code,canonical_team,team_name,clean_team_name,all_names
OUTPUT_FILE    = Path("data/processed/player_team_master.csv")

# ---------- Name normalization (unchanged logic) ----------
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFD", text)
    return "".join(c for c in text if unicodedata.category(c) != "Mn")

def _capitalize_mc_names_in_string(text):
    def repl(m):
        prefix = m.group(1)
        c1 = m.group(2).upper()
        rest = m.group(3).lower()
        return prefix.capitalize() + c1 + rest
    return re.sub(r"\b(mc)([a-z])([a-z]*)\b", repl, text, flags=re.IGNORECASE)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.title()
    name = _capitalize_mc_names_in_string(name)
    return name

# ---------- Team key helpers ----------
def norm_key(s: str) -> str:
    """Uppercase and remove all non-alphanumeric to build a merge key."""
    if s is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(s).upper())

def build_team_lookup(team_dir: pd.DataFrame) -> dict:
    """
    Build a dict: normalized_key -> (team_id, team_code, canonical_team).
    Keys include many aliases per row (canonical, official, clean, all_names split, code).
    """
    required = ["team_id", "team_code", "canonical_team", "team_name", "clean_team_name", "all_names"]
    missing = [c for c in required if c not in team_dir.columns]
    if missing:
        raise RuntimeError(f"{TEAM_DIR_FILE}: missing required columns: {missing}")

    lut = {}
    for _, row in team_dir.iterrows():
        tid  = row["team_id"]
        tcode = str(row["team_code"]).strip()
        canon = str(row["canonical_team"]).strip()
        keys = set()

        # Canonical / official names
        keys.add(norm_key(canon))
        keys.add(norm_key(row["team_name"]))
        keys.add(norm_key(row["clean_team_name"]))

        # Codes
        keys.add(norm_key(tcode))

        # All aliases by name
        if pd.notna(row.get("all_names")):
            for alias in str(row["all_names"]).split("|"):
                if alias.strip():
                    keys.add(norm_key(alias))

        for k in keys:
            if k and k not in lut:
                lut[k] = (int(tid), tcode, canon)
            # If collision, keep first; directory should be unique/clean.

    return lut

# ---------- Load team directory ----------
team_dir = pd.read_csv(TEAM_DIR_FILE)
team_dir["team_id"] = pd.to_numeric(team_dir["team_id"], errors="coerce").astype("Int64")
if team_dir["team_id"].isna().any():
    bad = team_dir[team_dir["team_id"].isna()]
    raise RuntimeError(f"{TEAM_DIR_FILE}: null team_id for rows: {bad.to_dict(orient='records')}")

team_lut = build_team_lookup(team_dir)

# ---------- Build player rows from team_csvs ----------
rows = []
for fn in os.listdir(TEAM_CSV_DIR):
    if not fn.endswith(".csv"):
        continue

    fpath = TEAM_CSV_DIR / fn
    if fn.startswith("batters_"):
        team_token = fn.replace("batters_", "").replace(".csv", "")
        df = pd.read_csv(fpath)
        if "last_name, first_name" in df.columns:
            for name in df["last_name, first_name"].dropna():
                rows.append({"name": normalize_name(name), "team": team_token, "type": "batter"})
    elif fn.startswith("pitchers_"):
        team_token = fn.replace("pitchers_", "").replace(".csv", "")
        df = pd.read_csv(fpath)
        if "last_name, first_name" in df.columns:
            for name in df["last_name, first_name"].dropna():
                rows.append({"name": normalize_name(name), "team": team_token, "type": "pitcher"})

master = pd.DataFrame(rows)

# ---------- Inject team_id / team_code via directory ----------
if master.empty:
    # Write empty with headers if no inputs
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    master.assign(team_id=pd.Series(dtype="Int64"),
                  team_code=pd.Series(dtype="string")).to_csv(OUTPUT_FILE, index=False)
    raise SystemExit(0)

master["team_key"] = master["team"].apply(norm_key)

def map_team(row):
    k = row["team_key"]
    if k in team_lut:
        tid, tcode, canon = team_lut[k]
        return pd.Series({"team_id": tid, "team_code": tcode, "canonical_team": canon})
    # Fail-fast for unknown keys
    raise RuntimeError(f"Unmapped team token '{row['team']}' → key '{k}'. Ensure it exists in {TEAM_DIR_FILE} (canonical or alias).")

mapped = master.apply(map_team, axis=1)
master = pd.concat([master.drop(columns=["team_key"]), mapped], axis=1)

# ---------- Sort and write ----------
master = master.sort_values(["team", "type", "name"]).reset_index(drop=True)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
master.to_csv(OUTPUT_FILE, index=False)
