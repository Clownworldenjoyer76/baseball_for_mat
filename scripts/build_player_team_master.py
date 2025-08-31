#!/usr/bin/env python3
# Consolidate player→team master.
# - Carries player_id from team CSVs when present.
# - Falls back to data/Data/batters.csv and data/Data/pitchers.csv by normalized name.
# - Injects team_id/team_code/canonical_team from data/manual/team_directory.csv.
# - Writes unmatched player_id audit to summaries/fetchrosters/unmatched_player_ids.txt.

import os
import re
import unicodedata
import pandas as pd
from pathlib import Path

# ===== Paths =====
TEAM_CSV_DIR        = Path("data/team_csvs")
TEAM_DIR_FILE       = Path("data/manual/team_directory.csv")   # requires: team_id,team_code,canonical_team,team_name,clean_team_name,all_names
BATTERS_ID_CSV      = Path("data/Data/batters.csv")
PITCHERS_ID_CSV     = Path("data/Data/pitchers.csv")
OUTPUT_FILE         = Path("data/processed/player_team_master.csv")
AUDIT_DIR           = Path("summaries/fetchrosters")
AUDIT_UNMATCHED_IDS = AUDIT_DIR / "unmatched_player_ids.txt"

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

_SUFFIX_RE = re.compile(r"\b(Jr|Sr|II|III|IV|V)\.?\b", flags=re.IGNORECASE)

def normalized_join_key(name: str) -> str:
    """LAST, FIRST key without accents/punct/suffixes; stable for joins."""
    n = normalize_person_name(name)
    n = _SUFFIX_RE.sub("", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

# ===== Team key helpers =====
def norm_key(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(s).upper())

def build_team_lookup(team_dir: pd.DataFrame) -> dict:
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

        keys = {
            norm_key(canon),
            norm_key(row["team_name"]),
            norm_key(row["clean_team_name"]),
            norm_key(tcode),
        }
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

# ===== Build base rows from team_csvs (carry player_id when present) =====
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
            pid_col = "player_id" if "player_id" in df.columns else None
            for _, r in df.iterrows():
                nm = r.get("last_name, first_name")
                if pd.isna(nm):
                    continue
                rows.append({
                    "name": normalize_person_name(nm),
                    "join_key": normalized_join_key(nm),
                    "team": team_token,
                    "type": "batter",
                    "player_id": pd.to_numeric(r.get(pid_col), errors="coerce").astype("Int64") if pid_col else pd.NA
                })
    elif fn.startswith("pitchers_"):
        team_token = fn.replace("pitchers_", "").replace(".csv", "")
        df = pd.read_csv(fpath)
        if "last_name, first_name" in df.columns:
            pid_col = "player_id" if "player_id" in df.columns else None
            for _, r in df.iterrows():
                nm = r.get("last_name, first_name")
                if pd.isna(nm):
                    continue
                rows.append({
                    "name": normalize_person_name(nm),
                    "join_key": normalized_join_key(nm),
                    "team": team_token,
                    "type": "pitcher",
                    "player_id": pd.to_numeric(r.get(pid_col), errors="coerce").astype("Int64") if pid_col else pd.NA
                })

master = pd.DataFrame(rows)

# If empty, write empty headers and exit cleanly
if master.empty:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    master = master.reindex(columns=["name","team","type","player_id","team_id","team_code","canonical_team"])
    master.to_csv(OUTPUT_FILE, index=False)
    with open(AUDIT_UNMATCHED_IDS, "w") as f:
        f.write("")  # empty audit
    raise SystemExit(0)

# ===== Inject team_id / team_code / canonical_team =====
master["team_key"] = master["team"].apply(norm_key)

def map_team(row):
    k = row["team_key"]
    if k in team_lut:
        tid, tcode, canon = team_lut[k]
        return pd.Series({"team_id": tid, "team_code": tcode, "canonical_team": canon})
    raise RuntimeError(
        f"Unmapped team token '{row['team']}' → key '{k}'. "
        f"Add to {TEAM_DIR_FILE} (canonical or alias)."
    )

mapped = master.apply(map_team, axis=1)
master = pd.concat([master.drop(columns=["team_key"]), mapped], axis=1)

# ===== Fallback: fill missing player_id from Data/batters.csv and Data/pitchers.csv =====
def detect_cols(df: pd.DataFrame) -> tuple[str, str]:
    name_candidates = ["last_name, first_name", "name", "full_name", "player_name", "player"]
    id_candidates   = ["player_id", "mlb_id", "id", "person_id"]
    name_col = next((c for c in name_candidates if c in df.columns), None)
    id_col   = next((c for c in id_candidates if c in df.columns), None)
    if not name_col or not id_col:
        return "", ""
    return name_col, id_col

def make_id_map(path: Path) -> dict:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    name_col, id_col = detect_cols(df)
    if not name_col or not id_col:
        return {}
    df["_jk"] = df[name_col].astype(str).map(normalized_join_key)
    ids = pd.to_numeric(df[id_col], errors="coerce").astype("Int64")
    df = df.loc[df["_jk"].ne("") & ids.notna(), ["_jk", id_col]]
    return {jk: int(pid) for jk, pid in zip(df["_jk"], df[id_col])}

batter_map  = make_id_map(BATTERS_ID_CSV)
pitcher_map = make_id_map(PITCHERS_ID_CSV)

mask_missing = master["player_id"].isna()
if mask_missing.any():
    bmask = mask_missing & (master["type"] == "batter")
    pmask = mask_missing & (master["type"] == "pitcher")
    if bmask.any() and batter_map:
        master.loc[bmask, "player_id"] = master.loc[bmask, "join_key"].map(batter_map).astype("Int64")
    if pmask.any() and pitcher_map:
        master.loc[pmask, "player_id"] = master.loc[pmask, "join_key"].map(pitcher_map).astype("Int64")

# ===== Audit unmatched player_ids =====
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
unmatched = master[master["player_id"].isna()][["team","type","name"]]
with open(AUDIT_UNMATCHED_IDS, "w") as f:
    if unmatched.empty:
        f.write("")  # nothing to report
    else:
        for _, r in unmatched.sort_values(["team","type","name"]).iterrows():
            f.write(f"{r['team']},{r['type']},{r['name']}\n")

# ===== Finalize and write =====
master = master[["name","team","type","player_id","team_id","team_code","canonical_team"]]
master = master.sort_values(["team","type","name"]).reset_index(drop=True)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
master.to_csv(OUTPUT_FILE, index=False)
