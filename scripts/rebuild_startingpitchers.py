#!/usr/bin/env python3
# Build (or refresh) data/raw/startingpitchers_with_opp_context.csv
# Source of truth: data/raw/todaysgames_normalized.csv
# Fallbacks for missing pitcher_id: data/Data/pitchers_2017-2025.csv, tools/missing_pitcher_id.csv
from __future__ import annotations
import csv
from pathlib import Path
import pandas as pd

TGN = Path("data/raw/todaysgames_normalized.csv")
OUT = Path("data/raw/startingpitchers_with_opp_context.csv")

PITCHERS_STATIC = Path("data/Data/pitchers_2017-2025.csv")
MISSING_LEDGER  = Path("tools/missing_pitcher_id.csv")  # append-only

REQ_TGN = {
    "game_id","home_team_id","away_team_id",
    "pitcher_home","pitcher_away","pitcher_home_id","pitcher_away_id"
}

def read_csv_str(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(p)
    df = pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[])
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip().replace({"None":"","NaN":"","nan":""})
    return df

def to_int_str(x: str) -> str:
    s = str(x or "").strip()
    if not s: return ""
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s

def load_static_pitchers() -> dict[str,str]:
    """Return name -> player_id map from the 2017-2025 static file."""
    m: dict[str,str] = {}
    if PITCHERS_STATIC.exists():
        df = read_csv_str(PITCHERS_STATIC)
        # Try common columns
        name_cols = [c for c in ["name","player_name","full_name","Name"] if c in df.columns]
        id_col = next((c for c in ["player_id","mlb_id","id","Id"] if c in df.columns), None)
        if name_cols and id_col:
            for _, r in df.iterrows():
                nm = str(r[name_cols[0]]).strip()
                pid = to_int_str(r[id_col])
                if nm and pid:
                    m[nm] = pid
    return m

def load_missing_ledger() -> dict[str,str]:
    """Existing manual map in ledger (name,player_id)."""
    m: dict[str,str] = {}
    if MISSING_LEDGER.exists():
        with MISSING_LEDGER.open() as f:
            r = csv.DictReader(f)
            if r.fieldnames and "name" in r.fieldnames and "player_id" in r.fieldnames:
                for row in r:
                    nm = (row.get("name") or "").strip()
                    pid = to_int_str(row.get("player_id") or "")
                    if nm and pid:
                        m[nm] = pid
    return m

def append_missing(nm: str) -> None:
    MISSING_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    write_header = not MISSING_LEDGER.exists()
    # avoid duplicates
    seen = set()
    if MISSING_LEDGER.exists():
        with MISSING_LEDGER.open() as f:
            r = csv.DictReader(f)
            if r.fieldnames:
                for row in r:
                    seen.add((row.get("name") or "").strip())
    if nm in seen:
        return
    with MISSING_LEDGER.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["name","player_id","note"])
        if write_header:
            w.writeheader()
        w.writerow({"name": nm, "player_id":"", "note":"auto-added; needs id"})

def try_resolve_id(name: str, existing: str, s_map: dict[str,str], ledger_map: dict[str,str]) -> str:
    if existing:  # already present
        return existing
    # direct match in static file
    pid = s_map.get(name)
    if pid:
        return pid
    # match in ledger (in case you pre-filled it)
    pid = ledger_map.get(name)
    if pid:
        return pid
    # not found -> append to ledger and leave blank
    append_missing(name)
    return ""

def main():
    tgn = read_csv_str(TGN)
    missing = sorted(list(REQ_TGN - set(tgn.columns)))
    if missing:
        raise RuntimeError(f"{TGN} missing required columns: {missing}")

    s_map = load_static_pitchers()
    l_map = load_missing_ledger()

    rows = []
    for _, r in tgn.iterrows():
        gid  = to_int_str(r["game_id"])
        hid  = to_int_str(r["home_team_id"])
        aid  = to_int_str(r["away_team_id"])
        ph   = (r.get("pitcher_home") or "").strip()
        pa   = (r.get("pitcher_away") or "").strip()
        phid = to_int_str(r.get("pitcher_home_id",""))
        paid = to_int_str(r.get("pitcher_away_id",""))

        # resolve IDs with fallbacks
        phid = try_resolve_id(ph, phid, s_map, l_map)
        paid = try_resolve_id(pa, paid, s_map, l_map)

        rows.append({"pitcher_id": phid, "name": ph, "team_id": hid, "opponent_team_id": aid, "side":"home", "game_id": gid})
        rows.append({"pitcher_id": paid, "name": pa, "team_id": aid, "opponent_team_id": hid, "side":"away", "game_id": gid})

    out = pd.DataFrame(rows, columns=["pitcher_id","name","team_id","opponent_team_id","side","game_id"])
    out.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(out)} rows -> {OUT}")

if __name__ == "__main__":
    main()
