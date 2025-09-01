#!/usr/bin/env python3
# Populate data/adjusted/batters_away.csv by matching on numeric team_id.

import pandas as pd
import os
import sys
from pathlib import Path

BATTERS_TODAY_FILE = "data/cleaned/batters_today.csv"
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
TEAMDIR_FILE = "data/manual/team_directory.csv"
OUTPUT_FILE = "data/adjusted/batters_away.csv"

def to_int64(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def build_alias_to_id(teamdir_path: Path):
    td = pd.read_csv(teamdir_path, dtype=str).fillna("")
    need = {"team_id","team_code","canonical_team","team_name","clean_team_name","all_codes","all_names"}
    missing = need - set(td.columns)
    if missing:
        raise ValueError(f"{teamdir_path} missing: {', '.join(sorted(missing))}")
    alias_to_id = {}
    def put(alias, tid):
        k = (alias or "").strip().upper()
        if k and tid and k not in alias_to_id:
            alias_to_id[k] = tid
    for _, r in td.iterrows():
        tid = str(r["team_id"])
        for col in ("team_code","canonical_team","team_name","clean_team_name"):
            put(r.get(col,""), tid)
        for name in (r.get("all_names","") or "").split("|"):
            put(name, tid)
        for code in (r.get("all_codes","") or "").split("|"):
            put(code, tid)
    return alias_to_id

def ensure_team_id(batters: pd.DataFrame) -> pd.Series:
    if "team_id" in batters.columns:
        return to_int64(batters["team_id"])
    # fallback: derive from team_directory via alias map using 'team' column
    alias_to_id = build_alias_to_id(Path(TEAMDIR_FILE))
    derived = batters["team"].astype(str).map(lambda v: alias_to_id.get((v or "").strip().upper()))
    return to_int64(derived)

def main(batters_path: str, games_path: str, out_path: str):
    print(f"--- splitaway: {out_path} ---")
    bat = pd.read_csv(batters_path, dtype=str).fillna("")
    if "team" not in bat.columns:
        print(f"INSUFFICIENT INFORMATION\nMissing 'team' in {batters_path}")
        return
    bat["team_id"] = ensure_team_id(bat)

    games = pd.read_csv(games_path, dtype=str).fillna("")
    if "away_team_id" not in games.columns:
        print(f"INSUFFICIENT INFORMATION\nMissing 'away_team_id' in {games_path}")
        return
    games["away_team_id"] = to_int64(games["away_team_id"])

    away_ids = set(games["away_team_id"].dropna().tolist())
    out = bat[bat["team_id"].isin(away_ids)].copy()
    out = out.drop(columns=["team_id"], errors="ignore")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"✅ Wrote {out_path} rows={len(out)}")

if __name__ == "__main__":
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        main(BATTERS_TODAY_FILE, GAMES_FILE, OUTPUT_FILE)
