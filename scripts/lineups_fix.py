#!/usr/bin/env python3
import glob
import sys
from pathlib import Path

import pandas as pd

LINEUPS = Path("data/raw/lineups.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")
BATTERS = Path("data/Data/batters.csv")
PITCHERS = Path("data/Data/pitchers.csv")

def load_name_id_map(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(columns=["last_name, first_name", "player_id"])
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    # standardize the key column name
    if "last_name, first_name" not in df.columns:
        # try common alternatives
        for cand in ("name", "Name", "player_name", "last_first"):
            if cand in df.columns:
                df = df.rename(columns={cand: "last_name, first_name"})
                break
    # keep only required
    cols = [c for c in ["last_name, first_name", "player_id"] if c in df.columns]
    return df[cols].dropna().drop_duplicates()

def build_team_code_to_id(team_dir: Path) -> pd.DataFrame:
    """
    team_directory.csv needs columns: team_id, team_code, all_codes
      - all_codes: pipe or comma-separated aliases (e.g., "NYY|Yankees|NYY-2024")
    """
    tdf = pd.read_csv(team_dir, dtype=str, keep_default_na=False)
    # explode all_codes into rows
    if "all_codes" in tdf.columns:
        alias = (
            tdf.assign(all_codes=tdf["all_codes"].str.replace(",", "|"))
               .assign(all_codes=tdf["all_codes"].str.split(r"\|"))
               .explode("all_codes")
        )
        alias["alias"] = alias["all_codes"].str.strip()
        code_map = pd.concat([
            tdf[["team_code", "team_id"]].rename(columns={"team_code": "alias"}),
            alias[["alias", "team_id"]],
        ], ignore_index=True).drop_duplicates()
    else:
        # no alias list; just use team_code
        code_map = tdf[["team_code", "team_id"]].rename(columns={"team_code": "alias"}).drop_duplicates()
    code_map["alias"] = code_map["alias"].str.strip()
    return code_map

def main():
    if not LINEUPS.exists():
        print(f"❌ {LINEUPS} not found")
        sys.exit(1)

    df = pd.read_csv(LINEUPS, dtype=str, keep_default_na=False)

    # Ensure required columns exist
    for col in ["team_code", "last_name, first_name", "type", "player_id", "team_id"]:
        if col not in df.columns:
            df[col] = ""

    # ---- Build name→id maps (priority order) ----
    src_maps = []
    bat_master = load_name_id_map(BATTERS)
    if not bat_master.empty:
        bat_master["src_type"] = "batter"
        src_maps.append(bat_master)

    pit_master = load_name_id_map(PITCHERS)
    if not pit_master.empty:
        pit_master["src_type"] = "pitcher"
        src_maps.append(pit_master)

    # team_csv fallbacks
    for path in sorted(glob.glob("data/team_csvs/batters_*.csv")):
        d = load_name_id_map(Path(path))
        if not d.empty:
            d["src_type"] = "batter"
            src_maps.append(d)

    for path in sorted(glob.glob("data/team_csvs/pitchers_*.csv")):
        d = load_name_id_map(Path(path))
        if not d.empty:
            d["src_type"] = "pitcher"
            src_maps.append(d)

    if src_maps:
        name_map = pd.concat(src_maps, ignore_index=True).drop_duplicates(subset=["last_name, first_name", "player_id"])
    else:
        name_map = pd.DataFrame(columns=["last_name, first_name", "player_id", "src_type"])

    # Merge to fill player_id/type
    df = df.merge(name_map, on="last_name, first_name", how="left")
    # prefer existing if already present; else take merged
    df["player_id"] = df["player_id"].where(df["player_id"].astype(bool), df["player_id_y"])
    # fill type from src_type only when empty
    df["type"] = df["type"].where(df["type"].astype(bool), df["src_type"].fillna(""))

    # clean up columns from merge
    drop_cols = [c for c in ["player_id_y", "src_type"] if c in df.columns]
    df = df.drop(columns=drop_cols, errors="ignore")
    df = df.rename(columns={"player_id_x": "player_id"})

    # ---- Map team_code → team_id ----
    code_map = build_team_code_to_id(TEAM_DIR)
    code_map = code_map.rename(columns={"alias": "team_code"})
    df = df.merge(code_map, on="team_code", how="left", suffixes=("", "_mapped"))
    df["team_id"] = df["team_id"].where(df["team_id"].astype(bool), df["team_id_mapped"])
    df = df.drop(columns=[c for c in ["team_id_mapped"] if c in df.columns], errors="ignore")

    # ---- Validators (warnings only) ----
    missing_ids = df[~df["player_id"].astype(bool)]
    if not missing_ids.empty:
        print(f"⚠️ {len(missing_ids)} rows still missing player_id (will remain blank). Examples:")
        print(missing_ids.head(10)[["team_code", "last_name, first_name"]].to_string(index=False))

    # player_id bound to multiple team_codes in this file
    multi_team = (
        df[df["player_id"].astype(bool)]
        .groupby("player_id")["team_code"]
        .nunique()
        .reset_index(name="n_codes")
    )
    conflicts = multi_team[multi_team["n_codes"] > 1]
    if not conflicts.empty:
        print(f"⚠️ {len(conflicts)} player_id values appear with multiple team_code entries in lineups.csv.")

    # team_code should exist as an alias for the mapped team_id (sanity)
    if "team_id" in df.columns and not code_map.empty:
        valid = code_map.groupby("team_id")["team_code"].apply(set).to_dict()
        bad = []
        for _, r in df.iterrows():
            tid = r.get("team_id", "").strip()
            tcode = r.get("team_code", "").strip()
            if tid and tcode and tid in valid and tcode not in valid[tid]:
                bad.append((tid, tcode, r["last_name, first_name"]))
        if bad:
            print(f"⚠️ {len(bad)} rows have team_code not listed for their mapped team_id. First 10:")
            for (tid, tcode, name) in bad[:10]:
                print(f"  team_id={tid} team_code={tcode} name={name}")

    # ---- Write back (in place) ----
    # keep only expected columns & order
    cols = ["team_code", "last_name, first_name", "type", "player_id", "team_id"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df = df[cols].drop_duplicates()
    df.to_csv(LINEUPS, index=False, encoding="utf-8")
    print(f"✅ lineups_fix.py updated {LINEUPS} with player_id/type/team_id (rows={len(df)})")

if __name__ == "__main__":
    main()
