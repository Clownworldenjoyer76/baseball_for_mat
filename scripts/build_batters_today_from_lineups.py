#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import glob
import os

LINEUPS_NORM = "data/raw/lineups_normalized.csv"
BATTERS_M   = "data/Data/batters.csv"
PITCHERS_M  = "data/Data/pitchers.csv"
TEAM_DIR    = "data/manual/team_directory.csv"
OUTFILE     = "data/cleaned/batters_today.csv"

def load_master(path):
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    key = "last_name, first_name"
    if key not in df.columns:
        raise SystemExit(f"Missing column '{key}' in {path}")
    df[key] = df[key].str.strip()
    df = df.drop_duplicates(subset=[key])  # ensure unique on name
    return df

def main():
    if not os.path.exists(LINEUPS_NORM):
        raise FileNotFoundError(f"{LINEUPS_NORM} not found")

    lineups = pd.read_csv(LINEUPS_NORM, dtype=str, keep_default_na=False)
    lineups.columns = [c.strip() for c in lineups.columns]
    if "team_code" not in lineups.columns or "last_name, first_name" not in lineups.columns:
        raise SystemExit("lineups_normalized.csv must have 'team_code' and 'last_name, first_name'")

    lineups["team_code"] = lineups["team_code"].str.strip()
    lineups["last_name, first_name"] = lineups["last_name, first_name"].str.strip()

    batters_m  = load_master(BATTERS_M)
    pitchers_m = load_master(PITCHERS_M)

    teams = pd.read_csv(TEAM_DIR, dtype=str, keep_default_na=False)
    for col in ("team_code","team_id","all_codes"):
        if col not in teams.columns:
            raise SystemExit(f"Missing column '{col}' in {TEAM_DIR}")
    teams["team_code"] = teams["team_code"].str.strip()
    teams["team_id"]   = pd.to_numeric(teams["team_id"], errors="coerce").astype("Int64")
    teams = teams.drop_duplicates(subset=["team_code"])  # ensure unique codes

    # Join team_id
    lu = lineups.merge(
        teams[["team_code","team_id"]],
        on="team_code",
        how="left",
        validate="m:1"
    )

    # Join player_id from batters
    lu = lu.merge(
        batters_m[["last_name, first_name","player_id"]].rename(columns={"player_id":"player_id_b"}),
        on="last_name, first_name",
        how="left",
        validate="m:1"
    )
    # Join player_id from pitchers
    lu = lu.merge(
        pitchers_m[["last_name, first_name","player_id"]].rename(columns={"player_id":"player_id_p"}),
        on="last_name, first_name",
        how="left",
        validate="m:1"
    )

    lu["player_id"] = lu["player_id_b"].where(lu["player_id_b"].notna() & (lu["player_id_b"]!=""), lu["player_id_p"])
    lu.drop(columns=["player_id_b","player_id_p"], inplace=True)

    lu["type"] = ""
    lu.loc[lu["player_id"].notna() & (lu["player_id"]!=""), "type"] = "batter"
    mask_pitch = (~lu["last_name, first_name"].isin(batters_m["last_name, first_name"])) & \
                 (lu["last_name, first_name"].isin(pitchers_m["last_name, first_name"])) & \
                 (lu["player_id"].notna()) & (lu["player_id"]!="")
    lu.loc[mask_pitch, "type"] = "pitcher"

    # Fallback search in team_csvs if still missing
    if lu["player_id"].isna().any() or (lu["player_id"]=="").any():
        bat_csvs = sorted(glob.glob("data/team_csvs/batters_*.csv"))
        pit_csvs = sorted(glob.glob("data/team_csvs/pitchers_*.csv"))

        def load_pairs(paths):
            pairs = {}
            for p in paths:
                try:
                    tmp = pd.read_csv(p, dtype=str, keep_default_na=False)
                    if {"last_name, first_name","player_id"}.issubset(tmp.columns):
                        tmp = tmp.drop_duplicates(subset=["last_name, first_name"])
                        for _, r in tmp.iterrows():
                            key = r["last_name, first_name"].strip()
                            val = r["player_id"].strip()
                            if key and val and key not in pairs:
                                pairs[key] = val
                except Exception:
                    pass
            return pairs

        bat_pairs = load_pairs(bat_csvs)
        pit_pairs = load_pairs(pit_csvs)

        def fallback_id(row):
            if row.get("player_id"):
                return row["player_id"]
            nm = row["last_name, first_name"]
            return bat_pairs.get(nm) or pit_pairs.get(nm) or ""

        lu["player_id"] = lu.apply(fallback_id, axis=1)

        def fallback_type(row):
            if row["type"]:
                return row["type"]
            nm = row["last_name, first_name"]
            if nm in bat_pairs: return "batter"
            if nm in pit_pairs: return "pitcher"
            return ""

        lu["type"] = lu.apply(fallback_type, axis=1)

    for col in ("type","player_id","team_id"):
        if col not in lu.columns:
            lu[col] = ""

    first_cols = [c for c in ["team_code","last_name, first_name","type","player_id","team_id"] if c in lu.columns]
    rest_cols  = [c for c in lu.columns if c not in first_cols]
    out = lu[first_cols + rest_cols]

    Path(OUTFILE).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTFILE, index=False)
    print(f"✅ build_batters_today_from_lineups: wrote {len(out)} rows -> {OUTFILE}")

if __name__ == "__main__":
    main()
