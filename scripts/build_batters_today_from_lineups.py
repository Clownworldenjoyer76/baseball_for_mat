#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build batters_today.csv from normalized lineups with correct player_id, type, and team_id.

Inputs:
  - data/raw/lineups_normalized.csv         (team_code, "last_name, first_name")
  - data/Data/batters.csv                   ("last_name, first_name", player_id)
  - data/Data/pitchers.csv                  ("last_name, first_name", player_id)
  - data/manual/team_directory.csv          (team_id, team_code, optional all_codes)

Output:
  - data/cleaned/batters_today.csv          (team_code, "last_name, first_name", type, player_id, team_id)
"""

from pathlib import Path
import pandas as pd

LINEUPS = Path("data/raw/lineups_normalized.csv")
BATTERS = Path("data/Data/batters.csv")
PITCHERS = Path("data/Data/pitchers.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")
OUTFILE = Path("data/cleaned/batters_today.csv")

REQ_LINEUP_COLS = ["team_code", "last_name, first_name"]

def load_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path, **kwargs)

def prep_team_directory(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names if needed
    rename = {}
    lower = {c.lower(): c for c in df.columns}
    if "team_id" not in df.columns and "teamid" in lower:
        rename[lower["teamid"]] = "team_id"
    if "team_code" not in df.columns and "teamcode" in lower:
        rename[lower["teamcode"]] = "team_code"
    if rename:
        df = df.rename(columns=rename)

    for col in ("team_id", "team_code"):
        if col not in df.columns:
            raise ValueError(f"team_directory.csv missing column: {col}")

    # Long-form mapping including all_codes fallbacks
    long_rows = []
    for _, r in df.iterrows():
        team_id = r.get("team_id")
        team_code = (r.get("team_code") or "").strip()
        if pd.isna(team_id) or team_code == "":
            continue
        long_rows.append({"team_code": team_code, "team_id": team_id})
        if "all_codes" in df.columns:
            raw = r.get("all_codes")
            if isinstance(raw, str) and raw.strip():
                # split on pipe/comma/slash/whitespace
                raw = raw.replace("|", ",").replace("/", ",")
                parts = []
                for chunk in raw.split(","):
                    parts.extend(chunk.strip().split())
                for p in parts:
                    if p and p != team_code:
                        long_rows.append({"team_code": p, "team_id": team_id})

    if long_rows:
        m = pd.DataFrame(long_rows).drop_duplicates()
    else:
        m = df[["team_code", "team_id"]].dropna().drop_duplicates().copy()

    m["team_id"] = pd.to_numeric(m["team_id"], errors="coerce").astype("Int64")
    return m

def enforce_output_types(df: pd.DataFrame) -> pd.DataFrame:
    if "team_id" in df.columns:
        df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64").astype("string")
        df["team_id"] = df["team_id"].replace({"<NA>": ""})
    if "player_id" in df.columns:
        pid = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        df["player_id"] = pid.astype("string").replace({"<NA>": ""})
    if "type" in df.columns:
        df["type"] = df["type"].fillna("").astype(str)
    return df

def main():
    # Load
    lu = load_csv(LINEUPS)
    bat = load_csv(BATTERS)
    pit = load_csv(PITCHERS)
    td_raw = load_csv(TEAM_DIR)

    # Sanity
    for c in REQ_LINEUP_COLS:
        if c not in lu.columns:
            raise ValueError(f"{LINEUPS} missing column: {c}")
    for c in ["last_name, first_name", "player_id"]:
        if c not in bat.columns:
            raise ValueError(f"{BATTERS} missing column: {c}")
        if c not in pit.columns:
            raise ValueError(f"{PITCHERS} missing column: {c}")

    # Canonical lineup base
    lu = lu[["team_code", "last_name, first_name"]].dropna(subset=["last_name, first_name"])
    lu = lu.drop_duplicates()

    # Deduplicate master files by name so merges are many-to-one
    b = bat[["last_name, first_name", "player_id"]].dropna(subset=["last_name, first_name"]).copy()
    b = b[b["player_id"].notna()]  # drop rows without IDs
    b = b.sort_values(["last_name, first_name"]).drop_duplicates(subset=["last_name, first_name"], keep="first")

    p = pit[["last_name, first_name", "player_id"]].dropna(subset=["last_name, first_name"]).copy()
    p = p[p["player_id"].notna()]
    p = p.sort_values(["last_name, first_name"]).drop_duplicates(subset=["last_name, first_name"], keep="first")

    # Merge (batters first)
    out = lu.merge(
        b.rename(columns={"player_id": "player_id_b"}),
        on="last_name, first_name",
        how="left",
        validate="m:1",
    )
    out["type"] = out["player_id_b"].apply(lambda x: "batter" if pd.notna(x) and str(x) != "" else "")

    # Fill missing from pitchers (still many-to-one thanks to de-dupe)
    missing_mask = out["player_id_b"].isna() | (out["player_id_b"].astype(str) == "")
    out = out.merge(
        p.rename(columns={"player_id": "player_id_p"}),
        on="last_name, first_name",
        how="left",
        validate="m:1",
    )
    out["player_id"] = out["player_id_b"].where(out["player_id_b"].notna() & (out["player_id_b"].astype(str) != ""), out["player_id_p"])
    out.loc[missing_mask & out["player_id_p"].notna() & (out["player_id_p"].astype(str) != ""), "type"] = "pitcher"

    out = out.drop(columns=["player_id_b", "player_id_p"])

    # Team IDs (primary team_code → team_id, then all_codes)
    td = prep_team_directory(td_raw)
    out = out.merge(td, on="team_code", how="left", validate="m:1")

    # Order and de-dupe
    out = out[["team_code", "last_name, first_name", "type", "player_id", "team_id"]]
    if out["player_id"].notna().any() and (out["player_id"].astype(str) != "").any():
        out = out.sort_values(["player_id", "team_code", "last_name, first_name"], na_position="last")
        out = out.drop_duplicates(subset=["player_id"], keep="first")
    else:
        out = out.drop_duplicates(subset=["team_code", "last_name, first_name"], keep="first")

    # Render types
    out = enforce_output_types(out)

    # Safety: remove any *_x/_y if ever present
    drop_extras = [c for c in out.columns if c.endswith("_x") or c.endswith("_y")]
    if drop_extras:
        out = out.drop(columns=drop_extras)

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTFILE, index=False)

    print(f"✅ build_batters_today_from_lineups: wrote {len(out)} rows -> {OUTFILE}")

if __name__ == "__main__":
    main()
