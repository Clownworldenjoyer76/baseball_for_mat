#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build batters_today.csv from normalized lineups with correct player_id, type, and team_id.

Inputs:
  - data/raw/lineups_normalized.csv         (from normalize_lineups.py; columns include team_code, "last_name, first_name")
  - data/Data/batters.csv                   (columns: "last_name, first_name", player_id)
  - data/Data/pitchers.csv                  (columns: "last_name, first_name", player_id)
  - data/manual/team_directory.csv          (columns: team_id, team_code, [optional] all_codes)

Output:
  - data/cleaned/batters_today.csv          (columns: team_code, "last_name, first_name", type, player_id, team_id)

Rules:
  - Match player_id by exact "last_name, first_name" string:
      1) batters.csv → type="batter"
      2) pitchers.csv → type="pitcher" (only where still missing)
  - Resolve team_id by team_code primary join; if missing, use all_codes fallback (pipe/comma/space separated).
  - Render team_id as an integer string (no decimals), blanks for missing.
  - Drop any accidental *_x/*_y columns before writing.
  - Deduplicate on player_id when present (keep first); otherwise deduplicate on name/team.
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
    # Normalize essential columns
    cols = {c.lower().strip(): c for c in df.columns}
    # canonicalize expected names
    rename = {}
    if "team_id" not in df.columns and "teamid" in cols:
        rename[cols["teamid"]] = "team_id"
    if "team_code" not in df.columns and "teamcode" in cols:
        rename[cols["teamcode"]] = "team_code"
    if rename:
        df = df.rename(columns=rename)

    # ensure required fields exist
    missing = [c for c in ["team_id", "team_code"] if c not in df.columns]
    if missing:
        raise ValueError(f"team_directory.csv is missing columns: {missing}")

    # Build long form mapping using all_codes fallback if present
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
                # Split by pipe, comma, or whitespace
                parts = [p.strip() for chunk in raw.replace("|", ",").replace("/", ",").split(",") for p in chunk.split()]
                for p in parts:
                    if p and p != team_code:
                        long_rows.append({"team_code": p, "team_id": team_id})

    if not long_rows:
        # fall back to just team_code → team_id mapping
        return df[["team_code", "team_id"]].dropna().drop_duplicates().copy()

    long_df = pd.DataFrame(long_rows).drop_duplicates()
    # Coerce team_id to Int64 for safety
    long_df["team_id"] = pd.to_numeric(long_df["team_id"], errors="coerce").astype("Int64")
    return long_df

def enforce_output_types(df: pd.DataFrame) -> pd.DataFrame:
    # team_id to Int64 -> string without <NA>
    if "team_id" in df.columns:
        df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64").astype("string")
        df["team_id"] = df["team_id"].replace({"<NA>": ""})
    # player_id render as string, blank if missing
    if "player_id" in df.columns:
        # Try to keep numeric-looking IDs as ints, but write as plain strings
        pid = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        df["player_id"] = pid.astype("string").replace({"<NA>": ""})
    # type -> string, blanks allowed
    if "type" in df.columns:
        df["type"] = df["type"].fillna("").astype(str)
    return df

def main():
    # Load inputs
    lu = load_csv(LINEUPS)
    bat = load_csv(BATTERS)
    pit = load_csv(PITCHERS)
    td_raw = load_csv(TEAM_DIR)

    # Basic column checks
    for c in REQ_LINEUP_COLS:
        if c not in lu.columns:
            raise ValueError(f"{LINEUPS} missing column: {c}")
    for c in ["last_name, first_name", "player_id"]:
        if c not in bat.columns:
            raise ValueError(f"{BATTERS} missing column: {c}")
        if c not in pit.columns:
            raise ValueError(f"{PITCHERS} missing column: {c}")

    # Keep just the necessary lineup columns; do not carry prior type/ids
    lu = lu.copy()
    # make sure exact schema base exists
    base_cols = ["team_code", "last_name, first_name"]
    lu = lu[base_cols].dropna(subset=["last_name, first_name"]).drop_duplicates()

    # --- Match player_id (batters first) ---
    b = bat[["last_name, first_name", "player_id"]].dropna(subset=["last_name, first_name"]).copy()
    p = pit[["last_name, first_name", "player_id"]].dropna(subset=["last_name, first_name"]).copy()

    # Left-merge on batters for player_id
    out = lu.merge(
        b.rename(columns={"player_id": "player_id_b"}),
        on="last_name, first_name",
        how="left",
        validate="m:1",
    )
    out["type"] = out["player_id_b"].apply(lambda x: "batter" if pd.notna(x) and str(x) != "" else "")

    # Fill missing from pitchers
    missing_mask = out["player_id_b"].isna() | (out["player_id_b"].astype(str) == "")
    if missing_mask.any():
        out = out.merge(
            p.rename(columns={"player_id": "player_id_p"}),
            on="last_name, first_name",
            how="left",
            validate="m:1",
        )
        # choose player_id: prefer batter match, else pitcher
        out["player_id"] = out["player_id_b"].where(out["player_id_b"].notna() & (out["player_id_b"].astype(str) != ""), out["player_id_p"])
        # set type if pitcher matched and still blank
        out.loc[missing_mask & out["player_id_p"].notna() & (out["player_id_p"].astype(str) != ""), "type"] = "pitcher"
    else:
        out["player_id"] = out["player_id_b"]

    # Clean up helper columns
    for c in ["player_id_b", "player_id_p"]:
        if c in out.columns:
            out = out.drop(columns=c)

    # --- Resolve team_id from team_directory (with all_codes fallback) ---
    td = prep_team_directory(td_raw)
    out = out.merge(td, on="team_code", how="left", validate="m:1")

    # --- Final column ordering ---
    out = out[["team_code", "last_name, first_name", "type", "player_id", "team_id"]]

    # Deduplicate: if we have player_id, keep first occurrence; else dedupe by (team_code, name)
    if out["player_id"].notna().any() and (out["player_id"].astype(str) != "").any():
        # Keep the first instance per player_id (stable)
        out = out.sort_values(["player_id", "team_code", "last_name, first_name"], na_position="last")
        out = out.drop_duplicates(subset=["player_id"], keep="first")
    else:
        out = out.drop_duplicates(subset=["team_code", "last_name, first_name"], keep="first")

    # Types / rendering
    out = enforce_output_types(out)

    # Safety: drop any accidental *_x/_y residues
    drop_extras = [c for c in out.columns if c.endswith("_x") or c.endswith("_y")]
    if drop_extras:
        out = out.drop(columns=drop_extras)

    # Ensure output dir exists and write
    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTFILE, index=False)

    print(f"✅ build_batters_today_from_lineups: wrote {len(out)} rows -> {OUTFILE}")

if __name__ == "__main__":
    main()
