#!/usr/bin/env python3
# Mobile-safe: reconcile stadium file team identifiers to team_directory.csv
import sys
from pathlib import Path
import pandas as pd

TEAM_DIR = Path("data/manual/team_directory.csv")
STADIUM_CSV = Path("data/Data/stadium_metadata.csv")

def _load_team_directory() -> pd.DataFrame:
    if not TEAM_DIR.exists():
        print(f"ERROR: missing {TEAM_DIR}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(TEAM_DIR, dtype=str).fillna("")
    required = {"team_id","team_code","canonical_team","team_name","clean_team_name","all_codes","all_names"}
    if not required.issubset(set(df.columns)):
        print("ERROR: team_directory.csv must contain exact headers: "
              "team_id, team_code, canonical_team, team_name, clean_team_name, all_codes, all_names", file=sys.stderr)
        sys.exit(1)
    df["team_id"] = df["team_id"].astype(str).str.strip()
    df["team_code"] = df["team_code"].astype(str).str.strip()
    df["canonical_team"] = df["canonical_team"].astype(str).str.strip()
    df["team_name"] = df["team_name"].astype(str).str.strip()
    df["clean_team_name"] = df["clean_team_name"].astype(str).str.strip()
    df["all_codes"] = df["all_codes"].astype(str).str.strip()
    df["all_names"] = df["all_names"].astype(str).str.strip()
    return df

def _build_alias_map(df: pd.DataFrame) -> dict:
    m = {}
    def add(k, code):
        k = (k or "").strip().upper()
        code = (code or "").strip()
        if k and code and k not in m:
            m[k] = code
    for _, r in df.iterrows():
        code = r["team_code"]
        add(r["team_code"], code)
        add(r["canonical_team"], code)
        add(r["team_name"], code)
        add(r["clean_team_name"], code)
        for bucket in ("all_codes","all_names"):
            raw = r[bucket]
            if raw:
                parts = [p for token in raw.split("|") for p in token.split(",")]
                for p in parts:
                    add(p, code)
    return m

def _normalize(series: pd.Series, amap: dict) -> pd.Series:
    return series.astype(str).apply(lambda v: amap.get(v.strip().upper(), v.strip()))

def main():
    if not STADIUM_CSV.exists():
        print(f"INSUFFICIENT INFORMATION\nMissing file: {STADIUM_CSV}", file=sys.stderr)
        sys.exit(1)

    td = _load_team_directory()
    amap = _build_alias_map(td)

    s = pd.read_csv(STADIUM_CSV)
    # Choose an existing team identifier column
    team_col = None
    for c in ("team","home_team","Team","HOME_TEAM"):
        if c in s.columns:
            team_col = c
            break
    if team_col is None:
        print(f"INSUFFICIENT INFORMATION\nNo team column in {STADIUM_CSV}", file=sys.stderr)
        sys.exit(1)

    # Normalize to canonical team_code and attach team_id
    s["team"] = _normalize(s[team_col], amap)
    if "team_id" not in s.columns:
        s = s.merge(td[["team_code","team_id"]].rename(columns={"team_code":"team"}),
                    on="team", how="left")

    # Drop the old column if different
    if team_col != "team" and team_col in s.columns:
        s.drop(columns=[team_col], inplace=True)

    # Write back
    STADIUM_CSV.parent.mkdir(parents=True, exist_ok=True)
    s.to_csv(STADIUM_CSV, index=False)
    print(f"Saved updated stadium metadata to {STADIUM_CSV}")

if __name__ == "__main__":
    main()
