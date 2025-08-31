#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from pathlib import Path

INPUT  = Path("data/raw/todaysgames.csv")
OUTPUT = Path("data/raw/todaysgames_normalized.csv")
TEAM_DIR = Path("data/manual/team_directory.csv")  # exact headers required

def _tokenize_pipe(s: str) -> list[str]:
    if not isinstance(s, str) or not s:
        return []
    return [t.strip() for t in s.split("|") if t.strip()]

def _build_team_code_lookup(team_df: pd.DataFrame) -> dict[str, str]:
    """
    Build a lowercase lookup {alias -> team_code} from exact headers in
    data/manual/team_directory.csv:
      - team_id
      - team_code
      - canonical_team
      - team_name
      - clean_team_name
      - all_codes
      - all_names
    """
    lut: dict[str, str] = {}

    for _, r in team_df.iterrows():
        code = str(r["team_code"]).strip()

        # direct keys
        for key_col in ("team_code", "canonical_team", "team_name", "clean_team_name"):
            v = str(r[key_col]).strip()
            if v:
                lut[v.lower()] = code

        # multi-value keys
        for alias in _tokenize_pipe(str(r["all_codes"])):
            lut[alias.lower()] = code
        for name in _tokenize_pipe(str(r["all_names"])):
            lut[name.lower()] = code

    # hard override for Athletics special case from upstream source ("ATH")
    lut.setdefault("ath", "OAK")

    return lut

def normalize() -> None:
    # read inputs
    games = pd.read_csv(INPUT)
    teams = pd.read_csv(TEAM_DIR, dtype={
        "team_id": "Int64",
        "team_code": "string",
        "canonical_team": "string",
        "team_name": "string",
        "clean_team_name": "string",
        "all_codes": "string",
        "all_names": "string",
    })

    # build lookup using EXACT headers present in team_directory.csv
    lut = _build_team_code_lookup(teams)

    # normalize home/away to canonical team_code
    def to_code(x: str) -> str:
        if not isinstance(x, str):
            return x
        key = x.strip().lower()
        return lut.get(key, x)

    games["home_team"] = games["home_team"].astype(str).map(to_code)
    games["away_team"] = games["away_team"].astype(str).map(to_code)

    # write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    games.to_csv(OUTPUT, index=False)
    print(f"✅ normalize_todays_games wrote {len(games)} rows -> {OUTPUT}")

if __name__ == "__main__":
    normalize()
