#!/usr/bin/env python3
import csv
import os
from pathlib import Path
from typing import Dict, List

import requests

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"
ROSTER_URL_TMPL = "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"

OUTPUT_PATH = Path("data/raw/lineups.csv")
HEADERS = ["team_code", "last_name, first_name"]  # keep EXACT

def get_team_map() -> Dict[int, str]:
    """
    Return {team_id: team_abbreviation} for all active MLB clubs.
    """
    params = {
        "sportId": 1,            # MLB
        "activeStatus": "Y",     # active franchises
    }
    resp = requests.get(TEAMS_URL, params=params, timeout=20)
    resp.raise_for_status()
    teams = resp.json().get("teams", [])
    return {t["id"]: t.get("abbreviation", "").strip() for t in teams}

def get_active_roster(team_id: int) -> List[dict]:
    """
    Fetch the 26-man ACTIVE roster for a team (excludes IL/Minors).
    """
    params = {"rosterType": "active"}
    url = ROSTER_URL_TMPL.format(team_id=team_id)
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json().get("roster", [])

def main():
    # Ensure output directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    team_map = get_team_map()

    rows = []
    for team_id, team_code in team_map.items():
        roster = get_active_roster(team_id)
        for entry in roster:
            person = entry.get("person", {})
            full_name = (person.get("fullName") or "").strip()
            if not full_name:
                continue  # skip if missing

            # You asked for values in "First Last" regardless of header text.
            name_value = full_name

            rows.append({
                "team_code": team_code,
                "last_name, first_name": name_value,  # value is "First Last"
            })

    # Sort for consistency: by team_code then name
    rows.sort(key=lambda r: (r["team_code"], r["last_name, first_name"]))

    # Write CSV with EXACT headers
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
