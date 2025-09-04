#!/usr/bin/env python3
import csv
import unicodedata
from pathlib import Path
from typing import Dict, List

import requests

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"
ROSTER_URL_TMPL = "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"

OUTPUT_PATH = Path("data/raw/lineups.csv")
TEAM_DIR_PATH = Path("data/manual/team_directory.csv")   # columns: team_code, team_id
BATTERS_PATH = Path("data/Data/batters.csv")             # columns: last_name, first_name | player_id

# Final CSV headers — keep EXACT
HEADERS = ["team_code", "team_id", "player_id", "last_name, first_name"]

def _strip(s: str) -> str:
    return (s or "").strip()

def _norm(s: str) -> str:
    """Normalize for robust matching: lowercase, strip spaces/punct/diacritics."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    for ch in [",", ".", "'", "-", "’", " "]:
        s = s.replace(ch, "")
    return s

def to_last_first(full_name: str) -> str:
    """Convert 'First Last ...' → 'Last, First ...'. If already has comma, return as-is."""
    full_name = _strip(full_name)
    if "," in full_name:
        return full_name
    parts = full_name.split()
    if len(parts) >= 2:
        last = parts[-1]
        first = " ".join(parts[:-1])
        return f"{last}, {first}"
    return full_name

def get_team_map_api() -> Dict[int, str]:
    """{api_team_id:int -> team_code:str} for active MLB teams from the API."""
    params = {"sportId": 1, "activeStatus": "Y"}
    resp = requests.get(TEAMS_URL, params=params, timeout=20)
    resp.raise_for_status()
    teams = resp.json().get("teams", [])
    return {t["id"]: _strip(t.get("abbreviation", "")) for t in teams}

def get_active_roster(team_id: int) -> List[dict]:
    """Fetch the 26-man ACTIVE roster for team_id."""
    params = {"rosterType": "active"}
    url = ROSTER_URL_TMPL.format(team_id=team_id)
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json().get("roster", [])

def load_team_directory() -> Dict[str, str]:
    """Return {team_code -> team_id} from data/manual/team_directory.csv (exact headers)."""
    mapping: Dict[str, str] = {}
    if TEAM_DIR_PATH.exists():
        with TEAM_DIR_PATH.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = _strip(row.get("team_code", ""))
                tid = _strip(str(row.get("team_id", "")))
                if code:
                    mapping[code] = tid
    return mapping

def load_batters_map() -> Dict[str, str]:
    """
    Build map {normalized 'last_name, first_name' -> player_id} from data/Data/batters.csv.
    Uses the exact headers: 'last_name, first_name' and 'player_id'.
    """
    mapping: Dict[str, str] = {}
    if BATTERS_PATH.exists():
        with BATTERS_PATH.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = _strip(row.get("last_name, first_name", ""))
                pid = _strip(str(row.get("player_id", "")))
                if name and pid:
                    mapping[_norm(name)] = pid
    return mapping

def main():
    # Ensure output directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    api_team_map = get_team_map_api()   # {api_team_id:int -> team_code:str}
    team_dir_map = load_team_directory()  # {team_code:str -> team_id:str}
    batter_pid_map = load_batters_map()   # {normalized 'Last, First' -> player_id}

    rows = []
    for api_team_id, team_code in api_team_map.items():
        roster = get_active_roster(api_team_id)
        for entry in roster:
            person = entry.get("person", {}) or {}
            full_name = _strip(person.get("fullName") or "")
            if not full_name:
                continue

            # Convert API 'First Last' → exact value 'Last, First'
            last_first = to_last_first(full_name)

            # Lookups
            team_id = team_dir_map.get(team_code, "")
            player_id = batter_pid_map.get(_norm(last_first), "")

            rows.append({
                "team_code": team_code,
                "team_id": team_id,                      # blank if unmatched
                "player_id": player_id,                  # blank if unmatched
                "last_name, first_name": last_first,
            })

    # Sort for consistency
    rows.sort(key=lambda r: (r["team_code"], r["last_name, first_name"]))

    # Write CSV with EXACT headers
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
