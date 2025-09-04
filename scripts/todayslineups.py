#!/usr/bin/env python3
import csv
import os
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

import requests

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"
ROSTER_URL_TMPL = "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"

OUTPUT_PATH = Path("data/raw/lineups.csv")
TEAM_DIR_PATH = Path("data/manual/team_directory.csv")          # needs cols: team_code, team_id
BATTERS_PATH = Path("data/Data/batters.csv")                    # needs cols: player_id, last_name, first_name (header is "last_name, first_name")

# Final CSV headers — keep EXACT
HEADERS = ["team_code", "team_id", "player_id", "last_name, first_name"]

def _strip(s: str) -> str:
    return (s or "").strip()

def _norm(s: str) -> str:
    """Normalize for matching: lowercase, strip spaces/punct/diacritics."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # remove common punctuation and whitespace
    for ch in [",", ".", "'", "-", "’", " "]:
        s = s.replace(ch, "")
    return s

def to_last_first(full_name: str) -> str:
    """
    Convert 'First Last' (possibly with middle names) → 'Last, First'
    Heuristic: last token is last name; everything before is first/middle.
    If name already contains a comma, return as-is.
    """
    full_name = _strip(full_name)
    if "," in full_name:
        return full_name  # already 'Last, First'
    parts = full_name.split()
    if len(parts) >= 2:
        last = parts[-1]
        first = " ".join(parts[:-1])
        return f"{last}, {first}"
    return full_name  # fallback

def load_team_directory() -> Dict[str, str]:
    """
    Returns {team_code -> team_id} from data/manual/team_directory.csv
    """
    mapping: Dict[str, str] = {}
    if TEAM_DIR_PATH.exists():
        with TEAM_DIR_PATH.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = _strip(row.get("team_code", ""))
                tid = _strip(str(row.get("team_id", "")))
                if code:
                    mapping[code] = tid
    return mapping

def load_batters_map() -> Dict[str, str]:
    """
    Build a robust name→player_id map from data/Data/batters.csv.
    Primary key: normalized 'Last, First'.
    Also stores an alias using 'First Last' derived from 'Last, First' to improve matching.
    """
    name_to_pid: Dict[str, str] = {}
    if BATTERS_PATH.exists():
        with BATTERS_PATH.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Expect a column literally named "last_name, first_name"
            name_col = "last_name, first_name"
            for row in reader:
                pid = _strip(str(row.get("player_id", "")))
                lf = _strip(row.get(name_col, ""))
                if not lf or not pid:
                    continue
                # store normalized 'Last, First'
                name_to_pid[_norm(lf)] = pid
                # also store 'First Last' alias
                if "," in lf:
                    last, first = [p.strip() for p in lf.split(",", 1)]
                    fl = f"{first} {last}"
                    name_to_pid[_norm(fl)] = pid
    return name_to_pid

def get_team_map_api() -> Dict[int, str]:
    """{team_id:int -> team_abbreviation:str} for active MLB teams from the API."""
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

def main():
    # Ensure output directory exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Lookups
    api_team_map = get_team_map_api()                 # {api_team_id:int -> team_code}
    team_dir_map = load_team_directory()              # {team_code:str -> team_id:str}
    batters_map = load_batters_map()                  # {normalized_name -> player_id}

    rows = []
    for api_team_id, team_code in api_team_map.items():
        roster = get_active_roster(api_team_id)
        for entry in roster:
            person = entry.get("person", {}) or {}
            full_name = _strip(person.get("fullName") or "")
            if not full_name:
                continue

            # Convert to EXACT header format value: "Last, First"
            last_first = to_last_first(full_name)

            # Match team_id from manual directory by team_code
            team_id = team_dir_map.get(team_code, "")

            # Match player_id by normalized name
            pid = batters_map.get(_norm(last_first), "")
            if not pid:
                # try also matching the raw 'First Last' form just in case
                pid = batters_map.get(_norm(full_name), "")

            rows.append({
                "team_code": team_code,
                "team_id": team_id,                      # may be "" if unmatched
                "player_id": pid,                        # may be "" if unmatched
                "last_name, first_name": last_first,     # value in "Last, First" form
            })

    # Sort for consistency: by team_code, then name
    rows.sort(key=lambda r: (r["team_code"], r["last_name, first_name"]))

    # Write CSV with EXACT headers
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
