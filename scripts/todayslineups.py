#!/usr/bin/env python3
import csv
from pathlib import Path
from typing import Dict, List

import requests

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"
ROSTER_URL_TMPL = "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"

OUTPUT_PATH = Path("data/raw/lineups.csv")
HEADERS = ["team_code", "last_name, first_name", "type", "player_id", "team_id"]

def _strip(s: str) -> str:
    return (s or "").strip()

def to_last_first(full_name: str) -> str:
    """
    Convert 'First [Middle ...] Last [SUFFIX]' → 'Last [SUFFIX], First [Middle ...]'.
    Handles surname particles (Del, De, De la, Van, Von, Di, ...) and suffixes (Jr., II, III, ...).
    If a comma already exists, return normalized spacing as-is.
    """
    import re, unicodedata

    def _strip(s): return (s or "").strip()
    name = _strip(full_name)
    if not name:
        return name
    if "," in name:
        # Already 'Last, First' → normalize spaces only
        return " ".join(re.sub(r"\s*,\s*", ", ", name).split())

    # Tokenize
    tokens = name.split()

    # 1) Pull off suffix if present
    SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
    suffix = ""
    if tokens and tokens[-1].rstrip(".").lower() in SUFFIXES:
        suffix = tokens.pop()  # keep original casing/punct

    if not tokens:
        # name was only a suffix (weird) → just return it
        return suffix

    # 2) Build last name from the end, attaching particles
    PARTICLES = {
        "de","del","della","de la","de las","de los","da","dos","do",
        "di","du","le","la","las","los","van","von","der","st","st.","san","santa",
        "bin","binti","al","ap","ibn"
    }

    last_parts = [tokens[-1]]  # base surname
    i = len(tokens) - 2
    while i >= 0:
        low = tokens[i].lower()
        # if token is a known particle or entirely lowercase (defensive), attach to surname
        if low in PARTICLES or tokens[i].islower():
            last_parts.append(tokens[i])
            i -= 1
            continue
        break

    # Remaining tokens (0..i) are the given names (first + middles)
    given = " ".join(tokens[: i + 1])
    last = " ".join(reversed(last_parts))  # restore original order
    if suffix:
        last = f"{last} {suffix}"

    return f"{last}, {given}".strip()

def get_team_map() -> Dict[int, str]:
    """{team_id:int -> team_abbreviation:str} for active MLB teams."""
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
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    team_map = get_team_map()
    rows = []
    for api_team_id, team_code in team_map.items():
        roster = get_active_roster(api_team_id)
        for entry in roster:
            person = entry.get("person", {}) or {}
            full_name = _strip(person.get("fullName") or "")
            if not full_name:
                continue
            last_first = to_last_first(full_name)
            rows.append({
                "team_code": team_code,
                "last_name, first_name": last_first,
                "type": "",
                "player_id": "",
                "team_id": "",
            })

    rows.sort(key=lambda r: (r["team_code"], r["last_name, first_name"]))

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    main()
