#!/usr/bin/env python3
import csv
import re
import unicodedata
from pathlib import Path
from typing import Dict, List

import requests

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams"
ROSTER_URL_TMPL = "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"

OUT = Path("data/raw/lineups.csv")
HEADERS = ["team_code", "last_name, first_name", "type", "player_id", "team_id"]  # type/player_id/team_id left blank

# ---------- name helpers ----------
def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFD", text or "")
    return "".join(c for c in text if unicodedata.category(c) != "Mn")

_SUFFIXES = {"Jr", "Sr", "II", "III", "IV", "Jr.", "Sr."}

def normalize_last_first(full_name: str) -> str:
    """
    Convert 'First Middle Last Suffix' -> 'Last Suffix, First Middle'
    Examples:
      'Adrian Del Castillo' -> 'Del Castillo, Adrian'
      'Nacho Alvarez Jr.'   -> 'Alvarez Jr., Nacho'
      'Michael Harris II'   -> 'Harris II, Michael'
    """
    if not full_name:
        return ""
    name = strip_accents(full_name)
    name = re.sub(r"[^A-Za-z .,'-]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    tokens = [t for t in name.replace(",", "").split(" ") if t]
    if len(tokens) < 2:
        return name

    # pull suffix if present at the end
    suffix = ""
    if tokens[-1].replace(".", "") in _SUFFIXES:
        suffix = tokens[-1]
        tokens = tokens[:-1]

    # try to detect multi-word last name patterns (e.g., "Del Castillo", "De La Cruz", "Van Wagenen")
    # common lowercase particles that can be part of last names
    particles = {"da", "de", "del", "della", "di", "du", "la", "le", "van", "von"}
    last_parts = [tokens[-1]]
    i = len(tokens) - 2
    while i >= 0 and tokens[i].lower() in particles:
        last_parts.insert(0, tokens[i])
        i -= 1

    first_parts = tokens[: i + 1]
    last = " ".join(last_parts + ([suffix] if suffix else []))
    first = " ".join(first_parts).strip()
    return f"{last}, {first}".strip(", ")

# ---------- data fetch ----------
def get_team_map() -> Dict[int, str]:
    params = {"sportId": 1, "activeStatus": "Y"}
    r = requests.get(TEAMS_URL, params=params, timeout=20)
    r.raise_for_status()
    teams = r.json().get("teams", [])
    return {int(t["id"]): (t.get("abbreviation") or "").strip() for t in teams}

def get_active_roster(team_id: int) -> List[dict]:
    url = ROSTER_URL_TMPL.format(team_id=team_id)
    r = requests.get(url, params={"rosterType": "active"}, timeout=20)
    r.raise_for_status()
    return r.json().get("roster", [])

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    team_map = get_team_map()

    rows = []
    for tid, tcode in team_map.items():
        roster = get_active_roster(tid)

        # sanity: if the API ever returned an empty/foreign roster, we still tag with the loop's team code
        for entry in roster:
            person = entry.get("person", {}) or {}
            full_name = (person.get("fullName") or "").strip()
            if not full_name:
                continue
            norm = normalize_last_first(full_name)
            rows.append({
                "team_code": tcode,                 # authoritative from teams endpoint
                "last_name, first_name": norm,      # normalized “Last, First”
                "type": "",                         # left blank (to be filled by lineups_fix.py)
                "player_id": "",                    # left blank
                "team_id": "",                      # left blank
            })

    # Sort deterministically
    rows.sort(key=lambda r: (r["team_code"], r["last_name, first_name"]))

    # Write CSV
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        w.writerows(rows)

    # quick self-check (no hard fail)
    missing = [r for r in rows if not r["team_code"] or not r["last_name, first_name"]]
    print(f"✅ todayslineups: wrote {len(rows)} rows to {OUT}")
    if missing:
        print(f"⚠️ {len(missing)} rows had missing team_code or name (kept for review).")

if __name__ == "__main__":
    main()
