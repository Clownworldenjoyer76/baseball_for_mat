#!/usr/bin/env python3
# Mobile-safe: align with current team_directory.csv schema; no obsolete header requirement.

from pathlib import Path
import pandas as pd
import sys

TEAMDIR = Path("data/manual/team_directory.csv")

def _die(msg):
    print(f"INSUFFICIENT INFORMATION\n{msg}", file=sys.stderr)
    sys.exit(1)

def main():
    if not TEAMDIR.exists():
        _die(f"Missing file: {TEAMDIR}")
    td = pd.read_csv(TEAMDIR, dtype=str).fillna("")
    required = {"team_id","team_code","canonical_team","team_name","clean_team_name","all_codes","all_names"}
    if not required.issubset(td.columns):
        _die(f"{TEAMDIR} must include columns: {', '.join(sorted(required))}")
    # No further action needed here; other steps now derive IDs correctly.
    print("✅ fetch_mlb_ids: team_directory.csv schema validated.")

if __name__ == "__main__":
    main()
