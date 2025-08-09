#!/usr/bin/env python3
"""
Grade a locked daily *player* props file:
  data/bets/bet_history/YYYY-MM-DD_player_props.csv

Fills:
  - actual_value  (numeric; derived from stats)
  - result        ('WIN','LOSS','PUSH') based on Over line
  - graded        TRUE/FALSE

Inputs for stats:
  A) --api <URL> returns JSON list of player game stats:
     items with at least: date, player_name (Last, First or First Last), team, and stat fields
  B) --results <CSV> with same fields

Usage:
  python scripts/score_player_bets.py --date 2025-08-08 --api https://example.com/boxscores
  python scripts/score_player_bets.py --date 2025-08-08 --results data/results/player_box_2025-08-08.csv

Notes:
- Name matching: tries player_id first (if your props file has it), else normalizes names.
- Supported batter props: hits, home_runs, total_bases, runs, rbi, walks, stolen_bases, strikeouts (batter).
- Unsupported props are left ungraded (graded=FALSE) so you can extend later.
"""

import argparse
from pathlib import Path
import math
import pandas as pd
def _normalize_api_base(api: str, endpoint: str) -> str:
    """If 'api' is just the MLB base (.../api/v1), append the endpoint."""
    if not api:
        return api
    a = api.rstrip('/')
    if a.endswith('/api/v1'):
        return f"{a}/{endpoint.lstrip('/')}"
    return api


BET_DIR = Path("data/bets/bet_history")

# ---------- helpers ----------
def _read_csv(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    return df

def _norm_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    t = " ".join(s.strip().split())
    # if "Last, First", turn into last,first key; else first last -> last,first
    if "," in t:
        last, rest = t.split(",", 1)
        first = rest.strip().split()[0] if rest.strip() else ""
        key = f"{last.strip().lower()}, {first.lower()}"
    else:
        parts = t.split()
        if len(parts) >= 2:
            first, last = parts[0], parts[-1]
            key = f"{last.lower()}, {first.lower()}"
        else:
            key = t.lower()
    return key

def _load_stats_from_api(url: str) -> pd.DataFrame:
    import requests
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    df = pd.DataFrame(data)
    df.columns = [c.strip() for c in df.columns]
    return df

def _load_stats_from_csv(path: Path) -> pd.DataFrame:
    return _read_csv(path)

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return math.nan

# Map prop_type to functions that compute actuals from a stat row
def _calc_actual(stat_row: dict, prop_type: str) -> float:
    """
    Expect stat_row keys to include typical baseball box score abbreviations.
    We try several aliases per stat. Unknown -> NaN.
    """
    aliases = {
        "hits": ["H", "hits"],
        "home_runs": ["HR", "home_runs", "hr"],
        "total_bases": ["TB", "total_bases", "tb"],
        "runs": ["R", "runs"],
        "rbi": ["RBI", "rbi"],
        "walks": ["BB", "walks", "bb"],
        "stolen_bases": ["SB", "stolen_bases", "sb"],
        # batter strikeouts (swinging + looking)
        "strikeouts": ["SO", "K", "strikeouts", "so", "k"],
    }
    key = (prop_type or "").strip().lower()
    # minor normalization (e.g., "home runs", "home_runs" -> "home_runs")
    key = key.replace(" ", "_")
    if key not in aliases:
        return math.nan
    for cand in aliases[key]:
        for col in stat_row.keys():
            if col.lower() == cand.lower():
                return _to_float(stat_row[col])
    return math.nan

def _grade_over(actual: float, line: float) -> str:
    if math.isnan(actual) or math.isnan(line):
        return ""
    if actual > line: return "WIN"
    if actual < line: return "LOSS"
    return "PUSH"

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (matches the _player_props.csv filename)")
    ap.add_argument("--api", help="URL to JSON stats")
    ap.add_argument("--results", help="Path to CSV stats")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()

    if bool(args.api) == bool(args.results):
        raise SystemExit("Provide exactly one of --api OR --results")

    locked_path = BET_DIR / f"{args.date}_player_props.csv"
    if not locked_path.exists():
        raise SystemExit(f"Locked file not found: {locked_path}")

    props = _read_csv(locked_path)

    # Light column expectations; we’ll be flexible:
    # player_name, team, prop_type, prop_line, (optional) player_id
    lc = {c.lower(): c for c in props.columns}
    for need in ["player_name","team","prop_type","prop_line"]:
        if need not in lc:
            raise SystemExit(f"Missing required column in locked file: {need}")

    col_name = lc["player_name"]
    col_team = lc["team"]
    col_type = lc["prop_type"]
    col_line = lc["prop_line"]
    col_id   = lc.get("player_id")

    # Load stats
    stats = _load_stats_from_api(args.api) if args.api else _load_stats_from_csv(Path(args.results))
    stats.columns = [c.strip() for c in stats.columns]

    # Build matching keys
    s_lc = {c.lower(): c for c in stats.columns}
    # Flexible columns: player id (preferred), name, team
    s_id = s_lc.get("player_id") or s_lc.get("mlb_id") or s_lc.get("id")
    s_name = s_lc.get("player_name") or s_lc.get("name") or s_lc.get("last_name, first_name")
    s_team = s_lc.get("team") or s_lc.get("team_name") or s_lc.get("team_code")

    # Precompute lookup dicts
    by_id = {}
    if s_id:
        for _, r in stats.iterrows():
            pid = str(r[s_id]).strip()
            if pid:
                by_id[pid] = r

    by_name_team = {}
    if s_name and s_team:
        for _, r in stats.iterrows():
            key = (_norm_name(r[s_name]), str(r[s_team]).strip().lower())
            by_name_team[key] = r

    # Prepare output columns
    if "actual_value" not in props.columns:
        props["actual_value"] = pd.NA
    if "result" not in props.columns:
        props["result"] = pd.NA
    if "graded" not in props.columns:
        props["graded"] = pd.NA

    # Grade each row
    results = []
    for _, row in props.iterrows():
        pid = str(row[col_id]).strip() if col_id else ""
        pname_key = _norm_name(row[col_name])
        team_key = str(row[col_team]).strip().lower()
        ptype = row[col_type]
        pline = _to_float(row[col_line])

        stat_row = None
        if pid and pid in by_id:
            stat_row = by_id[pid]
        elif (pname_key, team_key) in by_name_team:
            stat_row = by_name_team[(pname_key, team_key)]

        actual = _calc_actual(stat_row if stat_row is not None else {}, str(ptype))
        res = _grade_over(actual, pline) if not math.isnan(actual) else ""

        results.append((actual if not math.isnan(actual) else pd.NA,
                        res if res else pd.NA,
                        bool(res)))

    props["actual_value"] = [a for a,_,_ in results]
    props["result"] = [r for _,r,_ in results]
    props["graded"] = ["TRUE" if g else "FALSE" for *_, g in results]

    # Backup + overwrite
    if not args.no_backup:
        bak = locked_path.with_suffix(".bak.csv")
        props.to_csv(bak, index=False)
        print(f"🗂  Backup written: {bak}")

    props.to_csv(locked_path, index=False)
    graded_count = (props["graded"] == "TRUE").sum()
    print(f"✅ Graded {graded_count}/{len(props)} rows in {locked_path}")

if __name__ == "__main__":
    main()