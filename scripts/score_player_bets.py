#!/usr/bin/env python3
import argparse, csv, sys, time
from pathlib import Path
import requests
import pandas as pd

TEAM_MAP_FILE = Path("data/Data/team_name_master.csv")  # adjust path if different

def parse_args():
    p = argparse.ArgumentParser(
        description="Score daily PLAYER props: computes prop_correct only, based on MLB StatsAPI boxscores for the given date."
    )
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--api", default="https://statsapi.mlb.com/api/v1")
    p.add_argument("--out", required=True, help="Per-day player props CSV to update")
    return p.parse_args()

def _get(url, params=None, tries=3, sleep=0.8):
    for _ in range(tries):
        r = requests.get(url, params=params, timeout=20)
        if r.ok:
            return r.json()
        time.sleep(sleep)
    r.raise_for_status()

def norm_name_to_last_first(full_name: str) -> str:
    s = str(full_name or "").strip()
    if not s:
        return ""
    parts = s.split()
    if len(parts) == 1:
        return parts[0]
    last = parts[-1]
    first = " ".join(parts[:-1])
    return f"{last}, {first}"

def normalize_team_names(df: pd.DataFrame, team_col: str) -> pd.DataFrame:
    if TEAM_MAP_FILE.exists():
        try:
            tm = pd.read_csv(TEAM_MAP_FILE)
            tm["team_code"] = tm["team_code"].astype(str).str.strip().str.lower()
            tm["team_name_api"] = tm["team_name_api"].astype(str).str.strip()
            tm["team_name_short"] = tm["team_name_short"].astype(str).str.strip().str.lower()
            mapping = dict(zip(tm["team_name_short"], tm["team_name_api"]))
            df[team_col] = df[team_col].astype(str).str.strip().str.lower().replace(mapping)
        except Exception as e:
            print(f"⚠️ Could not load/parse team map: {e}", file=sys.stderr)
    return df

def collect_boxscore_stats_for_date(api_base: str, date: str) -> pd.DataFrame:
    sched = _get(f"{api_base}/schedule", {"sportId":1, "date":date})
    game_pks = [g.get("gamePk") for d in sched.get("dates", []) for g in d.get("games", [])]
    rows = []
    for pk in game_pks:
        try:
            box = _get(f"{api_base}/game/{pk}/boxscore")
        except Exception:
            continue
        teams = []
        if "teams" in box:
            teams = [("home", box["teams"].get("home")), ("away", box["teams"].get("away"))]
        for _, tjs in teams:
            if not tjs: continue
            team_name = tjs.get("team", {}).get("name", "")
            for group in ("batters", "pitchers"):
                for pid in tjs.get(group, []):
                    pnode = tjs.get("players", {}).get(f"ID{pid}", {})
                    info = pnode.get("person", {}) or {}
                    nm = norm_name_to_last_first(info.get("fullName", ""))
                    stats = pnode.get("stats", {})
                    bat = stats.get("batting", {}) or {}
                    pit = stats.get("pitching", {}) or {}

                    hits = bat.get("hits")
                    hr = bat.get("homeRuns")
                    singles = (bat.get("hits") or 0) - (bat.get("doubles") or 0) - (bat.get("triples") or 0) - (bat.get("homeRuns") or 0)
                    tb = (singles or 0) + 2*(bat.get("doubles") or 0) + 3*(bat.get("triples") or 0) + 4*(bat.get("homeRuns") or 0)

                    k_bat = bat.get("strikeOuts")
                    k_pit = pit.get("strikeOuts")
                    bb_bat = bat.get("baseOnBalls")
                    bb_pit = pit.get("baseOnBalls")

                    rows.append({
                        "team": team_name,
                        "player_name": nm,
                        "hits": hits if hits is not None else 0,
                        "home_runs": hr if hr is not None else 0,
                        "total_bases": tb if tb is not None else 0,
                        "strikeouts": max(k for k in [k_bat, k_pit] if k is not None) if (k_bat is not None or k_pit is not None) else 0,
                        "walks": max(b for b in [bb_bat, bb_pit] if b is not None) if (bb_bat is not None or bb_pit is not None) else 0,
                    })
    if not rows:
        return pd.DataFrame(columns=["team","player_name","hits","home_runs","total_bases","strikeouts","walks"])
    df = pd.DataFrame(rows)
    df = df.groupby(["team","player_name"], as_index=False).max(numeric_only=True)
    return df

def map_prop_to_metric(ptype: str) -> str:
    p = str(ptype or "").strip().lower()
    return {
        "hits": "hits",
        "home_runs": "home_runs",
        "total_bases": "total_bases",
        "strikeouts": "strikeouts",
        "pitcher_strikeouts": "strikeouts",
        "walks": "walks",
        "walks_allowed": "walks",
    }.get(p, "")

def main():
    args = parse_args()
    out_path = Path(args.out)
    if not out_path.exists():
        print(f"❌ Per-day player props not found: {out_path}", file=sys.stderr)
        sys.exit(1)

    picks = pd.read_csv(out_path)
    for col in ["date","team","player_name","prop_type"]:
        if col not in picks.columns:
            picks[col] = ""

    picks["player_name"] = picks["player_name"].fillna("").apply(lambda s: s.strip())

    # Normalize team names if mapping exists
    picks = normalize_team_names(picks, "team")

    if "prop_line" in picks.columns:
        line_series = pd.to_numeric(picks["prop_line"], errors="coerce")
    else:
        line_series = pd.to_numeric(picks.get("line"), errors="coerce")

    actual = collect_boxscore_stats_for_date(args.api, args.date)
    actual = normalize_team_names(actual, "team")

    # 1st pass: name-only match
    merged = picks.merge(actual.drop(columns=["team"]).drop_duplicates("player_name"),
                         on="player_name", how="left", suffixes=("", "_actual"))

    # 2nd pass: for still-missing, try team+name
    need = merged["hits"].isna() & merged["home_runs"].isna() & merged["total_bases"].isna()
    if need.any():
        fix = picks[need].merge(actual, on=["team","player_name"], how="left")
        merged.loc[need, ["hits","home_runs","total_bases","strikeouts","walks"]] = fix[["hits","home_runs","total_bases","strikeouts","walks"]].values

    metric_for_row = [map_prop_to_metric(pt) for pt in picks["prop_type"]]
    actual_vals = []
    for i, met in enumerate(metric_for_row):
        val = merged.at[i, met] if met else None
        try:
            actual_vals.append(float(val) if pd.notna(val) else None)
        except Exception:
            actual_vals.append(None)

    def decide(av, ln):
        if av is None or pd.isna(ln):
            return ""
        try:
            return "Yes" if float(av) >= float(ln) else "No"
        except Exception:
            return ""

    picks["prop_correct"] = [decide(av, ln) for av, ln in zip(actual_vals, line_series)]

    picks.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Player props scored: {out_path}")

if __name__ == "__main__":
    main()
