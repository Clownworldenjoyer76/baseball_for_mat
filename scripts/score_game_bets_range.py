#!/usr/bin/env python3
"""
Score locked daily *team* bet files in data/bets/bet_history:

  data/bets/bet_history/YYYY-MM-DD_game_props.csv

It fills:
- actual_real_run_total = home_score + away_score
- favorite_correct = TRUE/FALSE when scores exist
- run_total_diff = actual_real_run_total - projected_real_run_total

You supply finals via:
  A) --api <URL returning JSON [{date,home_team,away_team,home_score,away_score}, ...]>
  B) --results <CSV with columns date,home_team,away_team,home_score,away_score>

## Modes (pick ONE)
--auto        (default) Update:
               • yesterday (America/New_York), AND
               • any files missing actuals (catch-up)
--date YYYY-MM-DD       Update that single day
--range YYYY-MM-DD:YYYY-MM-DD
                         Update all days inclusive
--since YYYY-MM-DD       Update all days from this date forward

Examples:
  python scripts/score_game_bets_range.py --api https://example.com/finals
  python scripts/score_game_bets_range.py --date 2025-08-08 --results data/results/2025-08-08.csv
  python scripts/score_game_bets_range.py --range 2025-08-06:2025-08-08 --api $SCORES_API
  python scripts/score_game_bets_range.py --since 2025-08-01 --api $SCORES_API
"""

import argparse
import re
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
import pandas as pd

try:
    # Python 3.9+: use zoneinfo (no external deps)
    from zoneinfo import ZoneInfo
    TZ_NY = ZoneInfo("America/New_York")
except Exception:
    TZ_NY = None  # fallback to UTC below if needed

BET_DIR = Path("data/bets/bet_history")
FNAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_game_props\.csv$")

# ---------- IO helpers ----------
def _read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df

def _load_results_from_api(url: str) -> pd.DataFrame:
    import requests
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    df = pd.DataFrame(data)
    # normalize
    need = ["date","home_team","away_team","home_score","away_score"]
    df = df.rename(columns={k:k for k in need})  # no-op; clarity
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise SystemExit(f"API JSON missing required fields: {missing}")
    return df[need]

def _load_results_from_csv(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    lower = {c.lower(): c for c in df.columns}
    need = {
        "date": ["date","game_date"],
        "home_team": ["home_team","hometeam","home"],
        "away_team": ["away_team","awayteam","away","visitor"],
        "home_score": ["home_score","home_runs","homescore","homefinal"],
        "away_score": ["away_score","away_runs","awayscore","awayfinal"],
    }
    col = {}
    for want, cands in need.items():
        for c in cands:
            if c in lower:
                col[want] = lower[c]; break
        if want not in col:
            raise SystemExit(f"Results CSV missing column for {want}. Tried: {cands}")
    out = df[[col["date"], col["home_team"], col["away_team"], col["home_score"], col["away_score"]]].copy()
    out.columns = ["date","home_team","away_team","home_score","away_score"]
    return out

# ---------- scoring for one file ----------
def score_file(game_csv: Path, results_df: pd.DataFrame) -> int:
    """Return number of rows updated."""
    bets = _read_csv(game_csv)

    # Build keys (and swapped) for robust joins
    def keyify(df, home_col, away_col):
        return (
            df["date"].astype(str).str.strip().str.lower() + "|" +
            df[home_col].astype(str).str.strip().str.lower() + "|" +
            df[away_col].astype(str).str.strip().str.lower()
        )

    # Results: normal and swapped
    res = results_df.copy()
    res["_key_norm"] = keyify(res, "home_team", "away_team")
    res["_key_swap"] = keyify(res, "away_team", "home_team")

    bets["_key"] = keyify(bets, "home_team", "away_team")

    norm = res[["_key_norm","home_score","away_score"]].rename(columns={"_key_norm":"_key"})
    swap = res[["_key_swap","away_score","home_score"]].rename(columns={"_key_swap":"_key","away_score":"home_score","home_score":"away_score"})
    joined = bets.merge(pd.concat([norm, swap], ignore_index=True), on="_key", how="left")

    # Ensure target cols exist
    for c in ["actual_real_run_total","favorite_correct","run_total_diff"]:
        if c not in joined.columns:
            joined[c] = pd.NA

    # Compute fields
    hs = pd.to_numeric(joined["home_score"], errors="coerce")
    as_ = pd.to_numeric(joined["away_score"], errors="coerce")
    actual_total = (hs + as_).round(2)

    proj = pd.to_numeric(joined.get("projected_real_run_total"), errors="coerce")
    run_total_diff = (actual_total - proj).where(actual_total.notna()).round(2)

    fav = joined.get("favorite", pd.Series([None]*len(joined)))
    fav = fav.astype(str).str.strip().str.lower()
    home = joined["home_team"].astype(str).str.strip().str.lower()
    away = joined["away_team"].astype(str).str.strip().str.lower()

    winner = pd.Series(pd.NA, index=joined.index, dtype="object")
    winner = winner.mask(hs.gt(as_), joined["home_team"])
    winner = winner.mask(as_.gt(hs), joined["away_team"])
    favorite_correct = (fav == winner.astype(str).str.lower()).where(hs.notna() & as_.notna())
    favorite_correct = favorite_correct.map({True:"TRUE", False:"FALSE"})

    # Write values
    before = joined["actual_real_run_total"].notna().sum()
    joined["actual_real_run_total"] = actual_total
    joined["favorite_correct"] = favorite_correct
    joined["run_total_diff"] = run_total_diff
    after = joined["actual_real_run_total"].notna().sum()

    # Overwrite same file (with a quick .bak)
    bak = game_csv.with_suffix(".bak.csv")
    joined.to_csv(bak, index=False)
    joined.to_csv(game_csv, index=False)

    return int(after - before)

# ---------- date selection ----------
def list_game_files() -> dict[date, Path]:
    out = {}
    if not BET_DIR.exists():
        return out
    for p in BET_DIR.iterdir():
        m = FNAME_RE.match(p.name)
        if m:
            try:
                d = date.fromisoformat(m.group(1))
                out[d] = p
            except Exception:
                continue
    return dict(sorted(out.items()))

def yesterday_ny() -> date:
    if TZ_NY:
        now = datetime.now(TZ_NY)
    else:
        now = datetime.now(timezone.utc)
    return (now - timedelta(days=1)).date()

def needs_scoring(path: Path) -> bool:
    try:
        df = _read_csv(path)
        if "actual_real_run_total" not in df.columns:
            return True
        # Any missing actuals → needs scoring
        return df["actual_real_run_total"].isna().any()
    except Exception:
        return True

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--auto", action="store_true", help="(default) Score yesterday + any unscored files")
    g.add_argument("--date", help="Score a single day YYYY-MM-DD")
    g.add_argument("--range", help="Score an inclusive range YYYY-MM-DD:YYYY-MM-DD")
    g.add_argument("--since", help="Score from this date YYYY-MM-DD to the latest available")
    ap.add_argument("--api", help="Finals API URL returning JSON")
    ap.add_argument("--results", help="Finals CSV path")
    args = ap.parse_args()

    # finals source
    if not args.api and not args.results:
        raise SystemExit("Provide --api URL or --results CSV")

    results_df = _load_results_from_api(args.api) if args.api else _load_results_from_csv(Path(args.results))

    files = list_game_files()
    if not files:
        raise SystemExit(f"No *_game_props.csv files found in {BET_DIR}")

    targets: list[Path] = []

    if args.date:
        d = date.fromisoformat(args.date)
        if d in files: targets = [files[d]]
        else: raise SystemExit(f"No file for {d} under {BET_DIR}")
    elif args.range:
        try:
            start_s, end_s = args.range.split(":")
            start_d = date.fromisoformat(start_s)
            end_d = date.fromisoformat(end_s)
        except Exception:
            raise SystemExit("Invalid --range. Use YYYY-MM-DD:YYYY-MM-DD")
        targets = [p for d,p in files.items() if start_d <= d <= end_d]
        if not targets: raise SystemExit(f"No files in range {start_d}..{end_d}")
    elif args.since:
        start_d = date.fromisoformat(args.since)
        targets = [p for d,p in files.items() if d >= start_d]
        if not targets: raise SystemExit(f"No files on/after {start_d}")
    else:
        # --auto (default): yesterday + any unscored
        y = yesterday_ny()
        if y in files:
            targets.append(files[y])
        # add any unscored (catch-up)
        for d, p in files.items():
            if needs_scoring(p) and p not in targets:
                targets.append(p)

    if not targets:
        print("Nothing to score. All files are up to date.")
        return

    total_updated = 0
    for csv_path in targets:
        updated = score_file(csv_path, results_df)
        print(f"✔ {csv_path.name}: updated rows = {updated}")
        total_updated += updated

    print(f"✅ Done. Total rows updated across files: {total_updated}")

if __name__ == "__main__":
    main()
