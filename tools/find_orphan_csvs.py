#!/usr/bin/env python3
"""
find_orphan_csvs.py

Report CSV files under data/ that are not referenced anywhere in:
  - scripts/*.py
  - .github/workflows/*.yml
  - README*/docs/*.md (optional)
Heuristic: if a CSV's basename or relative path appears as a substring in any scanned file,
it's considered "referenced". Everything else is flagged as "orphan_candidate".

Optional flags:
  --days N      Mark files not modified in the last N days as "stale".
  --verbose     Print details while scanning.

Outputs:
  summaries/audit/orphan_csv_report.csv with columns:
    path, size_bytes, mtime_iso, referenced, referenced_where_count, stale_days, note
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]  # repo/
DATA_ROOT = REPO_ROOT / "data"
SUM_DIR = REPO_ROOT / "summaries" / "audit"

SCAN_GLOBS = [
    "scripts/**/*.py",
    ".github/workflows/**/*.yml",
    "README.*",
    "docs/**/*.md",
]

# Directories that are almost always ephemeral outputs; we STILL scan for references,
# but we annotate to help you decide faster.
LIKELY_OUTPUT_DIRS = {
    "data/_projections",
    "data/end_chain",
    "data/adjusted",
    "data/raw",
    "data/cleaned",
    "data/bets",
    "data/temp_inputs",
}

def human_dt(ts: float) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(ts))
    except Exception:
        return ""

def gather_csvs():
    if not DATA_ROOT.exists():
        return []
    return sorted([p for p in DATA_ROOT.rglob("*.csv") if p.is_file()])

def build_reference_index(verbose: bool = False):
    """Return a dict {filepath: set(of filepaths where it was referenced)} using
    basename and relative path substring search."""
    # Load all candidate source texts once
    source_files = []
    for pat in SCAN_GLOBS:
        source_files.extend(REPO_ROOT.glob(pat))
    source_files = [p for p in source_files if p.is_file()]

    if verbose:
        print(f"Scanning {len(source_files)} source files for references...")

    texts = {}
    for f in source_files:
        try:
            texts[f] = f.read_text(errors="ignore")
        except Exception:
            texts[f] = ""

    refs = {}
    for csv_path in gather_csvs():
        rel = csv_path.relative_to(REPO_ROOT).as_posix()
        base = csv_path.name
        hits = set()
        for src, content in texts.items():
            if base in content or rel in content:
                hits.add(src)
        refs[csv_path] = hits
        if verbose:
            print(f"[ref] {rel} -> {len(hits)} hits")
    return refs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=None,
                    help="Mark files older than N days as stale_days=N+")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    if args.verbose:
        print(f"Repo root: {REPO_ROOT}")
        print(f"Data root: {DATA_ROOT}")

    csvs = gather_csvs()
    if not csvs:
        print("No CSVs found under data/")
        return 0

    refs = build_reference_index(verbose=args.verbose)

    SUM_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = SUM_DIR / "orphan_csv_report.csv"

    now = time.time()
    rows = []
    for p in csvs:
        rel = p.relative_to(REPO_ROOT).as_posix()
        st = p.stat()
        size = st.st_size
        mtime = st.st_mtime
        days_old = int((now - mtime) / 86400)
        stale_mark = ""
        if args.days is not None and days_old >= args.days:
            stale_mark = f">={args.days}"

        hits = refs.get(p, set())
        referenced = bool(hits)
        where_count = len(hits)

        note_parts = []
        # annotate if in a likely-output dir
        for d in LIKELY_OUTPUT_DIRS:
            if rel.startswith(d + "/") or rel == d:
                note_parts.append("likely_output_dir")
                break
        if not referenced:
            note_parts.append("orphan_candidate")

        rows.append({
            "path": rel,
            "size_bytes": size,
            "mtime_iso": human_dt(mtime),
            "referenced": "yes" if referenced else "no",
            "referenced_where_count": where_count,
            "stale_days": days_old,
            "stale_mark": stale_mark,
            "note": ",".join(note_parts),
        })

    # Sort: orphans first, then oldest first
    rows.sort(key=lambda r: (r["referenced"] == "yes", -r["stale_days"], r["path"]))

    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "path",
                "size_bytes",
                "mtime_iso",
                "referenced",
                "referenced_where_count",
                "stale_days",
                "stale_mark",
                "note",
            ],
        )
        w.writeheader()
        w.writerows(rows)

    # Quick console summary
    orphan_count = sum(1 for r in rows if r["referenced"] == "no")
    total = len(rows)
    print(f"Wrote: {out_csv}")
    print(f"CSV files scanned: {total}")
    print(f"Orphan candidates: {orphan_count} (top of the file)")

    print("\nNext steps:")
    print("1) Review orphan candidates at the top of the CSV.")
    print("2) Move suspected junk to a quarantine folder first (e.g., data/_trash/).")
    print("3) If everything looks good, delete them in a follow-up commit.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
