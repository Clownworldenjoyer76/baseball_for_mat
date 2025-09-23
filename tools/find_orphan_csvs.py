#!/usr/bin/env python3
"""
find_orphan_csvs.py

Scan all CSVs under data/ and report only those NOT referenced in
scripts or workflows. Output = summaries/audit/orphan_csv_report.csv

CSV columns: path, referenced
Only rows written: referenced == "no"

Excludes:
  - data/rosters/**
  - data/team_csvs/**
"""

import csv
import sys
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

EXCLUDE_DIRS = [
    DATA_ROOT / "rosters",
    DATA_ROOT / "team_csvs",
]

def is_excluded(p: Path) -> bool:
    """Return True if p is under any excluded directory."""
    # Path.is_relative_to is available in Python 3.9+
    for ex in EXCLUDE_DIRS:
        try:
            if p.is_relative_to(ex):
                return True
        except AttributeError:
            # Fallback for older Python: string prefix check
            if ex.as_posix() in p.as_posix():
                return True
    return False

def gather_csvs():
    if not DATA_ROOT.exists():
        return []
    all_csvs = [p for p in DATA_ROOT.rglob("*.csv") if p.is_file()]
    return [p for p in all_csvs if not is_excluded(p)]

def build_reference_index():
    """Return a dict {csv_path: True/False} whether referenced somewhere."""
    source_files = []
    for pat in SCAN_GLOBS:
        source_files.extend(REPO_ROOT.glob(pat))
    source_files = [p for p in source_files if p.is_file()]

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
        referenced = any(base in content or rel in content for content in texts.values())
        refs[csv_path] = referenced
    return refs

def main():
    refs = build_reference_index()
    SUM_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = SUM_DIR / "orphan_csv_report.csv"

    rows = []
    for p, is_ref in refs.items():
        if not is_ref:
            rel = p.relative_to(REPO_ROOT).as_posix()
            rows.append({"path": rel, "referenced": "no"})

    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["path", "referenced"])
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote orphan report: {out_csv}")
    print(f"Total orphan candidates (excluding {', '.join(d.relative_to(REPO_ROOT).as_posix() for d in EXCLUDE_DIRS)}): {len(rows)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
