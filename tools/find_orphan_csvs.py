#!/usr/bin/env python3
"""
find_orphan_csvs.py

Scan all CSVs under data/ and report only those not referenced in
scripts or workflows. Output = summaries/audit/orphan_csv_report.csv
with columns: path, referenced (always "no").
"""

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

def gather_csvs():
    if not DATA_ROOT.exists():
        return []
    return sorted([p for p in DATA_ROOT.rglob("*.csv") if p.is_file()])

def build_reference_index():
    """Return a dict {csv_path: True/False} whether referenced."""
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
    print(f"Total orphan candidates: {len(rows)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
