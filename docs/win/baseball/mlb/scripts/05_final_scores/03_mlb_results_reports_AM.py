#!/usr/bin/env python3
# Morning wrapper for the shared MLB reports implementation.

from results_am_common import configure_morning_reports, load_sibling

reports = load_sibling("03_mlb_results_reports.py", "mlb_results_reports_shared")
configure_morning_reports(reports)

if __name__ == "__main__":
    reports.run("MLB morning reports")
