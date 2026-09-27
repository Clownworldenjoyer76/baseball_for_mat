#!/usr/bin/env python3
# Morning wrapper for the shared MLB analysis implementation.

from results_am_common import configure_morning_analyze, load_sibling

analyze = load_sibling("02_mlb_results_analyze.py", "mlb_results_analyze_shared")
configure_morning_analyze(analyze)

if __name__ == "__main__":
    analyze.run("MLB morning analyze")
