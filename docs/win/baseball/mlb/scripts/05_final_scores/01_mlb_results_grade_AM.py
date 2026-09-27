#!/usr/bin/env python3
# Morning wrapper for the shared MLB grading implementation.

from results_am_common import configure_morning_grade, load_sibling

grade = load_sibling("01_mlb_results_grade.py", "mlb_results_grade_shared")
configure_morning_grade(grade)

if __name__ == "__main__":
    grade.main("01_mlb_results_grade_AM.py")
