#!/usr/bin/env python3
"""
Patch build_juice_files.py to apply the item #8 market-specific calibration artifact.

This script:
- creates a timestamped backup
- makes only exact, validated text replacements
- refuses to patch an unexpected file shape
- does not fit calibrators; run fit_probability_calibrators.py first
"""

from __future__ import annotations

import py_compile
import shutil
from datetime import UTC, datetime
from pathlib import Path

BASE_DIR = Path("docs/win/baseball/mlb")
TARGET = (
    BASE_DIR
    / "scripts/01_merge/build_juice_files.py"
)


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / BASE_DIR).is_dir():
                return candidate
    raise RuntimeError("Could not locate repository root")


def replace_once(
    text: str,
    old: str,
    new: str,
    label: str,
) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"{label}: expected exactly 1 match, found {count}"
        )
    return text.replace(old, new, 1)


def main() -> int:
    root = find_repo_root()
    target = root / TARGET

    if not target.exists():
        raise FileNotFoundError(target)

    text = target.read_text(encoding="utf-8")

    marker = "from probability_calibration import ("
    if marker in text:
        print(f"Already patched: {target}")
        return 0

    original = text

    text = replace_once(
        text,
        "from scipy.stats import poisson, skellam\n",
        """from scipy.stats import poisson, skellam
from probability_calibration import (
    calibrate_binary_probability,
    calibration_artifact_path,
)
""",
        "import insertion",
    )

    text = replace_once(
        text,
        """    return (
        p_home_raw / resolved,
        p_away_raw / resolved,
        p_tie,
    )
""",
        """    p_home = calibrate_binary_probability(
        "moneyline",
        p_home_raw / resolved,
    )
    p_away = 1.0 - p_home

    return (
        p_home,
        p_away,
        p_tie,
    )
""",
        "moneyline calibration",
    )

    text = replace_once(
        text,
        """    p_away = 1.0 - p_home

    if (
""",
        """    p_home = calibrate_binary_probability(
        "run_line",
        p_home,
    )
    p_away = 1.0 - p_home

    if (
""",
        "run-line calibration",
    )

    text = replace_once(
        text,
        """    return (
        p_over,
        p_under,
        p_push,
    )
""",
        """    resolved = p_over + p_under
    if (
        not np.isfinite(resolved)
        or resolved <= 0.0
        or resolved > 1.0 + PROB_TOLERANCE
    ):
        raise ValueError(
            "invalid totals resolved probability mass"
        )

    conditional_over = p_over / resolved
    conditional_over = calibrate_binary_probability(
        "total",
        conditional_over,
    )

    p_over = resolved * conditional_over
    p_under = resolved * (1.0 - conditional_over)

    return (
        p_over,
        p_under,
        p_push,
    )
""",
        "totals calibration",
    )

    text = replace_once(
        text,
        """    log(
        "RUN-LINE PROBABILITIES USE RAW SKELLAM OUTPUT: "
        "no post-hoc calibration is applied"
    )
""",
        """    log(
        "OUT-OF-SAMPLE MARKET CALIBRATION ENABLED: "
        f"artifact={calibration_artifact_path()}"
    )
""",
        "calibration log",
    )

    if text == original:
        raise RuntimeError("No changes were made")

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    backup = target.with_name(
        f"{target.name}.item8_{timestamp}.bak"
    )
    shutil.copy2(
        target,
        backup,
    )
    target.write_text(
        text,
        encoding="utf-8",
    )

    try:
        py_compile.compile(
            str(target),
            doraise=True,
        )
    except Exception:
        shutil.copy2(
            backup,
            target,
        )
        raise

    print(f"Patched: {target}")
    print(f"Backup:  {backup}")
    print("Syntax validation: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
