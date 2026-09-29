#!/usr/bin/env python3
"""Pregame wrapper for build_run_projection.py.

Rebuilds only the current MLB date plus the previous 6 calendar days
(7 calendar days total) instead of rebuilding every historical prediction date.

A confirmed MLB no-game day is a successful condition. If scrape_mlb_raw.py
successfully confirmed that today's MLB schedule contains zero games, today's
prediction file is not required. Any available prior prediction dates in the
7-day window are still rebuilt. If no prediction dates exist in the window,
the wrapper exits successfully without invoking build_run_projection.py.

The underlying build_run_projection.py logic is unchanged, including its
leakage-safe training-history construction and walk-forward/production model
selection.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


BASE_DIR = Path("docs/win/baseball/mlb")
PRED_DIR = BASE_DIR / "00_intake/predictions/pred_with_game_id"
BUILD_RUN_PROJECTION = (
    BASE_DIR / "scripts/00_intake/build_run_projection.py"
)
SCRAPE_LOG_FILE = (
    BASE_DIR / "errors/00_intake/scrape_mlb_raw.txt"
)

TIMEZONE = ZoneInfo("America/New_York")
LOOKBACK_DAYS = 7


def _parse_prediction_date(path: Path):
    stem = path.stem

    if not stem.endswith("_MLB"):
        return None

    date_str = stem[:-4]

    try:
        parsed = datetime.strptime(
            date_str,
            "%Y_%m_%d",
        ).date()
    except ValueError:
        return None

    return date_str, parsed


def _scrape_log_confirms_no_games(
    target_date: date,
) -> bool:
    if not SCRAPE_LOG_FILE.exists():
        return False

    try:
        log_text = SCRAPE_LOG_FILE.read_text(
            encoding="utf-8",
        )
    except OSError:
        return False

    target_text = target_date.isoformat()

    required_markers = [
        f"Target date: {target_text}",
        "Schedule games found: 0",
        "No MLB games scheduled for target date;",
        "STATUS: SUCCESS",
    ]

    return all(
        marker in log_text
        for marker in required_markers
    )


def discover_recent_dates() -> list[str]:
    if not PRED_DIR.exists():
        raise RuntimeError(
            f"Prediction directory not found: {PRED_DIR}"
        )

    today = datetime.now(TIMEZONE).date()
    start_date = today - timedelta(
        days=LOOKBACK_DAYS - 1
    )

    dates: list[tuple[date, str]] = []

    for path in PRED_DIR.glob("*_MLB.csv"):
        parsed = _parse_prediction_date(path)

        if parsed is None:
            continue

        date_str, date_value = parsed

        if start_date <= date_value <= today:
            dates.append(
                (date_value, date_str)
            )

    dates.sort(
        key=lambda item: item[0]
    )

    selected = [
        date_str
        for _, date_str in dates
    ]

    today_str = today.strftime(
        "%Y_%m_%d"
    )

    today_prediction_exists = (
        today_str in selected
    )

    if not today_prediction_exists:
        if _scrape_log_confirms_no_games(
            today
        ):
            print(
                "No MLB games scheduled today; "
                "today's prediction file is not required."
            )
        else:
            raise RuntimeError(
                "Today's prediction file is missing and "
                "scrape_mlb_raw.py did not confirm a successful "
                "zero-game MLB schedule: "
                f"{PRED_DIR / f'{today_str}_MLB.csv'}"
            )

    if not selected:
        print(
            "No prediction files found in the pregame "
            f"{LOOKBACK_DAYS}-day window "
            f"{start_date.isoformat()} through {today.isoformat()}."
        )
        print(
            "Confirmed no-game day; "
            "nothing to rebuild."
        )
        return []

    print(
        "Pregame run-projection window: "
        f"{start_date.isoformat()} through {today.isoformat()}"
    )

    print(
        "Projection dates selected "
        f"({len(selected)}): "
        + ", ".join(selected)
    )

    return selected


def main() -> None:
    if not BUILD_RUN_PROJECTION.exists():
        raise RuntimeError(
            "build_run_projection.py not found: "
            f"{BUILD_RUN_PROJECTION}"
        )

    dates = discover_recent_dates()

    if not dates:
        return

    command = [
        sys.executable,
        str(BUILD_RUN_PROJECTION),
        *dates,
    ]

    print(
        "Running: "
        + " ".join(command)
    )

    subprocess.run(
        command,
        check=True,
    )


if __name__ == "__main__":
    main()
