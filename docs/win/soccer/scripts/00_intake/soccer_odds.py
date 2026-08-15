#!/usr/bin/env python3
"""Soccer odds intake with non-destructive same-day CSV persistence.

The ESPN retrieval/parsing implementation lives in _soccer_odds_core.py. This
wrapper preserves the public script path while ensuring later runs never replace
an already-populated CSV value with a blank and never drop previously captured
games that disappear from a later same-day ESPN scoreboard response.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import _soccer_odds_core as core
from _soccer_odds_core import *  # noqa: F401,F403 - preserve existing imports/API


def _is_blank(value: Any) -> bool:
    return value is None or not str(value).strip()


def _read_existing(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            return [
                {field: str(row.get(field) or "") for field in core.CSV_FIELDS}
                for row in reader
            ]
    except (OSError, csv.Error) as exc:
        print(f"warning: could not read existing CSV {path}: {exc}", file=sys.stderr)
        return []


def _merge_rows(
    path: Path, rows: Sequence[Mapping[str, str]]
) -> list[dict[str, str]]:
    """Merge fresh rows into a prior daily CSV without erasing captured data."""
    existing_rows = _read_existing(path)
    existing_by_id = {
        row["game_id"]: row
        for row in existing_rows
        if not _is_blank(row.get("game_id"))
    }

    merged_rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for fresh in rows:
        merged = {field: str(fresh.get(field) or "") for field in core.CSV_FIELDS}
        game_id = merged["game_id"].strip()
        prior = existing_by_id.get(game_id) if game_id else None

        if prior is not None:
            for field in core.CSV_FIELDS:
                if _is_blank(merged[field]) and not _is_blank(prior.get(field)):
                    merged[field] = prior[field]
            seen_ids.add(game_id)

        merged_rows.append(merged)

    # Keep previously captured games if ESPN no longer returns them later that day.
    for prior in existing_rows:
        game_id = prior.get("game_id", "").strip()
        if game_id and game_id in seen_ids:
            continue
        merged_rows.append(prior)

    merged_rows.sort(
        key=lambda row: (
            row.get("match_time", ""),
            row.get("home_team", ""),
            row.get("away_team", ""),
        )
    )
    return merged_rows


def write_csv(path: Path, rows: Sequence[Mapping[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    merged_rows = _merge_rows(path, rows)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=core.CSV_FIELDS, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(merged_rows)


# core.main() resolves write_csv from the core module, so replace that function
# before any CLI execution while keeping every other retrieval/parser unchanged.
core.write_csv = write_csv
main = core.main


if __name__ == "__main__":
    raise SystemExit(main())
