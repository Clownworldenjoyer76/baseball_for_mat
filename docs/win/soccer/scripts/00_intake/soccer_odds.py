#!/usr/bin/env python3
"""Soccer odds intake with non-destructive same-day CSV persistence.

The ESPN retrieval/parsing implementation lives in _soccer_odds_core.py. This
wrapper preserves the public script path while ensuring later runs never replace
an already-populated CSV value with a blank and never drop previously captured
games that disappear from a later same-day ESPN scoreboard response.

It also adds a strict fixture-discovery safeguard: the target date plus the
adjacent dates must each return a valid ESPN scoreboard payload. A legitimate
empty ``events`` list is accepted, but HTTP/network/malformed responses are not.
This prevents a blocked ESPN request from being mistaken for an empty slate and
silently producing a header-only success file.

Ligue 1 is enabled here as ESPN league ``fra.1`` so it participates in the same
retrieval, provider fallback, persistence, and daily CSV workflow as the other
configured soccer leagues.
"""

from __future__ import annotations

import csv
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import _soccer_odds_core as core
from _soccer_odds_core import *  # noqa: F401,F403 - preserve existing imports/API


# Extend the core league configuration without changing the proven ESPN parser.
# main() reads these dictionaries at runtime, so Ligue 1 is included by default
# and is also selectable explicitly through --league.
core.LEAGUES["ligue_1"] = "fra.1"
core.LEAGUE_ALIASES.update(
    {
        "ligue_1": "ligue_1",
        "ligue1": "ligue_1",
        "ligue-1": "ligue_1",
        "fra.1": "ligue_1",
        "france": "ligue_1",
    }
)


class ScoreboardFetchError(RuntimeError):
    """Raised when ESPN fixture discovery is incomplete or malformed."""


def fetch_scoreboard(
    client: core.ESPNClient,
    league_slug: str,
    target_date: date,
) -> list[Mapping[str, Any]]:
    """Fetch a complete three-day scoreboard window or fail safely.

    The core implementation asks for the target date plus one day on either side
    so America/New_York date assignment does not lose late MLS fixtures. Because
    all three legs contribute to that guarantee, none may silently fail.

    A payload with ``events: []`` is a successful empty slate. A non-dict payload,
    missing/non-list ``events``, HTTP failure, timeout, or JSON failure is treated
    as incomplete fixture discovery and no daily CSV should be written.
    """
    date_candidates = [
        target_date - timedelta(days=1),
        target_date,
        target_date + timedelta(days=1),
    ]

    events_by_id: dict[str, Mapping[str, Any]] = {}
    failed_days: list[str] = []

    for day in date_candidates:
        day_text = day.strftime("%Y%m%d")
        payload = client.get_json(
            f"{core.SITE_BASE}/{league_slug}/scoreboard",
            params={"dates": day_text, "limit": 1000},
        )

        if not isinstance(payload, dict):
            failed_days.append(day_text)
            continue

        events = payload.get("events")
        if not isinstance(events, list):
            client.log(
                f"  invalid scoreboard payload for {league_slug} {day_text}: "
                "missing list-valued events"
            )
            failed_days.append(day_text)
            continue

        for event in events:
            if isinstance(event, dict) and event.get("id") is not None:
                events_by_id[str(event["id"])] = event

    if failed_days:
        joined = ", ".join(failed_days)
        raise ScoreboardFetchError(
            f"ESPN scoreboard discovery incomplete for {league_slug}; "
            f"failed date request(s): {joined}. No sportsbook CSV was written."
        )

    return list(events_by_id.values())


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


# core.main() resolves these globals from the core module at execution time.
# Replace fixture discovery with the strict wrapper and keep non-destructive CSV
# persistence while leaving the existing ESPN odds parsing/fallback logic intact.
core.fetch_scoreboard = fetch_scoreboard
core.write_csv = write_csv
main = core.main


if __name__ == "__main__":
    raise SystemExit(main())
