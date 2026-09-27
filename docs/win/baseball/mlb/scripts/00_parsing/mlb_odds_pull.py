#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/00_parsing/mlb_odds_pull.py

from pathlib import Path

from mlb_odds_common import (
    current_time_context,
    fetch_espn_events,
    find_matching_event_index,
    has_started,
    is_target_et_date,
    print_pull_summary,
    read_json_list,
    sort_events,
    write_json,
)


NOW_UTC, NOW_ET, TARGET_ET_DATE, today = current_time_context()

PRIMARY_OUTPUT_PATH = Path(f"docs/win/baseball/mlb/odds/{today}.json")
OUTPUT_PATHS = [PRIMARY_OUTPUT_PATH]


def filter_target_date_events(events):
    return [
        event
        for event in events
        if isinstance(event, dict) and is_target_et_date(event, TARGET_ET_DATE)
    ]


def merge_with_existing(existing_events, pulled_events):
    merged = filter_target_date_events(existing_events)
    updated = 0
    added = 0
    preserved_started = 0

    for pulled in pulled_events:
        match_index = find_matching_event_index(merged, pulled)

        if match_index is None:
            merged.append(pulled)
            added += 1
            continue

        if has_started(merged[match_index], NOW_UTC):
            preserved_started += 1
            continue

        merged[match_index] = pulled
        updated += 1

    return sort_events(merged), updated, added, preserved_started


def main():
    (
        data,
        espn_events_found,
        skipped_non_target,
        skipped_started,
        skipped_no_odds,
        skipped_incomplete,
    ) = fetch_espn_events(TARGET_ET_DATE, NOW_UTC)

    data = sort_events(data)
    existing_data = read_json_list(PRIMARY_OUTPUT_PATH)
    output_data, updated_existing, added_new, preserved_started_existing = merge_with_existing(
        existing_data,
        data,
    )

    for output_path in OUTPUT_PATHS:
        write_json(output_path, output_data)
        print(f"Saved {output_path}")

    print_pull_summary(
        target_et_date=TARGET_ET_DATE,
        date_string=today,
        now_utc=NOW_UTC,
        now_et=NOW_ET,
        espn_events_found=espn_events_found,
        skipped_non_target=skipped_non_target,
        skipped_started=skipped_started,
        skipped_no_odds=skipped_no_odds,
        skipped_incomplete=skipped_incomplete,
        converted_count=len(data),
        started_label="Started events skipped",
    )
    print(f"Existing output seed count: {len(existing_data)}")
    print(f"Updated existing not-started events: {updated_existing}")
    print(f"Added new pending events: {added_new}")
    print(f"Preserved existing started events from overwrite: {preserved_started_existing}")
    print(f"Final output event count: {len(output_data)}")


if __name__ == "__main__":
    main()
