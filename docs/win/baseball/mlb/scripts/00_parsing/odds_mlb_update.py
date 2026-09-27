#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/00_parsing/odds_mlb_update.py

from pathlib import Path

from mlb_odds_common import (
    current_time_context,
    event_identity,
    fetch_espn_events,
    find_matching_event_index,
    has_started,
    is_target_et_date,
    print_pull_summary,
    read_json_list,
    sort_events,
    write_json,
)


now_utc, now_et, TARGET_ET_DATE, today = current_time_context()
stamp = now_et.strftime("%Y_%m_%d_%H%M")

ODDS_DIR = Path("docs/win/baseball/mlb/odds")
ORIGINAL_PATH = ODDS_DIR / f"{today}.json"
UPDATES_DIR = ODDS_DIR / "updates"
LATEST_DIR = ODDS_DIR / "latest"

UPDATE_PATH = UPDATES_DIR / f"{stamp}.json"
LATEST_PATH = LATEST_DIR / f"{today}.json"


def filter_target_date_events(events):
    filtered = []
    dropped = 0

    for event in events:
        if not isinstance(event, dict):
            dropped += 1
            continue

        if is_target_et_date(event, TARGET_ET_DATE):
            filtered.append(event)
        else:
            dropped += 1

    return filtered, dropped


def choose_latest_seed():
    if LATEST_PATH.exists():
        raw = read_json_list(LATEST_PATH)
        filtered, dropped = filter_target_date_events(raw)
        return LATEST_PATH, raw, filtered, dropped

    if ORIGINAL_PATH.exists():
        raw = read_json_list(ORIGINAL_PATH)
        filtered, dropped = filter_target_date_events(raw)
        return ORIGINAL_PATH, raw, filtered, dropped

    return None, [], [], 0


def merge_latest(existing_events, pulled_events):
    merged = list(existing_events)
    updated_count = 0
    added_count = 0
    preserved_count = 0
    skipped_started_updates = 0
    skipped_started_adds = 0

    pulled_identities = {event_identity(event) for event in pulled_events}

    for pulled in pulled_events:
        match_index = find_matching_event_index(merged, pulled)

        if match_index is None:
            if has_started(pulled, now_utc):
                skipped_started_adds += 1
                continue
            merged.append(pulled)
            added_count += 1
            continue

        if has_started(merged[match_index], now_utc):
            skipped_started_updates += 1
            continue

        merged[match_index] = pulled
        updated_count += 1

    for existing in merged:
        if event_identity(existing) not in pulled_identities:
            preserved_count += 1

    return (
        sort_events(merged),
        updated_count,
        added_count,
        preserved_count,
        skipped_started_updates,
        skipped_started_adds,
    )


def main():
    (
        pulled_data,
        espn_events_found,
        skipped_non_target,
        skipped_started,
        skipped_no_odds,
        skipped_incomplete,
    ) = fetch_espn_events(TARGET_ET_DATE, now_utc)

    pulled_data = sort_events(pulled_data)

    write_json(UPDATE_PATH, pulled_data)
    print(f"Saved update snapshot: {UPDATE_PATH}")

    seed_path, seed_raw, seed_data, seed_dropped = choose_latest_seed()

    (
        latest_data,
        updated_count,
        added_count,
        preserved_count,
        skipped_started_updates,
        skipped_started_adds,
    ) = merge_latest(seed_data, pulled_data)

    write_json(LATEST_PATH, latest_data)
    print(f"Saved latest cumulative odds: {LATEST_PATH}")

    print_pull_summary(
        target_et_date=TARGET_ET_DATE,
        date_string=today,
        now_utc=now_utc,
        now_et=now_et,
        espn_events_found=espn_events_found,
        skipped_non_target=skipped_non_target,
        skipped_started=skipped_started,
        skipped_no_odds=skipped_no_odds,
        skipped_incomplete=skipped_incomplete,
        converted_count=len(pulled_data),
        started_label="Started events skipped from fresh pull",
    )
    print(f"Update snapshot event count: {len(pulled_data)}")
    print(f"Latest seed path: {seed_path if seed_path else 'NONE'}")
    print(f"Latest seed raw count: {len(seed_raw)}")
    print(f"Latest seed target-date count: {len(seed_data)}")
    print(f"Latest seed non-target/invalid rows dropped: {seed_dropped}")
    print(f"Latest updated existing events: {updated_count}")
    print(f"Latest added new events: {added_count}")
    print(f"Latest preserved events not in fresh pull: {preserved_count}")
    print(f"Latest preserved started events from overwrite: {skipped_started_updates}")
    print(f"Latest skipped started new events: {skipped_started_adds}")
    print(f"Latest final event count: {len(latest_data)}")


if __name__ == "__main__":
    main()
