#!/usr/bin/env python3
# docs/win/baseball/scripts/00_intake/build_games_list.py
#
# Runs after scrape_mlb_raw.py and odds_parse.py.
# Joins mlb_raw to sportsbook to produce an authoritative {date}_games.csv.
#
# Matching rules:
#   1. Same home/away teams.
#   2. Never reuse a sportsbook row / game_id.
#   3. If same matchup has exactly one raw row and exactly one sportsbook row:
#      match them even if MLB raw time is stale, and log the time difference.
#   4. If same matchup has multiple raw rows and the same number of sportsbook rows:
#      pair by chronological order. This handles normal doubleheaders.
#   5. Otherwise, match to the closest unused sportsbook time within threshold.

import csv
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

MLB_RAW_DIR = Path("docs/win/baseball/mlb/00_intake/mlb_raw")
BOOK_DIR = Path("docs/win/baseball/mlb/00_intake/sportsbook")
MAPS_DIR = Path("docs/win/baseball/mlb/maps")

OUT_DIR = Path("docs/win/baseball/mlb/00_intake/games")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/baseball/mlb/errors/00_intake")
ERROR_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = ERROR_DIR / "build_games_list.txt"

MAX_TIME_DIFF_MINUTES = 90

OUTPUT_HEADER = [
    "gamePk",
    "game_id",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_team_id",
    "away_team_id",
    "venue_id",
    "doubleheader",
    "gameNumber",
    "home_pitcher_id",
    "away_pitcher_id",
    "day_night",
]


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc).isoformat()


def log(msg: str, level: str = "INFO"):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level:<5} | {msg.rstrip()}\n")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def norm(s):
    """Lowercase, strip punctuation, collapse spaces for team name matching."""
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def load_csv(path: Path) -> list:
    if not path.exists():
        log(f"MISSING: {path}", "WARN")
        return []

    with open(path, newline="", encoding="utf-8-sig") as f:
        _qodana_return_value = list(csv.DictReader(f))
    return _qodana_return_value


def load_team_map() -> dict:
    """Returns dict: norm(team_name) -> team_id."""
    rows = load_csv(MAPS_DIR / "mlb_team_ids.csv")
    m = {}

    for r in rows:
        tid = r.get("team_id", "").strip()

        if not tid:
            continue

        for col in [
            "name",
            "team_name",
            "short_name",
            "club_name",
            "franchise_name",
        ]:
            val = norm(r.get(col, ""))

            if val:
                m[val] = tid

    return m


def build_id_to_name_map(rows: list) -> dict:
    """Returns dict: team_id -> full name."""
    return {
        r.get("team_id", "").strip(): r.get("name", "").strip()
        for r in rows
    }


def parse_int(value, default=0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def utc_to_local_datetime(
    utc_str: str,
    tz_id: str = "America/New_York",
):
    """Convert MLB UTC ISO time string to local aware datetime."""
    try:
        dt = datetime.fromisoformat(str(utc_str).replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo(tz_id))
    except (TypeError, ValueError, OverflowError, KeyError):
        return None


def parse_book_datetime(
    date_str: str,
    time_str: str,
    tz_id: str = "America/New_York",
):
    """Parse sportsbook date + HH:MM:SS as local aware datetime."""
    try:
        date_clean = str(date_str).replace("_", "-").strip()
        time_clean = str(time_str).strip()
        dt = datetime.strptime(
            f"{date_clean} {time_clean}",
            "%Y-%m-%d %H:%M:%S",
        )
        return dt.replace(tzinfo=ZoneInfo(tz_id))
    except (TypeError, ValueError, OverflowError, KeyError):
        return None


def sort_dt_key(entry: dict):
    dt = entry.get("local_dt") or entry.get("book_dt")

    if dt is None:
        return (
            1,
            datetime.max.replace(
                tzinfo=ZoneInfo("America/New_York")
            ),
        )

    return 0, dt


def minutes_between(a, b):
    if a is None or b is None:
        return None

    return abs((a - b).total_seconds()) / 60.0


def make_output_row(raw_entry: dict, book_entry: dict) -> dict:
    r = raw_entry["row"]
    b = book_entry["row"]

    return {
        "gamePk": r.get("gamePk", ""),
        "game_id": b.get("game_id", ""),
        "game_date": r.get("game_date", ""),
        "game_time": b.get("game_time", ""),
        "home_team": b.get("home_team", ""),
        "away_team": b.get("away_team", ""),
        "home_team_id": r.get("home_team_id", ""),
        "away_team_id": r.get("away_team_id", ""),
        "venue_id": r.get("venue_id", ""),
        "doubleheader": r.get("doubleheader", "N"),
        "gameNumber": r.get("gameNumber", "1"),
        "home_pitcher_id": r.get("home_pitcher_id", ""),
        "away_pitcher_id": r.get("away_pitcher_id", ""),
        "day_night": r.get("day_night", ""),
    }


def write_games_file(
    out_path: Path,
    output_rows: list,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=OUTPUT_HEADER,
        )
        writer.writeheader()
        writer.writerows(output_rows)


# ─────────────────────────────────────────────
# PROCESS ONE DATE
# ─────────────────────────────────────────────

def _build_raw_game_groups(date_str, raw_rows, id_to_name):
    groups = {}
    order = []
    for row in raw_rows:
        home_tid = row.get("home_team_id", "").strip()
        away_tid = row.get("away_team_id", "").strip()
        home_name = id_to_name.get(home_tid, "")
        away_name = id_to_name.get(away_tid, "")
        key = (norm(home_name), norm(away_name))
        if not key[0] or not key[1]:
            log(
                f"{date_str} | raw gamePk={row.get('gamePk', '')} missing team name "
                f"home_team_id={home_tid} away_team_id={away_tid}", "WARN"
            )
            continue
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append({
            "row": row, "key": key, "home_name": home_name, "away_name": away_name,
            "local_dt": utc_to_local_datetime(row.get("game_time", "")),
            "game_number": parse_int(row.get("gameNumber", "1"), 1),
        })
    return groups, order


def _build_book_game_groups(date_str, book_rows):
    groups = {}
    for idx, row in enumerate(book_rows):
        key = (norm(row.get("home_team", "")), norm(row.get("away_team", "")))
        groups.setdefault(key, []).append({
            "row": row, "key": key,
            "book_dt": parse_book_datetime(date_str, row.get("game_time", "")),
            "used": False, "index": idx,
        })
    return groups


def _match_single_raw_game(date_str, label, raw_entry, book_entry, output_rows):
    book_entry["used"] = True
    output_rows.append(make_output_row(raw_entry, book_entry))
    diff = minutes_between(raw_entry.get("local_dt"), book_entry.get("book_dt"))
    diff_text = "" if diff is None else f" diff_minutes={round(diff, 1)}"
    level = "WARN" if diff is not None and diff > MAX_TIME_DIFF_MINUTES else "INFO"
    match_label = (
        "MATCHED one-to-one with time mismatch"
        if level == "WARN" else "MATCHED one-to-one"
    )
    log(
        f"{date_str} | {match_label}: {label} "
        f"gamePk={raw_entry['row'].get('gamePk', '')} "
        f"gameNumber={raw_entry['row'].get('gameNumber', '')} "
        f"game_id={book_entry['row'].get('game_id', '')}{diff_text}", level
    )


def _match_ordered_raw_games(date_str, label, raws, books, output_rows):
    sorted_raws = sorted(
        raws,
        key=lambda item: (
            sort_dt_key(item), item["game_number"], item["row"].get("gamePk", "")
        ),
    )
    sorted_books = sorted(books, key=lambda item: (sort_dt_key(item), item["index"]))
    log(
        f"{date_str} | ORDER MATCH duplicate matchup: {label} "
        f"raw_count={len(sorted_raws)} sportsbook_count={len(sorted_books)}"
    )
    for raw_entry, book_entry in zip(sorted_raws, sorted_books):
        book_entry["used"] = True
        output_rows.append(make_output_row(raw_entry, book_entry))
        diff = minutes_between(raw_entry.get("local_dt"), book_entry.get("book_dt"))
        diff_text = "" if diff is None else f" diff_minutes={round(diff, 1)}"
        log(
            f"{date_str} | MATCHED order: {label} "
            f"gamePk={raw_entry['row'].get('gamePk', '')} "
            f"gameNumber={raw_entry['row'].get('gameNumber', '')} "
            f"game_id={book_entry['row'].get('game_id', '')}{diff_text}"
        )


def _match_closest_raw_games(date_str, label, raws, books, output_rows):
    matched = 0
    unmatched = 0
    sorted_raws = sorted(
        raws,
        key=lambda item: (
            sort_dt_key(item), item["game_number"], item["row"].get("gamePk", "")
        ),
    )
    for raw_entry in sorted_raws:
        available = [book for book in books if not book["used"]]
        if not available:
            log(
                f"{date_str} | UNMATCHED no unused sportsbook row: {label} "
                f"gamePk={raw_entry['row'].get('gamePk', '')}", "WARN"
            )
            unmatched += 1
            continue
        scored = [
            (minutes_between(raw_entry.get("local_dt"), book.get("book_dt")), book)
            for book in available
        ]
        valid = [item for item in scored if item[0] is not None]
        selected = None
        selected_diff = None
        if valid:
            selected_diff, selected = min(valid, key=lambda item: item[0])
            if selected_diff > MAX_TIME_DIFF_MINUTES:
                selected = None
        if selected is None:
            diffs_text = ", ".join(
                f"{item[1]['row'].get('game_time', '')}:"
                f"{'NA' if item[0] is None else round(item[0], 1)}"
                for item in scored
            )
            log(
                f"{date_str} | UNMATCHED time threshold: {label} "
                f"gamePk={raw_entry['row'].get('gamePk', '')} candidate_diffs={diffs_text}",
                "WARN",
            )
            unmatched += 1
            continue
        selected["used"] = True
        output_rows.append(make_output_row(raw_entry, selected))
        matched += 1
        diff_text = "" if selected_diff is None else f" diff_minutes={round(selected_diff, 1)}"
        log(
            f"{date_str} | MATCHED closest: {label} "
            f"gamePk={raw_entry['row'].get('gamePk', '')} "
            f"gameNumber={raw_entry['row'].get('gameNumber', '')} "
            f"game_id={selected['row'].get('game_id', '')}{diff_text}"
        )
    return matched, unmatched


def _match_raw_game_group(date_str, raws, books, output_rows):
    label = (
        f"{raws[0]['away_name']} @ {raws[0]['home_name']}"
        if raws else ""
    )
    if not books:
        for raw_entry in raws:
            log(
                f"{date_str} | UNMATCHED no sportsbook rows: {label} "
                f"gamePk={raw_entry['row'].get('gamePk', '')}", "WARN"
            )
        return 0, len(raws)
    unused = [book for book in books if not book["used"]]
    if len(raws) == 1 and len(unused) == 1:
        _match_single_raw_game(date_str, label, raws[0], unused[0], output_rows)
        return 1, 0
    if 1 < len(raws) == len(unused):
        _match_ordered_raw_games(date_str, label, raws, unused, output_rows)
        return len(raws), 0
    return _match_closest_raw_games(date_str, label, raws, books, output_rows)


def _log_unused_book_rows(date_str, book_groups):
    for books in book_groups.values():
        for book_entry in [book for book in books if not book["used"]]:
            row = book_entry["row"]
            log(
                f"{date_str} | UNUSED sportsbook row: {row.get('away_team', '')} @ "
                f"{row.get('home_team', '')} game_id={row.get('game_id', '')} "
                f"game_time={row.get('game_time', '')}", "WARN"
            )


def _duplicate_output_game_ids(date_str, output_rows):
    seen = {}
    duplicates = 0
    for row in output_rows:
        game_id = row.get("game_id", "")
        if not game_id:
            continue
        if game_id in seen:
            duplicates += 1
            log(
                f"{date_str} | DUPLICATE OUTPUT game_id={game_id} "
                f"first_gamePk={seen[game_id]} second_gamePk={row.get('gamePk', '')}",
                "ERROR",
            )
        else:
            seen[game_id] = row.get("gamePk", "")
    return duplicates


def _write_games_output(date_str, output_rows, summary):
    if not output_rows:
        log(f"{date_str} | no matched games ??? file not written", "WARN")
        return
    output_rows = sorted(
        output_rows,
        key=lambda row: (
            row.get("game_date", ""), row.get("game_time", ""),
            row.get("home_team", ""), row.get("away_team", ""),
            parse_int(row.get("gameNumber", "1"), 1),
        ),
    )
    out_path = OUT_DIR / f"{date_str}_games.csv"
    write_games_file(out_path, output_rows)
    log(f"{date_str} | WROTE: {out_path} ({len(output_rows)} games)")
    summary["files_written"] += 1


def process_date(date_str: str, id_to_name: dict, summary: dict) -> None:
    raw_path = MLB_RAW_DIR / f"{date_str}_mlb_raw.csv"
    book_path = BOOK_DIR / f"{date_str}_MLB.csv"
    raw_rows = load_csv(raw_path)
    book_rows = load_csv(book_path)
    if not raw_rows:
        log(f"{date_str} | no mlb_raw ??? skipping", "WARN")
        summary["skipped"] += 1
        return
    if not book_rows:
        log(f"{date_str} | no sportsbook ??? skipping", "WARN")
        summary["skipped"] += 1
        return
    raw_groups, raw_key_order = _build_raw_game_groups(date_str, raw_rows, id_to_name)
    book_groups = _build_book_game_groups(date_str, book_rows)
    output_rows = []
    matched = 0
    unmatched = 0
    for key in raw_key_order:
        matched_delta, unmatched_delta = _match_raw_game_group(
            date_str, raw_groups.get(key, []), book_groups.get(key, []), output_rows
        )
        matched += matched_delta
        unmatched += unmatched_delta
    _log_unused_book_rows(date_str, book_groups)
    duplicates = _duplicate_output_game_ids(date_str, output_rows)
    summary["errors"] += duplicates
    log(f"{date_str} | matched={matched} unmatched={unmatched}")
    summary["total_matched"] += matched
    summary["total_unmatched"] += unmatched
    _write_games_output(date_str, output_rows, summary)

def main():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(
            f"=== build_games_list RUN {_now()} ===\n"
        )

    summary = {
        "files_written": 0,
        "total_matched": 0,
        "total_unmatched": 0,
        "skipped": 0,
        "errors": 0,
    }

    try:
        team_map = load_team_map()
        team_rows = load_csv(
            MAPS_DIR / "mlb_team_ids.csv"
        )
        id_to_name = build_id_to_name_map(
            team_rows
        )

        log(
            f"Team map loaded: "
            f"{len(team_map)} entries | "
            f"id_to_name: "
            f"{len(id_to_name)} entries"
        )

        raw_files = sorted(
            MLB_RAW_DIR.glob("*_mlb_raw.csv")
        )
        log(
            f"mlb_raw files found: "
            f"{len(raw_files)}"
        )

        for rf in raw_files:
            date_str = rf.stem.replace(
                "_mlb_raw",
                "",
            )

            try:
                process_date(
                    date_str,
                    id_to_name,
                    summary,
                )
            except Exception as e:
                log(
                    f"{date_str} FAILED: "
                    f"{e}\n"
                    f"{traceback.format_exc()}",
                    "ERROR",
                )
                summary["errors"] += 1

    except Exception as e:
        log(
            f"FATAL: {e}\n"
            f"{traceback.format_exc()}",
            "ERROR",
        )
        summary["errors"] += 1

    status = (
        "SUCCESS"
        if summary["errors"] == 0
        else "COMPLETED WITH ERRORS"
    )

    lines = [
        "",
        "=" * 60,
        f"SUMMARY  {_now()}",
        "=" * 60,
        (
            f"  files_written   : "
            f"{summary['files_written']}"
        ),
        (
            f"  total_matched   : "
            f"{summary['total_matched']}"
        ),
        (
            f"  total_unmatched : "
            f"{summary['total_unmatched']}"
        ),
        (
            f"  skipped         : "
            f"{summary['skipped']}"
        ),
        (
            f"  errors          : "
            f"{summary['errors']}"
        ),
        "",
        f"STATUS: {status}",
        "=" * 60,
    ]

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(
        f"build_games_list complete. "
        f"{summary['files_written']} "
        f"files written. "
        f"Status: {status}"
    )


if __name__ == "__main__":
    main()