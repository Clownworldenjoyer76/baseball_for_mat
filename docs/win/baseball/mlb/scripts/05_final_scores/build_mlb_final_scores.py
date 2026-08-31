#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/05_final_scores/build_mlb_final_scores.py

import csv
import json
import re
import traceback
from datetime import datetime, UTC
from pathlib import Path
from zoneinfo import ZoneInfo

ERROR_DIR = Path("docs/win/baseball/mlb/errors/05_final_scores")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "build_mlb_final_scores.txt"

RAW_DIR = Path("docs/win/baseball/mlb/00_intake/drat_raw")
GAMES_DIR = Path("docs/win/baseball/mlb/00_intake/games")
PRED_DIR = Path("docs/win/baseball/mlb/00_intake/predictions/pred_with_game_id")
SPORTSBOOK_DIR = Path("docs/win/baseball/mlb/00_intake/sportsbook")
FINAL_DIR = Path("docs/win/baseball/mlb/05_final_scores/results/final_scores")
AUDIT_DIR = Path("docs/win/baseball/mlb/05_final_scores/results/audit")

FINAL_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

STATUS_AUDIT_FILE = AUDIT_DIR / "final_score_status_audit.csv"
KEY_AUDIT_FILE = AUDIT_DIR / "final_score_key_audit.csv"
UNRESOLVED_AUDIT_FILE = AUDIT_DIR / "unresolved_completed_games.csv"

RUN_TS = datetime.now(UTC).isoformat()
DOUBLEHEADER_TIME_TOLERANCE_MINUTES = 90
ET = ZoneInfo("America/New_York")

TEAM_KEY_ALIASES = {
    "oakland athletics": "athletics",
    "athletics": "athletics",
    "st louis cardinals": "st louis cardinals",
    "st. louis cardinals": "st louis cardinals",
}

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== build_mlb_final_scores RUN {RUN_TS} ===\n")


class FinalScoreConflictError(RuntimeError):
    """Fatal contradiction between records that identify the same game."""


def log(msg: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(UTC).isoformat()} | {msg}\n")


def fail(msg: str) -> None:
    log(f"FATAL: {msg}")
    raise RuntimeError(msg)


def fail_conflict(msg: str) -> None:
    log(f"FATAL: {msg}")
    raise FinalScoreConflictError(msg)


def failure_context(
    *,
    source_file,
    game_date,
    game_time,
    away_team,
    home_team,
    game_id,
    gamePk,
):
    return (
        f"source_file={source_file} | "
        f"game_date={game_date} | "
        f"game_time={game_time} | "
        f"away_team={away_team} | "
        f"home_team={home_team} | "
        f"game_id={game_id} | "
        f"gamePk={gamePk}"
    )


def parse_datetime(dt_str):
    dt = datetime.strptime(dt_str.strip(), "%m/%d/%Y %I:%M %p")
    return dt, dt.strftime("%Y_%m_%d"), dt.strftime("%I:%M %p")


def parse_time_minutes(value):
    value = str(value).strip()
    if not value:
        return None

    for fmt in ["%I:%M %p", "%H:%M:%S", "%H:%M"]:
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.hour * 60 + parsed.minute
        except ValueError:
            continue

    return None


def clean_team(team_str):
    return str(team_str).split("(")[0].strip()


def normalize_team_key(team_str):
    cleaned = clean_team(team_str)
    lowered = cleaned.lower().strip()
    lowered = TEAM_KEY_ALIASES.get(lowered, lowered)
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def matchup_key(home_team, away_team):
    return (
        normalize_team_key(home_team),
        normalize_team_key(away_team),
    )


def normalize_status(raw_status):
    raw = str(raw_status or "").strip().lower()

    if raw in {"final", "game over", "completed", "complete"}:
        return "final"

    if raw in {"postponed", "ppd"}:
        return "postponed"

    if raw in {"canceled", "cancelled"}:
        return "canceled"

    if raw in {"suspended"}:
        return "suspended"

    if raw in {"delayed", "delay"}:
        return "delayed"

    if raw in {"in progress", "live", "active"}:
        return "in_progress"

    if raw in {"scheduled", "pre-game", "pregame", "preview"}:
        return "scheduled"

    return "unknown"


def infer_game_status(row):
    """
    DRatings raw rows currently appear list-based.

    Completed historical score rows use the 8-field row shape.
    If an explicit status field exists in a dict-shaped source later,
    preserve it. Otherwise 8-field rows are inferred as final.
    """
    explicit_status_fields = [
        "game_status",
        "status",
        "abstractGameState",
        "detailedState",
        "codedGameState",
        "statusCode",
    ]

    if isinstance(row, dict):
        for field in explicit_status_fields:
            val = row.get(field)
            if val not in (None, ""):
                return normalize_status(val), str(val).strip(), field, True

    if isinstance(row, list) and len(row) == 8:
        return "final", "final", "row_len_8_completed_score", False

    return "unknown", "unknown", "not_available_in_current_raw_shape", False


def is_completed_game(row):
    status_norm, _raw_status, _status_source, _status_available = infer_game_status(row)
    return status_norm == "final" and isinstance(row, list) and len(row) == 8


def raw_snapshot_date_from_path(path):
    suffix = "_mlb_raw.json"
    name = Path(path).name

    if not name.endswith(suffix):
        return ""

    return name[:-len(suffix)]


def et_utc_offset_minutes_for_date(game_date):
    try:
        local_dt = datetime.strptime(game_date, "%Y_%m_%d").replace(
            hour=12,
            tzinfo=ET,
        )
    except ValueError:
        return 0

    offset = local_dt.utcoffset()
    if offset is None:
        return 0

    return int(abs(offset.total_seconds()) // 60)


def time_match_targets(
    target_game_time,
    *,
    correction_minutes=0,
    prefer_correction=False,
):
    target_minutes = parse_time_minutes(target_game_time)

    if target_minutes is None:
        return []

    correction_minutes = int(correction_minutes or 0)

    if correction_minutes <= 0:
        return [(target_minutes, 0)]

    corrected_minutes = (target_minutes + correction_minutes) % (24 * 60)

    if corrected_minutes == target_minutes:
        return [(target_minutes, 0)]

    if prefer_correction:
        return [
            (corrected_minutes, 0),
            (target_minutes, 1),
        ]

    return [
        (target_minutes, 0),
        (corrected_minutes, 1),
    ]


def closest_time_record_match(
    candidates,
    target_game_time,
    *,
    correction_minutes=0,
    prefer_correction=False,
):
    if not candidates:
        return {}

    targets = time_match_targets(
        target_game_time,
        correction_minutes=correction_minutes,
        prefer_correction=prefer_correction,
    )

    if not targets:
        return {}

    scored = []

    for candidate_index, candidate in enumerate(candidates):
        candidate_minutes = parse_time_minutes(candidate.get("game_time", ""))
        if candidate_minutes is None:
            continue

        candidate_best = None

        for target_minutes, target_priority in targets:
            diff = abs(candidate_minutes - target_minutes)
            diff = min(diff, (24 * 60) - diff)
            score = (diff, target_priority)

            if candidate_best is None or score < candidate_best:
                candidate_best = score

        if candidate_best is not None:
            scored.append((candidate_best, candidate_index, candidate))

    if not scored:
        return {}

    best_score = min(item[0] for item in scored)

    if best_score[0] > DOUBLEHEADER_TIME_TOLERANCE_MINUTES:
        return {}

    tied = [
        item
        for item in scored
        if item[0] == best_score
    ]

    if len(tied) != 1:
        return {}

    return tied[0][2]


def closest_time_match(
    candidates,
    target_game_time,
    value_field,
    *,
    correction_minutes=0,
    prefer_correction=False,
):
    match = closest_time_record_match(
        candidates,
        target_game_time,
        correction_minutes=correction_minutes,
        prefer_correction=prefer_correction,
    )

    if not match:
        return ""

    return match.get(value_field, "")


def closest_time_book_match(
    candidates,
    target_game_time,
    *,
    correction_minutes=0,
    prefer_correction=False,
):
    return closest_time_record_match(
        candidates,
        target_game_time,
        correction_minutes=correction_minutes,
        prefer_correction=prefer_correction,
    )


def load_games_lookup(date):
    path = GAMES_DIR / f"{date}_games.csv"
    lookup = {}

    if not path.exists():
        log(f"GAMES FILE MISSING FOR FINAL-SCORE GAME_ID/GAMEPK LOOKUP: {path}")
        return lookup

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for r in reader:
            home_team = str(r.get("home_team", "") or "").strip()
            away_team = str(r.get("away_team", "") or "").strip()
            key = matchup_key(home_team, away_team)

            lookup.setdefault(key, []).append({
                "game_id": str(r.get("game_id", "") or "").strip(),
                "gamePk": str(r.get("gamePk", "") or "").strip(),
                "gameNumber": str(r.get("gameNumber", "") or "").strip(),
                "game_time": str(r.get("game_time", "") or "").strip(),
                "home_team": home_team,
                "away_team": away_team,
            })

    return lookup


def load_games_by_game_id(date):
    path = GAMES_DIR / f"{date}_games.csv"
    lookup = {}

    if not path.exists():
        log(f"GAMES FILE MISSING FOR FINAL-SCORE GAME_ID/GAMEPK LOOKUP: {path}")
        return lookup

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            game_id = str(row.get("game_id", "") or "").strip()

            if not game_id:
                continue

            if game_id in lookup:
                fail(
                    "Duplicate game_id in games file during final-score backfill: "
                    f"date={date} game_id={game_id}"
                )

            lookup[game_id] = {
                "game_id": game_id,
                "gamePk": str(row.get("gamePk", "") or "").strip(),
                "gameNumber": str(row.get("gameNumber", "") or "").strip(),
                "game_time": str(row.get("game_time", "") or "").strip(),
                "home_team": str(row.get("home_team", "") or "").strip(),
                "away_team": str(row.get("away_team", "") or "").strip(),
            }

    return lookup


def load_games_by_gamepk(date):
    path = GAMES_DIR / f"{date}_games.csv"
    lookup = {}

    if not path.exists():
        return lookup

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            gamePk = str(row.get("gamePk", "") or "").strip()

            if not gamePk:
                continue

            if gamePk in lookup:
                fail(
                    "Duplicate gamePk in games file during final-score backfill: "
                    f"date={date} gamePk={gamePk}"
                )

            lookup[gamePk] = {
                "game_id": str(row.get("game_id", "") or "").strip(),
                "gamePk": gamePk,
                "gameNumber": str(row.get("gameNumber", "") or "").strip(),
                "game_time": str(row.get("game_time", "") or "").strip(),
                "home_team": str(row.get("home_team", "") or "").strip(),
                "away_team": str(row.get("away_team", "") or "").strip(),
            }

    return lookup


def load_predictions_lookup(date):
    path = PRED_DIR / f"{date}_MLB.csv"
    lookup = {}

    if not path.exists():
        log(f"PREDICTION FILE MISSING FOR FINAL-SCORE GAME_ID LOOKUP: {path}")
        return lookup

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for r in reader:
            home_team = str(r.get("home_team", "") or "").strip()
            away_team = str(r.get("away_team", "") or "").strip()
            key = matchup_key(home_team, away_team)

            lookup.setdefault(key, []).append({
                "game_id": str(r.get("game_id", "") or "").strip(),
                "gamePk": str(r.get("gamePk", "") or "").strip(),
                "gameNumber": str(r.get("gameNumber", "") or "").strip(),
                "game_time": str(r.get("game_time", "") or "").strip(),
                "home_team": home_team,
                "away_team": away_team,
            })

    return lookup


def load_sportsbook_lookup(date):
    path = SPORTSBOOK_DIR / f"{date}_MLB.csv"
    lookup = {}

    if not path.exists():
        log(f"SPORTSBOOK FILE MISSING FOR FINAL-SCORE MARKET-LINE LOOKUP: {path}")
        return lookup

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for r in reader:
            home_team = str(r.get("home_team", "") or "").strip()
            away_team = str(r.get("away_team", "") or "").strip()
            key = matchup_key(home_team, away_team)

            lookup.setdefault(key, []).append({
                "game_time": str(r.get("game_time", "") or "").strip(),
                "away_run_line": r.get("away_run_line"),
                "home_run_line": r.get("home_run_line"),
                "total": r.get("total"),
            })

    return lookup


def candidate_matches_teams(candidate, home_team, away_team):
    if not candidate:
        return False

    return matchup_key(
        candidate.get("home_team", ""),
        candidate.get("away_team", ""),
    ) == matchup_key(home_team, away_team)


def resolve_completed_game_ids(
    *,
    game_date,
    game_time,
    home_team,
    away_team,
    current_game_id="",
    current_gamePk="",
    current_gameNumber="",
    games_lookup,
    games_by_game_id,
    games_by_gamepk,
    predictions_lookup,
):
    key = matchup_key(home_team, away_team)
    games_candidates = games_lookup.get(key, [])
    pred_candidates = predictions_lookup.get(key, [])

    current_game_id = str(current_game_id or "").strip()
    current_gamePk = str(current_gamePk or "").strip()
    current_gameNumber = str(current_gameNumber or "").strip()

    def result_from_game(candidate, source):
        return {
            "resolved": bool(
                str(candidate.get("game_id", "") or "").strip()
                and str(candidate.get("gamePk", "") or "").strip()
            ),
            "game_id": str(candidate.get("game_id", "") or "").strip(),
            "gamePk": str(candidate.get("gamePk", "") or "").strip(),
            "gameNumber": str(candidate.get("gameNumber", "") or "").strip(),
            "scheduled_game_time": str(
                candidate.get("game_time", "") or game_time or ""
            ).strip(),
            "resolution_source": source,
            "games_candidate_count": len(games_candidates),
            "prediction_candidate_count": len(pred_candidates),
            "reason": "",
        }

    if current_gamePk:
        candidate = games_by_gamepk.get(current_gamePk, {})
        if candidate and candidate_matches_teams(candidate, home_team, away_team):
            resolved = result_from_game(candidate, "games_by_existing_gamePk")
            if not resolved["game_id"] and current_game_id:
                resolved["game_id"] = current_game_id
                resolved["resolved"] = bool(resolved["gamePk"] and resolved["game_id"])
            return resolved

    games_match = closest_time_record_match(
        games_candidates,
        game_time,
        correction_minutes=0,
        prefer_correction=False,
    )

    if games_match:
        return result_from_game(games_match, "games_date_teams_time")

    pred_match = closest_time_record_match(
        pred_candidates,
        game_time,
        correction_minutes=0,
        prefer_correction=False,
    )

    if pred_match:
        pred_game_id = str(pred_match.get("game_id", "") or "").strip()
        pred_gamePk = str(pred_match.get("gamePk", "") or "").strip()
        pred_gameNumber = str(pred_match.get("gameNumber", "") or "").strip()

        if pred_gamePk:
            official = games_by_gamepk.get(pred_gamePk, {})
            if official and candidate_matches_teams(official, home_team, away_team):
                return result_from_game(
                    official,
                    "predictions_date_teams_time_then_games_by_gamePk",
                )

        if pred_game_id:
            official = games_by_game_id.get(pred_game_id, {})
            if official and candidate_matches_teams(official, home_team, away_team):
                return result_from_game(
                    official,
                    "predictions_date_teams_time_then_games_by_game_id",
                )

        return {
            "resolved": bool(pred_game_id and pred_gamePk),
            "game_id": pred_game_id,
            "gamePk": pred_gamePk,
            "gameNumber": pred_gameNumber,
            "scheduled_game_time": str(
                pred_match.get("game_time", "") or game_time or ""
            ).strip(),
            "resolution_source": "predictions_date_teams_time",
            "games_candidate_count": len(games_candidates),
            "prediction_candidate_count": len(pred_candidates),
            "reason": (
                "prediction matched but corresponding official games row "
                "could not supply both game_id and gamePk"
            ),
        }

    if current_game_id and parse_time_minutes(game_time) is None:
        candidate = games_by_game_id.get(current_game_id, {})
        if candidate and candidate_matches_teams(candidate, home_team, away_team):
            return result_from_game(candidate, "games_by_existing_game_id_no_time")

    reason_parts = []

    if not games_candidates:
        reason_parts.append("no normalized date/team candidate in games")
    else:
        reason_parts.append(
            "games candidates existed but scheduled time did not resolve uniquely"
        )

    if not pred_candidates:
        reason_parts.append("no normalized date/team candidate in predictions")
    else:
        reason_parts.append(
            "prediction candidates existed but scheduled time did not resolve uniquely"
        )

    if current_game_id:
        reason_parts.append(
            "existing game_id was not accepted without a supporting scheduled-time match"
        )

    return {
        "resolved": False,
        "game_id": current_game_id,
        "gamePk": current_gamePk,
        "gameNumber": current_gameNumber,
        "scheduled_game_time": str(game_time or "").strip(),
        "resolution_source": "unresolved",
        "games_candidate_count": len(games_candidates),
        "prediction_candidate_count": len(pred_candidates),
        "reason": "; ".join(reason_parts),
    }


def make_unresolved_completed_row(
    *,
    source_file,
    row_index,
    game_date,
    game_time,
    away_team,
    home_team,
    final_away_score,
    final_home_score,
    game_id,
    gamePk,
    gameNumber,
    games_candidate_count,
    prediction_candidate_count,
    resolution_reason,
    raw_row,
):
    return {
        "source_file": source_file,
        "row_index": row_index,
        "game_date": game_date,
        "game_time": game_time,
        "away_team": away_team,
        "home_team": home_team,
        "final_away_score": final_away_score,
        "final_home_score": final_home_score,
        "game_id": game_id,
        "gamePk": gamePk,
        "gameNumber": gameNumber,
        "games_candidate_count": games_candidate_count,
        "prediction_candidate_count": prediction_candidate_count,
        "resolution_reason": resolution_reason,
        "raw_row": raw_row,
    }


SUMMARY_ROW_PREFIXES = {"Sportsbooks", "DRatings"}

FINAL_HEADER = [
    "sport",
    "league",
    "game_id",
    "gamePk",
    "gameNumber",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "final_away_score",
    "final_home_score",
    "final_total",
    "away_run_line",
    "home_run_line",
    "total",
    "game_status",
    "final_scores_generated_at",
]


def is_summary_row(row):
    return row and isinstance(row, list) and str(row[0]).strip() in SUMMARY_ROW_PREFIXES


def write_csv(path, header, rows, files_written, label):
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    files_written.append((str(path), len(rows)))
    log(f"WROTE {label} -> {path} ({len(rows)} rows)")


def write_audit_csv(path, header, rows, label):
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()

        for row in rows:
            writer.writerow({col: row.get(col, "") for col in header})

    log(f"WROTE {label} -> {path} ({len(rows)} rows)")


def raw_row_text(row):
    try:
        return json.dumps(row, ensure_ascii=False, default=str)
    except Exception:
        return repr(row)


def make_parse_error_row(*, source_file, row_index, stage, error, row):
    return {
        "source_file": source_file,
        "row_index": row_index,
        "stage": stage,
        "error": str(error),
        "raw_row": raw_row_text(row),
    }


def log_review_rows(parse_error_rows, unresolved_completed_rows):
    log("--- PARSE ERROR ROWS FOR REVIEW ---")

    if not parse_error_rows:
        log("None")
    else:
        for item in parse_error_rows:
            log(
                "PARSE_ERROR | "
                f"source_file={item.get('source_file', '')} | "
                f"row_index={item.get('row_index', '')} | "
                f"stage={item.get('stage', '')} | "
                f"error={item.get('error', '')} | "
                f"raw_row={item.get('raw_row', '')}"
            )

    log("--- UNRESOLVED COMPLETED GAMES FOR REVIEW ---")

    if not unresolved_completed_rows:
        log("None")
    else:
        for item in unresolved_completed_rows:
            log(
                "UNRESOLVED_COMPLETED_GAME | "
                f"source_file={item.get('source_file', '')} | "
                f"row_index={item.get('row_index', '')} | "
                f"game_date={item.get('game_date', '')} | "
                f"game_time={item.get('game_time', '')} | "
                f"away_team={item.get('away_team', '')} | "
                f"home_team={item.get('home_team', '')} | "
                f"final_away_score={item.get('final_away_score', '')} | "
                f"final_home_score={item.get('final_home_score', '')} | "
                f"game_id={item.get('game_id', '')} | "
                f"gamePk={item.get('gamePk', '')} | "
                f"gameNumber={item.get('gameNumber', '')} | "
                f"games_candidate_count={item.get('games_candidate_count', '')} | "
                f"prediction_candidate_count={item.get('prediction_candidate_count', '')} | "
                f"resolution_reason={item.get('resolution_reason', '')} | "
                f"raw_row={item.get('raw_row', '')}"
            )


def final_row_signature(record):
    return (
        str(record.get("sport", "") or "").strip(),
        str(record.get("league", "") or "").strip(),
        str(record.get("game_date", "") or "").strip(),
        str(record.get("home_team", "") or "").strip(),
        str(record.get("away_team", "") or "").strip(),
        str(record.get("final_away_score", "") or "").strip(),
        str(record.get("final_home_score", "") or "").strip(),
        str(record.get("final_total", "") or "").strip(),
        str(record.get("game_status", "") or "").strip(),
    )


def merge_duplicate_metadata(existing, record):
    existing_gamepk = str(existing.get("gamePk", "") or "").strip()
    incoming_gamepk = str(record.get("gamePk", "") or "").strip()

    for field in (
        "gamePk",
        "gameNumber",
        "away_run_line",
        "home_run_line",
        "total",
    ):
        if not str(existing.get(field, "") or "").strip():
            incoming = record.get(field, "")
            if str(incoming or "").strip():
                existing[field] = incoming

    if not existing_gamepk and incoming_gamepk:
        incoming_time = str(record.get("game_time", "") or "").strip()
        if incoming_time:
            existing["game_time"] = incoming_time

    return existing


def make_key_audit_row(
    *,
    game_date,
    game_id,
    gamePk,
    gameNumber,
    away_team,
    home_team,
    duplicate_count,
    status,
    notes,
):
    return {
        "game_date": game_date,
        "game_id": game_id,
        "gamePk": gamePk,
        "gameNumber": gameNumber,
        "away_team": away_team,
        "home_team": home_team,
        "duplicate_count": duplicate_count,
        "status": status,
        "notes": notes,
    }


def add_final_record(
    *,
    record,
    source_file,
    final_records_by_date,
    seen_by_game_id,
    seen_by_fallback_key,
    key_audit_rows,
    use_game_time_for_fallback,
):
    game_id = str(record.get("game_id", "") or "").strip()
    gamePk = str(record.get("gamePk", "") or "").strip()
    game_date = str(record.get("game_date", "") or "").strip()
    game_time = str(record.get("game_time", "") or "").strip()
    home_team = str(record.get("home_team", "") or "").strip()
    away_team = str(record.get("away_team", "") or "").strip()

    record["_source_file"] = source_file

    if game_id:
        existing = seen_by_game_id.get(game_id)

        if existing is None:
            seen_by_game_id[game_id] = record
            final_records_by_date.setdefault(game_date, []).append(record)

            key_audit_rows.append(make_key_audit_row(
                game_date=game_date,
                game_id=game_id,
                gamePk=record.get("gamePk", ""),
                gameNumber=record.get("gameNumber", ""),
                away_team=away_team,
                home_team=home_team,
                duplicate_count=1,
                status="unique_game_id",
                notes="accepted; primary key game_id",
            ))
            return "accepted"

        if final_row_signature(existing) == final_row_signature(record):
            merge_duplicate_metadata(existing, record)
            key_audit_rows.append(make_key_audit_row(
                game_date=game_date,
                game_id=game_id,
                gamePk=record.get("gamePk", ""),
                gameNumber=record.get("gameNumber", ""),
                away_team=away_team,
                home_team=home_team,
                duplicate_count=2,
                status="identical_duplicate_collapsed",
                notes="duplicate game_id row was identical and was not written twice",
            ))
            return "duplicate_collapsed"

        key_audit_rows.append(make_key_audit_row(
            game_date=game_date,
            game_id=game_id,
            gamePk=record.get("gamePk", ""),
            gameNumber=record.get("gameNumber", ""),
            away_team=away_team,
            home_team=home_team,
            duplicate_count=2,
            status="conflicting_duplicate_game_id",
            notes="same game_id had conflicting final-score fields",
        ))

        context = failure_context(
            source_file=source_file,
            game_date=game_date,
            game_time=game_time,
            away_team=away_team,
            home_team=home_team,
            game_id=game_id,
            gamePk=gamePk,
        )

        existing_source_file = str(
            existing.get("_source_file", "") or ""
        ).strip()

        fail_conflict(
            "Conflicting final-score duplicate game_id found | "
            f"{context} | "
            f"existing_source_file={existing_source_file}"
        )

    fallback_key = (game_date, home_team, away_team, game_time)
    fallback_notes = (
        "game_id missing; fallback date/team/time key used "
        "to distinguish unresolved games and detect true duplicates"
    )

    existing_fallback = seen_by_fallback_key.get(fallback_key)

    if existing_fallback is None:
        seen_by_fallback_key[fallback_key] = record
        final_records_by_date.setdefault(game_date, []).append(record)

        key_audit_rows.append(make_key_audit_row(
            game_date=game_date,
            game_id="",
            gamePk=record.get("gamePk", ""),
            gameNumber=record.get("gameNumber", ""),
            away_team=away_team,
            home_team=home_team,
            duplicate_count=1,
            status="blank_game_id_written_for_downstream_audit",
            notes=fallback_notes,
        ))
        return "accepted_blank_game_id"

    if final_row_signature(existing_fallback) == final_row_signature(record):
        merge_duplicate_metadata(existing_fallback, record)
        key_audit_rows.append(make_key_audit_row(
            game_date=game_date,
            game_id="",
            gamePk=record.get("gamePk", ""),
            gameNumber=record.get("gameNumber", ""),
            away_team=away_team,
            home_team=home_team,
            duplicate_count=2,
            status="blank_game_id_identical_duplicate_collapsed",
            notes="blank-game_id duplicate was identical and was not written twice",
        ))
        return "blank_game_id_duplicate_collapsed"

    key_audit_rows.append(make_key_audit_row(
        game_date=game_date,
        game_id="",
        gamePk=record.get("gamePk", ""),
        gameNumber=record.get("gameNumber", ""),
        away_team=away_team,
        home_team=home_team,
        duplicate_count=2,
        status="blank_game_id_conflicting_duplicate",
        notes="blank-game_id duplicate fallback key had conflicting fields",
    ))

    context = failure_context(
        source_file=source_file,
        game_date=game_date,
        game_time=game_time,
        away_team=away_team,
        home_team=home_team,
        game_id="",
        gamePk=gamePk,
    )

    existing_source_file = str(
        existing_fallback.get("_source_file", "") or ""
    ).strip()

    fail_conflict(
        "Conflicting blank-game_id final-score duplicate found | "
        f"{context} | "
        f"existing_source_file={existing_source_file}"
    )

    return "failed"


def process_file(
    file_path,
    final_records_by_date,
    seen_by_game_id,
    seen_by_fallback_key,
    status_audit_rows,
    key_audit_rows,
    parse_error_rows,
    unresolved_completed_rows,
):
    log(f"Processing {file_path.name}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    games_lookup_cache = {}
    games_by_game_id_cache = {}
    games_by_gamepk_cache = {}
    predictions_lookup_cache = {}
    sportsbook_lookup_cache = {}

    parse_errors = 0
    skipped_summary = 0
    skipped_duplicate = 0
    skipped_not_completed = 0
    completed_rows_seen = 0
    accepted_rows = 0
    unresolved_rows = 0

    if not isinstance(data, list):
        parse_errors += 1
        parse_error_rows.append(make_parse_error_row(
            source_file=file_path.name,
            row_index="",
            stage="validate_json_structure",
            error=f"expected top-level JSON list, found {type(data).__name__}",
            row=data,
        ))

        log(
            f"  completed_rows_seen={completed_rows_seen}, "
            f"accepted_rows={accepted_rows}, "
            f"unresolved_completed_rows={unresolved_rows}, "
            f"parse_errors={parse_errors}, "
            f"skipped_summary={skipped_summary}, "
            f"skipped_duplicate={skipped_duplicate}, "
            f"skipped_not_completed={skipped_not_completed}, "
            f"final_score_dates_accumulated={len(final_records_by_date)}"
        )
        return

    for row_index, row in enumerate(data, start=1):
        if not isinstance(row, list):
            parse_errors += 1
            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="validate_row_structure",
                error=f"expected row list, found {type(row).__name__}",
                row=row,
            ))
            continue

        if not row:
            parse_errors += 1
            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="validate_row_structure",
                error="empty row",
                row=row,
            ))
            continue

        if is_summary_row(row):
            skipped_summary += 1
            continue

        if len(row) < 2:
            parse_errors += 1
            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="validate_row_structure",
                error=f"expected at least 2 fields, found {len(row)}",
                row=row,
            ))
            continue

        status_norm, raw_status, status_source, status_available = infer_game_status(row)

        if not is_completed_game(row):
            skipped_not_completed += 1
            status_audit_rows.append({
                "game_date": "",
                "game_id": "",
                "gamePk": "",
                "gameNumber": "",
                "away_team": "",
                "home_team": "",
                "final_away_score": "",
                "final_home_score": "",
                "game_status": status_norm,
                "status_source": status_source,
                "status_available": str(status_available),
                "status_notes": "non-final row not written to final-score output",
            })
            continue

        completed_rows_seen += 1

        try:
            _dt, game_date, raw_game_time = parse_datetime(row[0])
        except Exception as exc:
            parse_errors += 1
            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="parse_datetime",
                error=exc,
                row=row,
            ))
            continue

        try:
            team_value = row[1]

            if not isinstance(team_value, str):
                raise TypeError(
                    f"expected team field to be str, found {type(team_value).__name__}"
                )

            teams = team_value.split("\n")

            if len(teams) < 2:
                raise ValueError("expected at least two team names")

            away_team = clean_team(teams[0])
            home_team = clean_team(teams[1])

            if not away_team or not home_team:
                raise ValueError("away or home team is blank")

        except Exception as exc:
            parse_errors += 1
            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="parse_teams",
                error=exc,
                row=row,
            ))
            continue

        key = matchup_key(home_team, away_team)

        try:
            score_value = row[5]

            if not isinstance(score_value, str):
                raise TypeError(
                    f"expected score field to be str, found {type(score_value).__name__}"
                )

            scores = score_value.split("\n")

            if len(scores) < 2:
                raise ValueError(
                    f"expected away/home final scores, found {len(scores)} score field(s)"
                )

            away_score = int(scores[0].strip())
            home_score = int(scores[1].strip())
            final_total = str(away_score + home_score)

        except Exception as exc:
            parse_errors += 1
            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="parse_scores",
                error=exc,
                row=row,
            ))
            continue

        try:
            if game_date not in games_lookup_cache:
                games_lookup_cache[game_date] = load_games_lookup(game_date)
                games_by_game_id_cache[game_date] = load_games_by_game_id(game_date)
                games_by_gamepk_cache[game_date] = load_games_by_gamepk(game_date)

            if game_date not in predictions_lookup_cache:
                predictions_lookup_cache[game_date] = load_predictions_lookup(game_date)

            if game_date not in sportsbook_lookup_cache:
                sportsbook_lookup_cache[game_date] = load_sportsbook_lookup(game_date)

            games_lookup = games_lookup_cache[game_date]
            games_by_game_id = games_by_game_id_cache[game_date]
            games_by_gamepk = games_by_gamepk_cache[game_date]
            pred_lookup = predictions_lookup_cache[game_date]
            book_lookup = sportsbook_lookup_cache[game_date]

            resolution = resolve_completed_game_ids(
                game_date=game_date,
                game_time=raw_game_time,
                home_team=home_team,
                away_team=away_team,
                games_lookup=games_lookup,
                games_by_game_id=games_by_game_id,
                games_by_gamepk=games_by_gamepk,
                predictions_lookup=pred_lookup,
            )

            game_id = str(resolution.get("game_id", "") or "").strip()
            gamePk = str(resolution.get("gamePk", "") or "").strip()
            gameNumber = str(resolution.get("gameNumber", "") or "").strip()
            scheduled_game_time = str(
                resolution.get("scheduled_game_time", "") or raw_game_time
            ).strip()

            if not resolution.get("resolved") or not game_id or not gamePk:
                unresolved_rows += 1
                unresolved_completed_rows.append(make_unresolved_completed_row(
                    source_file=file_path.name,
                    row_index=row_index,
                    game_date=game_date,
                    game_time=raw_game_time,
                    away_team=away_team,
                    home_team=home_team,
                    final_away_score=str(away_score),
                    final_home_score=str(home_score),
                    game_id=game_id,
                    gamePk=gamePk,
                    gameNumber=gameNumber,
                    games_candidate_count=resolution.get("games_candidate_count", 0),
                    prediction_candidate_count=resolution.get(
                        "prediction_candidate_count", 0
                    ),
                    resolution_reason=resolution.get("reason", "unresolved"),
                    raw_row=raw_row_text(row),
                ))

                status_audit_rows.append({
                    "game_date": game_date,
                    "game_id": game_id,
                    "gamePk": gamePk,
                    "gameNumber": gameNumber,
                    "away_team": away_team,
                    "home_team": home_team,
                    "final_away_score": str(away_score),
                    "final_home_score": str(home_score),
                    "game_status": status_norm,
                    "status_source": status_source,
                    "status_available": str(status_available),
                    "status_notes": (
                        "completed game unresolved; excluded from final-score output "
                        "and written to unresolved_completed_games.csv"
                    ),
                })
                continue

            book_candidates = book_lookup.get(key, [])
            book = closest_time_book_match(
                book_candidates,
                raw_game_time,
                correction_minutes=0,
                prefer_correction=False,
            )

            record = {
                "sport": "baseball",
                "league": "mlb",
                "game_id": game_id,
                "gamePk": gamePk,
                "gameNumber": gameNumber,
                "game_date": game_date,
                "game_time": scheduled_game_time,
                "home_team": home_team,
                "away_team": away_team,
                "final_away_score": str(away_score),
                "final_home_score": str(home_score),
                "final_total": final_total,
                "away_run_line": book.get("away_run_line"),
                "home_run_line": book.get("home_run_line"),
                "total": book.get("total"),
                "game_status": status_norm,
                "final_scores_generated_at": RUN_TS,
            }

            action = add_final_record(
                record=record,
                source_file=file_path.name,
                final_records_by_date=final_records_by_date,
                seen_by_game_id=seen_by_game_id,
                seen_by_fallback_key=seen_by_fallback_key,
                key_audit_rows=key_audit_rows,
                use_game_time_for_fallback=False,
            )

            if action in {
                "duplicate_collapsed",
                "blank_game_id_duplicate_collapsed",
            }:
                skipped_duplicate += 1
            else:
                accepted_rows += 1

            status_audit_rows.append({
                "game_date": game_date,
                "game_id": game_id,
                "gamePk": gamePk,
                "gameNumber": gameNumber,
                "away_team": away_team,
                "home_team": home_team,
                "final_away_score": str(away_score),
                "final_home_score": str(home_score),
                "game_status": status_norm,
                "status_source": status_source,
                "status_available": str(status_available),
                "status_notes": (
                    f"resolved_ids={resolution.get('resolution_source', '')}; "
                    + (
                        "explicit source status available"
                        if status_available
                        else "status inferred as final from completed DRatings row shape"
                    )
                ),
            })

        except FinalScoreConflictError:
            raise

        except Exception as exc:
            parse_errors += 1
            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="build_final_record",
                error=exc,
                row=row,
            ))
            continue

    log(
        f"  completed_rows_seen={completed_rows_seen}, "
        f"accepted_rows={accepted_rows}, "
        f"unresolved_completed_rows={unresolved_rows}, "
        f"parse_errors={parse_errors}, "
        f"skipped_summary={skipped_summary}, "
        f"skipped_duplicate={skipped_duplicate}, "
        f"skipped_not_completed={skipped_not_completed}, "
        f"final_score_dates_accumulated={len(final_records_by_date)}"
    )


def legacy_final_date_from_path(path):
    suffix = "_final_scores_MLB.csv"
    name = path.name

    if not name.endswith(suffix):
        return ""

    return name[:-len(suffix)]


def legacy_row_has_final_score(row):
    try:
        away_score = int(str(row.get("final_away_score", "")).strip())
        home_score = int(str(row.get("final_home_score", "")).strip())
    except (TypeError, ValueError):
        return False

    return away_score >= 0 and home_score >= 0


def migrate_legacy_final_score_files(files_written, unresolved_completed_rows):
    migrated_files = 0
    migrated_rows = 0
    resolved_rows = 0
    unresolved_rows = 0

    for path in sorted(FINAL_DIR.glob("*_final_scores_MLB.csv")):
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)

        if not fieldnames:
            fail(f"Legacy final-score file has no header: {path}")

        date = legacy_final_date_from_path(path)

        if not date:
            fail(f"Could not derive date from legacy final-score path: {path}")

        games_lookup = load_games_lookup(date)
        games_by_game_id = load_games_by_game_id(date)
        games_by_gamepk = load_games_by_gamepk(date)
        predictions_lookup = load_predictions_lookup(date)

        missing_header_columns = [
            col
            for col in FINAL_HEADER
            if col not in fieldnames
        ]

        changed = bool(missing_header_columns)
        output_rows = []

        for row_index, row in enumerate(rows, start=2):
            record = {
                col: str(row.get(col, "") or "").strip()
                for col in FINAL_HEADER
            }

            record["sport"] = record["sport"] or "baseball"
            record["league"] = record["league"] or "mlb"
            record["game_date"] = record["game_date"] or date

            if not record["game_status"] and legacy_row_has_final_score(record):
                record["game_status"] = "final"
                changed = True

            if not record["final_total"] and legacy_row_has_final_score(record):
                record["final_total"] = str(
                    int(record["final_away_score"])
                    + int(record["final_home_score"])
                )
                changed = True

            if not record["final_scores_generated_at"]:
                record["final_scores_generated_at"] = RUN_TS
                changed = True

            completed = (
                record["game_status"].strip().lower() == "final"
                and legacy_row_has_final_score(record)
            )

            if completed:
                before_ids = (
                    record["game_id"],
                    record["gamePk"],
                    record["gameNumber"],
                    record["game_time"],
                )

                resolution = resolve_completed_game_ids(
                    game_date=record["game_date"],
                    game_time=record["game_time"],
                    home_team=record["home_team"],
                    away_team=record["away_team"],
                    current_game_id=record["game_id"],
                    current_gamePk=record["gamePk"],
                    current_gameNumber=record["gameNumber"],
                    games_lookup=games_lookup,
                    games_by_game_id=games_by_game_id,
                    games_by_gamepk=games_by_gamepk,
                    predictions_lookup=predictions_lookup,
                )

                record["game_id"] = str(
                    resolution.get("game_id", "") or ""
                ).strip()
                record["gamePk"] = str(
                    resolution.get("gamePk", "") or ""
                ).strip()
                record["gameNumber"] = str(
                    resolution.get("gameNumber", "") or ""
                ).strip()

                scheduled_time = str(
                    resolution.get("scheduled_game_time", "") or ""
                ).strip()

                if scheduled_time:
                    record["game_time"] = scheduled_time

                after_ids = (
                    record["game_id"],
                    record["gamePk"],
                    record["gameNumber"],
                    record["game_time"],
                )

                if after_ids != before_ids:
                    changed = True
                    resolved_rows += 1

                if (
                    not resolution.get("resolved")
                    or not record["game_id"]
                    or not record["gamePk"]
                ):
                    unresolved_rows += 1
                    changed = True
                    unresolved_completed_rows.append(make_unresolved_completed_row(
                        source_file=path.name,
                        row_index=row_index,
                        game_date=record["game_date"],
                        game_time=record["game_time"],
                        away_team=record["away_team"],
                        home_team=record["home_team"],
                        final_away_score=record["final_away_score"],
                        final_home_score=record["final_home_score"],
                        game_id=record["game_id"],
                        gamePk=record["gamePk"],
                        gameNumber=record["gameNumber"],
                        games_candidate_count=resolution.get(
                            "games_candidate_count", 0
                        ),
                        prediction_candidate_count=resolution.get(
                            "prediction_candidate_count", 0
                        ),
                        resolution_reason=resolution.get(
                            "reason",
                            "legacy completed game could not resolve both IDs",
                        ),
                        raw_row=raw_row_text(row),
                    ))
                    continue

            output_rows.append([
                record.get(col, "")
                for col in FINAL_HEADER
            ])

        if changed:
            write_csv(
                path,
                FINAL_HEADER,
                output_rows,
                files_written,
                "historical final-score ID/schema backfill",
            )

            migrated_files += 1
            migrated_rows += len(output_rows)

            log(
                "MIGRATED HISTORICAL FINAL-SCORE FILE | "
                f"file={path.name} | rows={len(output_rows)} | "
                f"missing_header_columns={missing_header_columns}"
            )

    log(f"Historical final-score files updated: {migrated_files}")
    log(f"Historical final-score rows retained: {migrated_rows}")
    log(f"Historical completed rows resolved/backfilled: {resolved_rows}")
    log(f"Historical completed rows moved to unresolved audit: {unresolved_rows}")

    return {
        "migrated_files": migrated_files,
        "migrated_rows": migrated_rows,
        "resolved_rows": resolved_rows,
        "unresolved_rows": unresolved_rows,
    }


def verify_final_score_outputs_have_gamepk():
    bad_rows = []

    for path in sorted(FINAL_DIR.glob("*_final_scores_MLB.csv")):
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row_index, row in enumerate(reader, start=2):
                status = str(row.get("game_status", "") or "").strip().lower()

                if status != "final" or not legacy_row_has_final_score(row):
                    continue

                gamePk = str(row.get("gamePk", "") or "").strip()

                if gamePk:
                    continue

                bad_rows.append({
                    "file": path.name,
                    "row": row_index,
                    "game_id": str(row.get("game_id", "") or "").strip(),
                    "game_date": str(row.get("game_date", "") or "").strip(),
                    "game_time": str(row.get("game_time", "") or "").strip(),
                    "away_team": str(row.get("away_team", "") or "").strip(),
                    "home_team": str(row.get("home_team", "") or "").strip(),
                })

    if bad_rows:
        sample = bad_rows[:10]
        fail(
            "Completed final-score rows with blank gamePk remain after backfill; "
            f"bad_rows={len(bad_rows)} sample={sample}"
        )

    log("VERIFY: completed final-score output rows with blank gamePk: 0")


def main():
    files_written = []
    final_records_by_date = {}
    seen_by_game_id = {}
    seen_by_fallback_key = {}
    status_audit_rows = []
    key_audit_rows = []
    parse_error_rows = []
    unresolved_completed_rows = []

    status_audit_header = [
        "game_date",
        "game_id",
        "gamePk",
        "gameNumber",
        "away_team",
        "home_team",
        "final_away_score",
        "final_home_score",
        "game_status",
        "status_source",
        "status_available",
        "status_notes",
    ]

    key_audit_header = [
        "game_date",
        "game_id",
        "gamePk",
        "gameNumber",
        "away_team",
        "home_team",
        "duplicate_count",
        "status",
        "notes",
    ]

    unresolved_audit_header = [
        "source_file",
        "row_index",
        "game_date",
        "game_time",
        "away_team",
        "home_team",
        "final_away_score",
        "final_home_score",
        "game_id",
        "gamePk",
        "gameNumber",
        "games_candidate_count",
        "prediction_candidate_count",
        "resolution_reason",
        "raw_row",
    ]

    try:
        raw_files = sorted(RAW_DIR.glob("*_mlb_raw.json"))

        if not raw_files:
            fail(f"No DRatings raw files found in {RAW_DIR}")

        log(f"Raw files found: {len(raw_files)}")
        log(f"Historical final-score build timestamp: {RUN_TS}")

        for file in raw_files:
            process_file(
                file_path=file,
                final_records_by_date=final_records_by_date,
                seen_by_game_id=seen_by_game_id,
                seen_by_fallback_key=seen_by_fallback_key,
                status_audit_rows=status_audit_rows,
                key_audit_rows=key_audit_rows,
                parse_error_rows=parse_error_rows,
                unresolved_completed_rows=unresolved_completed_rows,
            )

        total_parse_errors = len(parse_error_rows)

        if total_parse_errors > 0:
            log("--- SUMMARY ---")
            log(f"Raw files processed before failure: {len(raw_files)}")
            log(f"Parse errors encountered: {total_parse_errors}")

            log_review_rows(
                parse_error_rows,
                unresolved_completed_rows,
            )

            fail(
                "Final-score build aborted because parse_errors="
                f"{total_parse_errors}. "
                "Final-score outputs were not written."
            )

        for date in sorted(final_records_by_date):
            records = final_records_by_date[date]

            bad_resolved = [
                record
                for record in records
                if str(record.get("game_status", "") or "").strip().lower() == "final"
                and (
                    not str(record.get("game_id", "") or "").strip()
                    or not str(record.get("gamePk", "") or "").strip()
                )
            ]

            if bad_resolved:
                fail(
                    "Resolved completed rows cannot be written with blank game_id/gamePk; "
                    f"date={date} bad_rows={len(bad_resolved)}"
                )

            out = FINAL_DIR / f"{date}_final_scores_MLB.csv"
            rows = [
                [record.get(col, "") for col in FINAL_HEADER]
                for record in records
            ]

            write_csv(
                out,
                FINAL_HEADER,
                rows,
                files_written,
                "final scores",
            )

        legacy_backfill_summary = migrate_legacy_final_score_files(
            files_written,
            unresolved_completed_rows,
        )

        verify_final_score_outputs_have_gamepk()

        write_audit_csv(
            STATUS_AUDIT_FILE,
            status_audit_header,
            status_audit_rows,
            "final-score status audit",
        )

        write_audit_csv(
            KEY_AUDIT_FILE,
            key_audit_header,
            key_audit_rows,
            "final-score key audit",
        )

        write_audit_csv(
            UNRESOLVED_AUDIT_FILE,
            unresolved_audit_header,
            unresolved_completed_rows,
            "unresolved completed-game audit",
        )

        unknown_status_count = sum(
            1
            for row in status_audit_rows
            if str(row.get("game_status", "")).strip().lower() == "unknown"
        )

        total_parse_errors = len(parse_error_rows)
        unresolved_completed_rows_count = len(unresolved_completed_rows)

        log("--- SUMMARY ---")
        log(f"Raw files processed: {len(raw_files)}")
        log(f"Files written: {len(files_written)}")
        log(f"Final-score dates written once: {len(final_records_by_date)}")
        log(f"Final-score game_id primary-key rows: {len(seen_by_game_id)}")
        log(f"Unresolved completed rows: {unresolved_completed_rows_count}")
        log(f"Parse errors encountered: {total_parse_errors}")
        log(f"Unknown status audit rows: {unknown_status_count}")
        log(
            "Historical final-score files updated: "
            f"{legacy_backfill_summary['migrated_files']}"
        )
        log(
            "Historical final-score rows retained: "
            f"{legacy_backfill_summary['migrated_rows']}"
        )
        log(
            "Historical completed rows resolved/backfilled: "
            f"{legacy_backfill_summary['resolved_rows']}"
        )
        log(
            "Historical completed rows moved to unresolved audit: "
            f"{legacy_backfill_summary['unresolved_rows']}"
        )
        log(f"Status audit: {STATUS_AUDIT_FILE}")
        log(f"Key audit: {KEY_AUDIT_FILE}")
        log(f"Unresolved completed-game audit: {UNRESOLVED_AUDIT_FILE}")

        if total_parse_errors:
            fail(
                "Final-score build cannot report success because "
                f"parse_errors={total_parse_errors}"
            )
        else:
            log("Parse-error review: no parse errors encountered.")

        if unresolved_completed_rows_count:
            log(
                "WARNING: Genuinely unresolved completed games were excluded "
                f"from final-score outputs and written to {UNRESOLVED_AUDIT_FILE}."
            )
        else:
            log("Unresolved completed-game review: none.")

        for path, count in files_written:
            log(f"  FILE: {path} ({count} rows)")

        log_review_rows(
            parse_error_rows,
            unresolved_completed_rows,
        )

        log("STATUS: SUCCESS")

    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        raise

    print("MLB final-score build complete.")


if __name__ == "__main__":
    main()