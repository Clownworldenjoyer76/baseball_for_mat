#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/05_final_scores/build_mlb_final_scores.py

import csv
import json
import re
import traceback
import urllib.error
import urllib.request
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
MLB_API_TIMEOUT_SECONDS = 20
MLB_API_USER_AGENT = "baseball_for_mat-final-score-builder/1.0"

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

    if raw in {
        "final",
        "game over",
        "completed",
        "complete",
        "completed early",
    }:
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


def time_difference_minutes(first_time, second_time):
    first = parse_time_minutes(first_time)
    second = parse_time_minutes(second_time)

    if first is None or second is None:
        return None

    diff = abs(first - second)
    return min(diff, (24 * 60) - diff)


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


def select_game_candidate(
    candidates,
    target_game_time,
    *,
    current_gamePk="",
    current_gameNumber="",
):
    """
    Resolve one same-date/same-team candidate safely.

    Doubleheader precedence:
      1. exact gamePk when already known
      2. gameNumber when multiple same-team games exist
      3. scheduled game time

    A candidate is never chosen arbitrarily when multiple games remain.
    """
    candidates = list(candidates or [])

    current_gamePk = str(current_gamePk or "").strip()
    current_gameNumber = str(current_gameNumber or "").strip()

    if not candidates:
        return {}, "no candidates"

    if current_gamePk:
        gamepk_matches = [
            candidate
            for candidate in candidates
            if str(candidate.get("gamePk", "") or "").strip() == current_gamePk
        ]

        if len(gamepk_matches) != 1:
            return {}, (
                "existing gamePk did not identify exactly one "
                "date/team candidate"
            )

        candidate = gamepk_matches[0]
        candidate_game_number = str(
            candidate.get("gameNumber", "") or ""
        ).strip()

        if (
            current_gameNumber
            and candidate_game_number
            and candidate_game_number != current_gameNumber
        ):
            return {}, (
                "existing gamePk matched but gameNumber conflicted "
                f"(existing={current_gameNumber}, "
                f"candidate={candidate_game_number})"
            )

        candidate_time = str(candidate.get("game_time", "") or "").strip()

        if parse_time_minutes(target_game_time) is not None:
            if parse_time_minutes(candidate_time) is None:
                return {}, (
                    "existing gamePk matched but candidate scheduled "
                    "time was unavailable"
                )

            diff = time_difference_minutes(
                target_game_time,
                candidate_time,
            )

            if (
                diff is None
                or diff > DOUBLEHEADER_TIME_TOLERANCE_MINUTES
            ):
                return {}, (
                    "existing gamePk matched but scheduled time "
                    f"was outside tolerance ({diff} minutes)"
                )

        return candidate, "gamePk+gameNumber+scheduled_time"

    pool = candidates

    if len(pool) > 1 and current_gameNumber:
        number_matches = [
            candidate
            for candidate in pool
            if str(candidate.get("gameNumber", "") or "").strip()
            == current_gameNumber
        ]

        if not number_matches:
            return {}, (
                "doubleheader candidates existed but none matched "
                f"gameNumber={current_gameNumber}"
            )

        pool = number_matches

    if len(pool) == 1:
        candidate = pool[0]

        if parse_time_minutes(target_game_time) is None:
            if current_gameNumber:
                return candidate, "gameNumber_unique_no_time"

            return {}, (
                "single date/team candidate existed but scheduled "
                "target time was unavailable"
            )

        matched = closest_time_record_match(
            pool,
            target_game_time,
            correction_minutes=0,
            prefer_correction=False,
        )

        if not matched:
            return {}, (
                "candidate failed scheduled-time tolerance"
            )

        if current_gameNumber:
            return matched, "gameNumber+scheduled_time"

        return matched, "scheduled_time"

    matched = closest_time_record_match(
        pool,
        target_game_time,
        correction_minutes=0,
        prefer_correction=False,
    )

    if not matched:
        if current_gameNumber:
            return {}, (
                "doubleheader gameNumber candidates remained "
                "ambiguous after scheduled-time matching"
            )

        return {}, (
            "same-team candidates could not be resolved uniquely "
            "by scheduled time"
        )

    if current_gameNumber:
        return matched, "gameNumber+scheduled_time"

    return matched, "scheduled_time"


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
        games_match, match_reason = select_game_candidate(
            games_candidates,
            game_time,
            current_gamePk=current_gamePk,
            current_gameNumber=current_gameNumber,
        )

        if games_match:
            resolved = result_from_game(
                games_match,
                f"games_existing_gamePk_{match_reason}",
            )

            if not resolved["game_id"] and current_game_id:
                resolved["game_id"] = current_game_id
                resolved["resolved"] = bool(
                    resolved["gamePk"]
                    and resolved["game_id"]
                )

            return resolved

    if current_game_id:
        candidate = games_by_game_id.get(current_game_id, {})

        if (
            candidate
            and candidate_matches_teams(
                candidate,
                home_team,
                away_team,
            )
        ):
            candidate_gamePk = str(
                candidate.get("gamePk", "") or ""
            ).strip()

            candidate_gameNumber = str(
                candidate.get("gameNumber", "") or ""
            ).strip()

            candidate_match, match_reason = select_game_candidate(
                [candidate],
                game_time,
                current_gamePk=candidate_gamePk,
                current_gameNumber=(
                    current_gameNumber
                    or candidate_gameNumber
                ),
            )

            if candidate_match:
                return result_from_game(
                    candidate_match,
                    f"games_existing_game_id_{match_reason}",
                )

    games_match, games_match_reason = select_game_candidate(
        games_candidates,
        game_time,
        current_gameNumber=current_gameNumber,
    )

    if games_match:
        return result_from_game(
            games_match,
            f"games_date_teams_{games_match_reason}",
        )

    pred_match, pred_match_reason = select_game_candidate(
        pred_candidates,
        game_time,
        current_gamePk=current_gamePk,
        current_gameNumber=current_gameNumber,
    )

    if pred_match:
        pred_game_id = str(
            pred_match.get("game_id", "") or ""
        ).strip()

        pred_gamePk = str(
            pred_match.get("gamePk", "") or ""
        ).strip()

        pred_gameNumber = str(
            pred_match.get("gameNumber", "") or ""
        ).strip()

        pred_game_time = str(
            pred_match.get("game_time", "") or game_time or ""
        ).strip()

        if pred_gamePk:
            official = games_by_gamepk.get(pred_gamePk, {})

            if (
                official
                and candidate_matches_teams(
                    official,
                    home_team,
                    away_team,
                )
            ):
                official_match, official_reason = select_game_candidate(
                    [official],
                    pred_game_time,
                    current_gamePk=pred_gamePk,
                    current_gameNumber=pred_gameNumber,
                )

                if official_match:
                    return result_from_game(
                        official_match,
                        "predictions_"
                        f"{pred_match_reason}_then_games_by_gamePk_"
                        f"{official_reason}",
                    )

        if pred_game_id:
            official = games_by_game_id.get(pred_game_id, {})

            if (
                official
                and candidate_matches_teams(
                    official,
                    home_team,
                    away_team,
                )
            ):
                official_gamePk = str(
                    official.get("gamePk", "") or ""
                ).strip()

                official_match, official_reason = select_game_candidate(
                    [official],
                    pred_game_time,
                    current_gamePk=official_gamePk,
                    current_gameNumber=pred_gameNumber,
                )

                if official_match:
                    return result_from_game(
                        official_match,
                        "predictions_"
                        f"{pred_match_reason}_then_games_by_game_id_"
                        f"{official_reason}",
                    )

        official_from_matchup, official_reason = select_game_candidate(
            games_candidates,
            pred_game_time,
            current_gamePk=pred_gamePk,
            current_gameNumber=pred_gameNumber,
        )

        if official_from_matchup:
            return result_from_game(
                official_from_matchup,
                "predictions_"
                f"{pred_match_reason}_then_games_matchup_"
                f"{official_reason}",
            )

        return {
            "resolved": bool(pred_game_id and pred_gamePk),
            "game_id": pred_game_id,
            "gamePk": pred_gamePk,
            "gameNumber": pred_gameNumber,
            "scheduled_game_time": pred_game_time,
            "resolution_source": (
                f"predictions_{pred_match_reason}"
            ),
            "games_candidate_count": len(games_candidates),
            "prediction_candidate_count": len(pred_candidates),
            "reason": (
                "prediction candidate resolved, but the corresponding "
                "official games row could not be verified using "
                "gamePk, gameNumber, and scheduled time"
            ),
        }

    reason_parts = []

    if not games_candidates:
        reason_parts.append(
            "no normalized date/team candidate in games"
        )
    else:
        reason_parts.append(
            "games candidates existed but gamePk/gameNumber/"
            "scheduled-time resolution was not unique"
        )

    if not pred_candidates:
        reason_parts.append(
            "no normalized date/team candidate in predictions"
        )
    else:
        reason_parts.append(
            "prediction candidates existed but gamePk/gameNumber/"
            "scheduled-time resolution was not unique"
        )

    if current_game_id:
        reason_parts.append(
            "existing game_id could not be verified against "
            "gamePk/gameNumber/scheduled time"
        )

    if current_gamePk:
        reason_parts.append(
            "existing gamePk could not be verified against "
            "date/team/gameNumber/scheduled time"
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
    return (
        row
        and isinstance(row, list)
        and str(row[0]).strip() in SUMMARY_ROW_PREFIXES
    )


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
            writer.writerow({
                col: row.get(col, "")
                for col in header
            })

    log(f"WROTE {label} -> {path} ({len(rows)} rows)")


def raw_row_text(row):
    try:
        return json.dumps(
            row,
            ensure_ascii=False,
            default=str,
        )
    except Exception:
        return repr(row)


def make_parse_error_row(
    *,
    source_file,
    row_index,
    stage,
    error,
    row,
):
    return {
        "source_file": source_file,
        "row_index": row_index,
        "stage": stage,
        "error": str(error),
        "raw_row": raw_row_text(row),
    }


def log_review_rows(
    parse_error_rows,
    unresolved_completed_rows,
):
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
                f"prediction_candidate_count="
                f"{item.get('prediction_candidate_count', '')} | "
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


def game_identity_conflict_reason(existing, incoming):
    existing_gamePk = str(
        existing.get("gamePk", "") or ""
    ).strip()

    incoming_gamePk = str(
        incoming.get("gamePk", "") or ""
    ).strip()

    existing_game_number = str(
        existing.get("gameNumber", "") or ""
    ).strip()

    incoming_game_number = str(
        incoming.get("gameNumber", "") or ""
    ).strip()

    if (
        existing_gamePk
        and incoming_gamePk
        and existing_gamePk != incoming_gamePk
    ):
        return (
            "same game_id mapped to different gamePk values "
            f"({existing_gamePk} vs {incoming_gamePk})"
        )

    if (
        existing_game_number
        and incoming_game_number
        and existing_game_number != incoming_game_number
    ):
        return (
            "same game_id mapped to different gameNumber values "
            f"({existing_game_number} vs {incoming_game_number})"
        )

    if (
        existing_gamePk
        and incoming_gamePk
        and existing_gamePk == incoming_gamePk
        and existing_game_number
        and incoming_game_number
        and existing_game_number == incoming_game_number
    ):
        existing_time = str(
            existing.get("game_time", "") or ""
        ).strip()

        incoming_time = str(
            incoming.get("game_time", "") or ""
        ).strip()

        diff = time_difference_minutes(
            existing_time,
            incoming_time,
        )

        if (
            diff is not None
            and diff > DOUBLEHEADER_TIME_TOLERANCE_MINUTES
        ):
            return (
                "same game_id/gamePk/gameNumber had incompatible "
                f"scheduled times ({existing_time} vs {incoming_time})"
            )

    return ""


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
    gameNumber = str(record.get("gameNumber", "") or "").strip()
    game_date = str(record.get("game_date", "") or "").strip()
    game_time = str(record.get("game_time", "") or "").strip()
    home_team = str(record.get("home_team", "") or "").strip()
    away_team = str(record.get("away_team", "") or "").strip()

    record["_source_file"] = source_file

    if game_id:
        existing = seen_by_game_id.get(game_id)

        if existing is None:
            seen_by_game_id[game_id] = record
            final_records_by_date.setdefault(
                game_date,
                [],
            ).append(record)

            key_audit_rows.append(make_key_audit_row(
                game_date=game_date,
                game_id=game_id,
                gamePk=gamePk,
                gameNumber=gameNumber,
                away_team=away_team,
                home_team=home_team,
                duplicate_count=1,
                status="unique_game_id",
                notes="accepted; primary key game_id",
            ))

            return "accepted"

        identity_conflict = game_identity_conflict_reason(
            existing,
            record,
        )

        if identity_conflict:
            key_audit_rows.append(make_key_audit_row(
                game_date=game_date,
                game_id=game_id,
                gamePk=gamePk,
                gameNumber=gameNumber,
                away_team=away_team,
                home_team=home_team,
                duplicate_count=2,
                status="conflicting_duplicate_game_identity",
                notes=identity_conflict,
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
                "Conflicting final-score game identity found | "
                f"{context} | "
                f"gameNumber={gameNumber} | "
                f"reason={identity_conflict} | "
                f"existing_source_file={existing_source_file}"
            )

        if final_row_signature(existing) == final_row_signature(record):
            merge_duplicate_metadata(existing, record)

            key_audit_rows.append(make_key_audit_row(
                game_date=game_date,
                game_id=game_id,
                gamePk=gamePk,
                gameNumber=gameNumber,
                away_team=away_team,
                home_team=home_team,
                duplicate_count=2,
                status="identical_duplicate_collapsed",
                notes=(
                    "duplicate game_id row was identical "
                    "and had compatible gamePk/gameNumber/time"
                ),
            ))

            return "duplicate_collapsed"

        key_audit_rows.append(make_key_audit_row(
            game_date=game_date,
            game_id=game_id,
            gamePk=gamePk,
            gameNumber=gameNumber,
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
            f"gameNumber={gameNumber} | "
            f"existing_source_file={existing_source_file}"
        )

    fallback_key = (
        game_date,
        normalize_team_key(home_team),
        normalize_team_key(away_team),
        gamePk,
        gameNumber,
        game_time,
    )

    fallback_notes = (
        "game_id missing; fallback date/team/gamePk/gameNumber/time "
        "key used so same-team doubleheaders cannot collapse"
    )

    existing_fallback = seen_by_fallback_key.get(fallback_key)

    if existing_fallback is None:
        seen_by_fallback_key[fallback_key] = record
        final_records_by_date.setdefault(
            game_date,
            [],
        ).append(record)

        key_audit_rows.append(make_key_audit_row(
            game_date=game_date,
            game_id="",
            gamePk=gamePk,
            gameNumber=gameNumber,
            away_team=away_team,
            home_team=home_team,
            duplicate_count=1,
            status="blank_game_id_written_for_downstream_audit",
            notes=fallback_notes,
        ))

        return "accepted_blank_game_id"

    if (
        final_row_signature(existing_fallback)
        == final_row_signature(record)
    ):
        merge_duplicate_metadata(
            existing_fallback,
            record,
        )

        key_audit_rows.append(make_key_audit_row(
            game_date=game_date,
            game_id="",
            gamePk=gamePk,
            gameNumber=gameNumber,
            away_team=away_team,
            home_team=home_team,
            duplicate_count=2,
            status="blank_game_id_identical_duplicate_collapsed",
            notes=(
                "blank-game_id duplicate had matching "
                "gamePk/gameNumber/time and was not written twice"
            ),
        ))

        return "blank_game_id_duplicate_collapsed"

    key_audit_rows.append(make_key_audit_row(
        game_date=game_date,
        game_id="",
        gamePk=gamePk,
        gameNumber=gameNumber,
        away_team=away_team,
        home_team=home_team,
        duplicate_count=2,
        status="blank_game_id_conflicting_duplicate",
        notes=(
            "blank-game_id duplicate fallback identity had "
            "conflicting final-score fields"
        ),
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
        f"gameNumber={gameNumber} | "
        f"existing_source_file={existing_source_file}"
    )

    return "failed"


def legacy_final_date_from_path(path):
    suffix = "_final_scores_MLB.csv"
    name = path.name

    if not name.endswith(suffix):
        return ""

    return name[:-len(suffix)]


def games_date_from_path(path):
    suffix = "_games.csv"
    name = path.name

    if not name.endswith(suffix):
        return ""

    return name[:-len(suffix)]


def legacy_row_has_final_score(row):
    try:
        away_score = int(
            str(row.get("final_away_score", "")).strip()
        )
        home_score = int(
            str(row.get("final_home_score", "")).strip()
        )
    except (TypeError, ValueError):
        return False

    return away_score >= 0 and home_score >= 0


def preserve_existing_final_score_records(
    *,
    final_records_by_date,
    seen_by_game_id,
    seen_by_fallback_key,
    status_audit_rows,
    key_audit_rows,
):
    files_seen = 0
    rows_seen = 0
    rows_preserved = 0
    rows_skipped_missing_ids = 0
    rows_skipped_not_final = 0
    duplicate_rows = 0

    for path in sorted(FINAL_DIR.glob("*_final_scores_MLB.csv")):
        files_seen += 1
        date = legacy_final_date_from_path(path)

        if not date:
            continue

        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row_index, row in enumerate(reader, start=2):
                rows_seen += 1

                record = {
                    col: str(row.get(col, "") or "").strip()
                    for col in FINAL_HEADER
                }

                record["sport"] = record["sport"] or "baseball"
                record["league"] = record["league"] or "mlb"
                record["game_date"] = record["game_date"] or date

                if not record["game_status"] and legacy_row_has_final_score(record):
                    record["game_status"] = "final"

                completed = (
                    record["game_status"].strip().lower() == "final"
                    and legacy_row_has_final_score(record)
                )

                if not completed:
                    rows_skipped_not_final += 1
                    continue

                if not record["final_total"]:
                    record["final_total"] = str(
                        int(record["final_away_score"])
                        + int(record["final_home_score"])
                    )

                if not record["final_scores_generated_at"]:
                    record["final_scores_generated_at"] = RUN_TS

                if not record["game_id"] or not record["gamePk"]:
                    rows_skipped_missing_ids += 1
                    continue

                action = add_final_record(
                    record=record,
                    source_file=f"existing:{path.name}",
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
                    duplicate_rows += 1
                else:
                    rows_preserved += 1

                status_audit_rows.append({
                    "game_date": record["game_date"],
                    "game_id": record["game_id"],
                    "gamePk": record["gamePk"],
                    "gameNumber": record["gameNumber"],
                    "away_team": record["away_team"],
                    "home_team": record["home_team"],
                    "final_away_score": record["final_away_score"],
                    "final_home_score": record["final_home_score"],
                    "game_status": "final",
                    "status_source": "existing_final_score_file",
                    "status_available": "True",
                    "status_notes": (
                        "valid existing final preserved before DRatings rebuild"
                    ),
                })

    log(
        "EXISTING FINAL PRESERVATION | "
        f"files_seen={files_seen} | "
        f"rows_seen={rows_seen} | "
        f"rows_preserved={rows_preserved} | "
        f"duplicates={duplicate_rows} | "
        f"skipped_missing_ids={rows_skipped_missing_ids} | "
        f"skipped_not_final={rows_skipped_not_final}"
    )

    return {
        "files_seen": files_seen,
        "rows_seen": rows_seen,
        "rows_preserved": rows_preserved,
        "duplicates": duplicate_rows,
        "skipped_missing_ids": rows_skipped_missing_ids,
        "skipped_not_final": rows_skipped_not_final,
    }


def fetch_mlb_game_feed(gamePk, cache):
    gamePk = str(gamePk or "").strip()

    if not gamePk:
        return None

    if gamePk in cache:
        return cache[gamePk]

    url = (
        "https://statsapi.mlb.com/api/v1.1/game/"
        f"{gamePk}/feed/live"
    )

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": MLB_API_USER_AGENT,
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=MLB_API_TIMEOUT_SECONDS,
        ) as response:
            payload = json.load(response)

    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
        json.JSONDecodeError,
        OSError,
    ) as exc:
        log(
            "MLB API ERROR | "
            f"gamePk={gamePk} | "
            f"error={exc}"
        )
        cache[gamePk] = None
        return None

    cache[gamePk] = payload
    return payload


def extract_mlb_feed_status(feed):
    if not isinstance(feed, dict):
        return "unknown", "", ""

    game_data = feed.get("gameData") or {}
    status = game_data.get("status") or {}

    for field in (
        "abstractGameState",
        "detailedState",
        "codedGameState",
        "statusCode",
    ):
        raw = str(status.get(field, "") or "").strip()

        if not raw:
            continue

        normalized = normalize_status(raw)

        if normalized != "unknown":
            return normalized, raw, field

    return "unknown", "", ""


def extract_mlb_final_score(feed):
    status_norm, status_raw, status_field = extract_mlb_feed_status(feed)

    if status_norm != "final":
        return {
            "is_final": False,
            "game_status": status_norm,
            "raw_status": status_raw,
            "status_field": status_field,
        }

    try:
        live_data = feed.get("liveData") or {}
        linescore = live_data.get("linescore") or {}
        teams = linescore.get("teams") or {}

        away_score = int(
            (teams.get("away") or {}).get("runs")
        )
        home_score = int(
            (teams.get("home") or {}).get("runs")
        )
    except (TypeError, ValueError):
        return {
            "is_final": False,
            "game_status": "final",
            "raw_status": status_raw,
            "status_field": status_field,
            "score_missing": True,
        }

    game_data = feed.get("gameData") or {}
    team_data = game_data.get("teams") or {}

    api_away_team = str(
        ((team_data.get("away") or {}).get("name")) or ""
    ).strip()

    api_home_team = str(
        ((team_data.get("home") or {}).get("name")) or ""
    ).strip()

    return {
        "is_final": True,
        "game_status": "final",
        "raw_status": status_raw,
        "status_field": status_field,
        "away_score": away_score,
        "home_score": home_score,
        "api_away_team": api_away_team,
        "api_home_team": api_home_team,
    }


def backfill_missing_finals_from_mlb(
    *,
    final_records_by_date,
    seen_by_game_id,
    seen_by_fallback_key,
    status_audit_rows,
    key_audit_rows,
):
    feed_cache = {}
    sportsbook_cache = {}

    games_files_seen = 0
    games_rows_seen = 0
    skipped_already_present = 0
    skipped_missing_ids = 0
    api_checked = 0
    api_errors = 0
    api_not_final = 0
    api_score_missing = 0
    api_team_mismatch = 0
    added = 0
    duplicate_collapsed = 0

    for games_path in sorted(GAMES_DIR.glob("*_games.csv")):
        date = games_date_from_path(games_path)

        if not date:
            continue

        games_files_seen += 1

        if date not in sportsbook_cache:
            sportsbook_cache[date] = load_sportsbook_lookup(date)

        sportsbook_lookup = sportsbook_cache[date]

        with open(
            games_path,
            newline="",
            encoding="utf-8-sig",
        ) as f:
            reader = csv.DictReader(f)

            for row_index, row in enumerate(reader, start=2):
                games_rows_seen += 1

                game_id = str(
                    row.get("game_id", "") or ""
                ).strip()

                gamePk = str(
                    row.get("gamePk", "") or ""
                ).strip()

                gameNumber = str(
                    row.get("gameNumber", "") or ""
                ).strip()

                game_time = str(
                    row.get("game_time", "") or ""
                ).strip()

                home_team = str(
                    row.get("home_team", "") or ""
                ).strip()

                away_team = str(
                    row.get("away_team", "") or ""
                ).strip()

                if not game_id or not gamePk:
                    skipped_missing_ids += 1
                    continue

                if game_id in seen_by_game_id:
                    skipped_already_present += 1
                    continue

                api_checked += 1

                feed = fetch_mlb_game_feed(
                    gamePk,
                    feed_cache,
                )

                if feed is None:
                    api_errors += 1
                    continue

                result = extract_mlb_final_score(feed)

                if not result.get("is_final"):
                    if result.get("score_missing"):
                        api_score_missing += 1

                        log(
                            "MLB FALLBACK FINAL SCORE MISSING | "
                            f"games_file={games_path.name} | "
                            f"row={row_index} | "
                            f"game_id={game_id} | "
                            f"gamePk={gamePk} | "
                            f"away_team={away_team} | "
                            f"home_team={home_team}"
                        )
                    else:
                        api_not_final += 1

                        log(
                            "MLB FALLBACK NOT FINAL | "
                            f"games_file={games_path.name} | "
                            f"row={row_index} | "
                            f"game_id={game_id} | "
                            f"gamePk={gamePk} | "
                            f"status={result.get('raw_status', '')}"
                        )

                    continue

                api_away_team = str(
                    result.get("api_away_team", "") or ""
                ).strip()

                api_home_team = str(
                    result.get("api_home_team", "") or ""
                ).strip()

                if (
                    api_away_team
                    and api_home_team
                    and matchup_key(
                        api_home_team,
                        api_away_team,
                    )
                    != matchup_key(
                        home_team,
                        away_team,
                    )
                ):
                    api_team_mismatch += 1

                    log(
                        "MLB FALLBACK TEAM MISMATCH; SKIPPED | "
                        f"games_file={games_path.name} | "
                        f"row={row_index} | "
                        f"game_id={game_id} | "
                        f"gamePk={gamePk} | "
                        f"local={away_team} @ {home_team} | "
                        f"mlb={api_away_team} @ {api_home_team}"
                    )

                    continue

                away_score = int(result["away_score"])
                home_score = int(result["home_score"])
                final_total = str(away_score + home_score)

                key = matchup_key(
                    home_team,
                    away_team,
                )

                book_candidates = sportsbook_lookup.get(
                    key,
                    [],
                )

                book = closest_time_book_match(
                    book_candidates,
                    game_time,
                    correction_minutes=0,
                    prefer_correction=False,
                )

                record = {
                    "sport": "baseball",
                    "league": "mlb",
                    "game_id": game_id,
                    "gamePk": gamePk,
                    "gameNumber": gameNumber,
                    "game_date": date,
                    "game_time": game_time,
                    "home_team": home_team,
                    "away_team": away_team,
                    "final_away_score": str(away_score),
                    "final_home_score": str(home_score),
                    "final_total": final_total,
                    "away_run_line": book.get("away_run_line"),
                    "home_run_line": book.get("home_run_line"),
                    "total": book.get("total"),
                    "game_status": "final",
                    "final_scores_generated_at": RUN_TS,
                }

                action = add_final_record(
                    record=record,
                    source_file=(
                        "MLB_STATSAPI_"
                        f"gamePk_{gamePk}"
                    ),
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
                    duplicate_collapsed += 1
                else:
                    added += 1

                status_audit_rows.append({
                    "game_date": date,
                    "game_id": game_id,
                    "gamePk": gamePk,
                    "gameNumber": gameNumber,
                    "away_team": away_team,
                    "home_team": home_team,
                    "final_away_score": str(away_score),
                    "final_home_score": str(home_score),
                    "game_status": "final",
                    "status_source": (
                        "MLB StatsAPI gamePk fallback"
                    ),
                    "status_available": "True",
                    "status_notes": (
                        "DRatings/existing finals did not contain "
                        "this game; official MLB final score added "
                        f"using gamePk={gamePk}"
                    ),
                })

                log(
                    "MLB FALLBACK ADDED | "
                    f"game_date={date} | "
                    f"game_id={game_id} | "
                    f"gamePk={gamePk} | "
                    f"gameNumber={gameNumber} | "
                    f"game_time={game_time} | "
                    f"away_team={away_team} | "
                    f"home_team={home_team} | "
                    f"final={away_score}-{home_score}"
                )

    log(
        "MLB FALLBACK SUMMARY | "
        f"games_files_seen={games_files_seen} | "
        f"games_rows_seen={games_rows_seen} | "
        f"skipped_already_present={skipped_already_present} | "
        f"skipped_missing_ids={skipped_missing_ids} | "
        f"api_checked={api_checked} | "
        f"api_errors={api_errors} | "
        f"api_not_final={api_not_final} | "
        f"api_score_missing={api_score_missing} | "
        f"api_team_mismatch={api_team_mismatch} | "
        f"added={added} | "
        f"duplicate_collapsed={duplicate_collapsed}"
    )

    return {
        "games_files_seen": games_files_seen,
        "games_rows_seen": games_rows_seen,
        "skipped_already_present": skipped_already_present,
        "skipped_missing_ids": skipped_missing_ids,
        "api_checked": api_checked,
        "api_errors": api_errors,
        "api_not_final": api_not_final,
        "api_score_missing": api_score_missing,
        "api_team_mismatch": api_team_mismatch,
        "added": added,
        "duplicate_collapsed": duplicate_collapsed,
    }


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
            error=(
                "expected top-level JSON list, found "
                f"{type(data).__name__}"
            ),
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
            f"final_score_dates_accumulated="
            f"{len(final_records_by_date)}"
        )

        return

    for row_index, row in enumerate(data, start=1):
        if not isinstance(row, list):
            parse_errors += 1

            parse_error_rows.append(make_parse_error_row(
                source_file=file_path.name,
                row_index=row_index,
                stage="validate_row_structure",
                error=(
                    "expected row list, found "
                    f"{type(row).__name__}"
                ),
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
                error=(
                    f"expected at least 2 fields, "
                    f"found {len(row)}"
                ),
                row=row,
            ))

            continue

        (
            status_norm,
            raw_status,
            status_source,
            status_available,
        ) = infer_game_status(row)

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
                "status_notes": (
                    "non-final row not written "
                    "to final-score output"
                ),
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
                    "expected team field to be str, found "
                    f"{type(team_value).__name__}"
                )

            teams = team_value.split("\n")

            if len(teams) < 2:
                raise ValueError(
                    "expected at least two team names"
                )

            away_team = clean_team(teams[0])
            home_team = clean_team(teams[1])

            if not away_team or not home_team:
                raise ValueError(
                    "away or home team is blank"
                )

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

        key = matchup_key(
            home_team,
            away_team,
        )

        try:
            score_value = row[5]

            if not isinstance(score_value, str):
                raise TypeError(
                    "expected score field to be str, found "
                    f"{type(score_value).__name__}"
                )

            scores = score_value.split("\n")

            if len(scores) < 2:
                raise ValueError(
                    "expected away/home final scores, found "
                    f"{len(scores)} score field(s)"
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
                games_lookup_cache[game_date] = load_games_lookup(
                    game_date
                )

                games_by_game_id_cache[game_date] = (
                    load_games_by_game_id(game_date)
                )

                games_by_gamepk_cache[game_date] = (
                    load_games_by_gamepk(game_date)
                )

            if game_date not in predictions_lookup_cache:
                predictions_lookup_cache[game_date] = (
                    load_predictions_lookup(game_date)
                )

            if game_date not in sportsbook_lookup_cache:
                sportsbook_lookup_cache[game_date] = (
                    load_sportsbook_lookup(game_date)
                )

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

            game_id = str(
                resolution.get("game_id", "") or ""
            ).strip()

            gamePk = str(
                resolution.get("gamePk", "") or ""
            ).strip()

            gameNumber = str(
                resolution.get("gameNumber", "") or ""
            ).strip()

            scheduled_game_time = str(
                resolution.get(
                    "scheduled_game_time",
                    "",
                )
                or raw_game_time
            ).strip()

            if (
                not resolution.get("resolved")
                or not game_id
                or not gamePk
            ):
                unresolved_rows += 1

                unresolved_completed_rows.append(
                    make_unresolved_completed_row(
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
                        games_candidate_count=resolution.get(
                            "games_candidate_count",
                            0,
                        ),
                        prediction_candidate_count=resolution.get(
                            "prediction_candidate_count",
                            0,
                        ),
                        resolution_reason=resolution.get(
                            "reason",
                            "unresolved",
                        ),
                        raw_row=raw_row_text(row),
                    )
                )

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
                    "status_available": str(
                        status_available
                    ),
                    "status_notes": (
                        "completed game unresolved; excluded "
                        "from final-score output and written to "
                        "unresolved_completed_games.csv"
                    ),
                })

                continue

            book_candidates = book_lookup.get(
                key,
                [],
            )

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
                    "resolved_ids="
                    f"{resolution.get('resolution_source', '')}; "
                    + (
                        "explicit source status available"
                        if status_available
                        else (
                            "status inferred as final from "
                            "completed DRatings row shape"
                        )
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
        f"final_score_dates_accumulated="
        f"{len(final_records_by_date)}"
    )


def migrate_legacy_final_score_files(
    files_written,
    unresolved_completed_rows,
):
    migrated_files = 0
    migrated_rows = 0
    resolved_rows = 0
    unresolved_rows = 0

    for path in sorted(FINAL_DIR.glob("*_final_scores_MLB.csv")):
        with open(
            path,
            newline="",
            encoding="utf-8-sig",
        ) as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)

        if not fieldnames:
            fail(
                f"Legacy final-score file has no header: {path}"
            )

        date = legacy_final_date_from_path(path)

        if not date:
            fail(
                "Could not derive date from legacy "
                f"final-score path: {path}"
            )

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

            record["sport"] = (
                record["sport"]
                or "baseball"
            )

            record["league"] = (
                record["league"]
                or "mlb"
            )

            record["game_date"] = (
                record["game_date"]
                or date
            )

            if (
                not record["game_status"]
                and legacy_row_has_final_score(record)
            ):
                record["game_status"] = "final"
                changed = True

            if (
                not record["final_total"]
                and legacy_row_has_final_score(record)
            ):
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
                    resolution.get(
                        "scheduled_game_time",
                        "",
                    )
                    or ""
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

                    unresolved_completed_rows.append(
                        make_unresolved_completed_row(
                            source_file=path.name,
                            row_index=row_index,
                            game_date=record["game_date"],
                            game_time=record["game_time"],
                            away_team=record["away_team"],
                            home_team=record["home_team"],
                            final_away_score=record[
                                "final_away_score"
                            ],
                            final_home_score=record[
                                "final_home_score"
                            ],
                            game_id=record["game_id"],
                            gamePk=record["gamePk"],
                            gameNumber=record["gameNumber"],
                            games_candidate_count=resolution.get(
                                "games_candidate_count",
                                0,
                            ),
                            prediction_candidate_count=resolution.get(
                                "prediction_candidate_count",
                                0,
                            ),
                            resolution_reason=resolution.get(
                                "reason",
                                (
                                    "legacy completed game "
                                    "could not resolve both IDs"
                                ),
                            ),
                            raw_row=raw_row_text(row),
                        )
                    )

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
                f"file={path.name} | "
                f"rows={len(output_rows)} | "
                f"missing_header_columns="
                f"{missing_header_columns}"
            )

    log(
        "Historical final-score files updated: "
        f"{migrated_files}"
    )

    log(
        "Historical final-score rows retained: "
        f"{migrated_rows}"
    )

    log(
        "Historical completed rows resolved/backfilled: "
        f"{resolved_rows}"
    )

    log(
        "Historical completed rows moved to unresolved audit: "
        f"{unresolved_rows}"
    )

    return {
        "migrated_files": migrated_files,
        "migrated_rows": migrated_rows,
        "resolved_rows": resolved_rows,
        "unresolved_rows": unresolved_rows,
    }


def verify_final_score_outputs_have_gamepk():
    bad_rows = []

    for path in sorted(FINAL_DIR.glob("*_final_scores_MLB.csv")):
        with open(
            path,
            newline="",
            encoding="utf-8-sig",
        ) as f:
            reader = csv.DictReader(f)

            for row_index, row in enumerate(reader, start=2):
                status = str(
                    row.get("game_status", "") or ""
                ).strip().lower()

                if (
                    status != "final"
                    or not legacy_row_has_final_score(row)
                ):
                    continue

                gamePk = str(
                    row.get("gamePk", "") or ""
                ).strip()

                if gamePk:
                    continue

                bad_rows.append({
                    "file": path.name,
                    "row": row_index,
                    "game_id": str(
                        row.get("game_id", "") or ""
                    ).strip(),
                    "game_date": str(
                        row.get("game_date", "") or ""
                    ).strip(),
                    "game_time": str(
                        row.get("game_time", "") or ""
                    ).strip(),
                    "away_team": str(
                        row.get("away_team", "") or ""
                    ).strip(),
                    "home_team": str(
                        row.get("home_team", "") or ""
                    ).strip(),
                })

    if bad_rows:
        sample = bad_rows[:10]

        fail(
            "Completed final-score rows with blank gamePk "
            "remain after backfill; "
            f"bad_rows={len(bad_rows)} "
            f"sample={sample}"
        )

    log(
        "VERIFY: completed final-score output rows "
        "with blank gamePk: 0"
    )


def verify_doubleheader_identity_integrity():
    """
    Verify same-date/same-team multi-game sets cannot collapse.

    Any final row belonging to a doubleheader/multi-game matchup must
    map back to exactly one games row using gamePk, gameNumber, and
    scheduled time.
    """
    doubleheader_matchups = 0
    verified_final_rows = 0
    bad_rows = []

    for games_path in sorted(GAMES_DIR.glob("*_games.csv")):
        date = games_date_from_path(games_path)

        if not date:
            continue

        with open(
            games_path,
            newline="",
            encoding="utf-8-sig",
        ) as f:
            games_rows = list(csv.DictReader(f))

        groups = {}

        for row in games_rows:
            key = matchup_key(
                row.get("home_team", ""),
                row.get("away_team", ""),
            )
            groups.setdefault(key, []).append(row)

        multi_groups = {
            key: rows
            for key, rows in groups.items()
            if len(rows) > 1
        }

        if not multi_groups:
            continue

        final_path = FINAL_DIR / f"{date}_final_scores_MLB.csv"

        if not final_path.exists():
            continue

        with open(
            final_path,
            newline="",
            encoding="utf-8-sig",
        ) as f:
            final_rows = list(csv.DictReader(f))

        for key, candidate_rows in multi_groups.items():
            doubleheader_matchups += 1

            candidate_gamepks = [
                str(row.get("gamePk", "") or "").strip()
                for row in candidate_rows
                if str(row.get("gamePk", "") or "").strip()
            ]

            if len(candidate_gamepks) != len(set(candidate_gamepks)):
                fail(
                    "Duplicate gamePk values exist inside a "
                    "same-date/same-team games group; "
                    f"date={date} matchup={key} "
                    f"gamePks={candidate_gamepks}"
                )

            relevant_finals = [
                row
                for row in final_rows
                if matchup_key(
                    row.get("home_team", ""),
                    row.get("away_team", ""),
                ) == key
                and str(
                    row.get("game_status", "") or ""
                ).strip().lower() == "final"
                and legacy_row_has_final_score(row)
            ]

            seen_final_game_ids = set()
            seen_final_gamepks = set()

            for final_row in relevant_finals:
                game_id = str(
                    final_row.get("game_id", "") or ""
                ).strip()

                gamePk = str(
                    final_row.get("gamePk", "") or ""
                ).strip()

                gameNumber = str(
                    final_row.get("gameNumber", "") or ""
                ).strip()

                game_time = str(
                    final_row.get("game_time", "") or ""
                ).strip()

                if not game_id or not gamePk:
                    bad_rows.append({
                        "date": date,
                        "matchup": key,
                        "game_id": game_id,
                        "gamePk": gamePk,
                        "gameNumber": gameNumber,
                        "game_time": game_time,
                        "reason": "blank game_id/gamePk in multi-game matchup",
                    })
                    continue

                if game_id in seen_final_game_ids:
                    bad_rows.append({
                        "date": date,
                        "matchup": key,
                        "game_id": game_id,
                        "gamePk": gamePk,
                        "gameNumber": gameNumber,
                        "game_time": game_time,
                        "reason": (
                            "same game_id used by multiple finals "
                            "in same-team multi-game matchup"
                        ),
                    })
                    continue

                if gamePk in seen_final_gamepks:
                    bad_rows.append({
                        "date": date,
                        "matchup": key,
                        "game_id": game_id,
                        "gamePk": gamePk,
                        "gameNumber": gameNumber,
                        "game_time": game_time,
                        "reason": (
                            "same gamePk used by multiple finals "
                            "in same-team multi-game matchup"
                        ),
                    })
                    continue

                seen_final_game_ids.add(game_id)
                seen_final_gamepks.add(gamePk)

                gamepk_candidates = [
                    candidate
                    for candidate in candidate_rows
                    if str(
                        candidate.get("gamePk", "") or ""
                    ).strip() == gamePk
                ]

                if len(gamepk_candidates) != 1:
                    bad_rows.append({
                        "date": date,
                        "matchup": key,
                        "game_id": game_id,
                        "gamePk": gamePk,
                        "gameNumber": gameNumber,
                        "game_time": game_time,
                        "reason": (
                            "final gamePk did not map to exactly one "
                            "games candidate"
                        ),
                    })
                    continue

                official = gamepk_candidates[0]

                official_game_id = str(
                    official.get("game_id", "") or ""
                ).strip()

                official_game_number = str(
                    official.get("gameNumber", "") or ""
                ).strip()

                official_game_time = str(
                    official.get("game_time", "") or ""
                ).strip()

                if (
                    official_game_id
                    and official_game_id != game_id
                ):
                    bad_rows.append({
                        "date": date,
                        "matchup": key,
                        "game_id": game_id,
                        "gamePk": gamePk,
                        "gameNumber": gameNumber,
                        "game_time": game_time,
                        "reason": (
                            "final game_id disagreed with the games "
                            "row selected by gamePk"
                        ),
                    })
                    continue

                if (
                    gameNumber
                    and official_game_number
                    and gameNumber != official_game_number
                ):
                    bad_rows.append({
                        "date": date,
                        "matchup": key,
                        "game_id": game_id,
                        "gamePk": gamePk,
                        "gameNumber": gameNumber,
                        "game_time": game_time,
                        "reason": (
                            "final gameNumber disagreed with games "
                            "row selected by gamePk"
                        ),
                    })
                    continue

                diff = time_difference_minutes(
                    game_time,
                    official_game_time,
                )

                if (
                    diff is None
                    or diff > DOUBLEHEADER_TIME_TOLERANCE_MINUTES
                ):
                    bad_rows.append({
                        "date": date,
                        "matchup": key,
                        "game_id": game_id,
                        "gamePk": gamePk,
                        "gameNumber": gameNumber,
                        "game_time": game_time,
                        "reason": (
                            "final scheduled time did not agree with "
                            "games row selected by gamePk/gameNumber"
                        ),
                    })
                    continue

                verified_final_rows += 1

    if bad_rows:
        fail(
            "Doubleheader/multi-game identity verification failed; "
            f"bad_rows={len(bad_rows)} "
            f"sample={bad_rows[:10]}"
        )

    log(
        "VERIFY: doubleheader/multi-game identity integrity passed | "
        f"matchups={doubleheader_matchups} | "
        f"final_rows_verified={verified_final_rows}"
    )


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
        raw_files = sorted(
            RAW_DIR.glob("*_mlb_raw.json")
        )

        if not raw_files:
            fail(
                f"No DRatings raw files found in {RAW_DIR}"
            )

        log(
            f"Raw files found: {len(raw_files)}"
        )

        log(
            "Historical final-score build timestamp: "
            f"{RUN_TS}"
        )

        existing_final_summary = (
            preserve_existing_final_score_records(
                final_records_by_date=final_records_by_date,
                seen_by_game_id=seen_by_game_id,
                seen_by_fallback_key=seen_by_fallback_key,
                status_audit_rows=status_audit_rows,
                key_audit_rows=key_audit_rows,
            )
        )

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

            log(
                "Raw files processed before failure: "
                f"{len(raw_files)}"
            )

            log(
                "Parse errors encountered: "
                f"{total_parse_errors}"
            )

            log_review_rows(
                parse_error_rows,
                unresolved_completed_rows,
            )

            fail(
                "Final-score build aborted because "
                f"parse_errors={total_parse_errors}. "
                "Final-score outputs were not written."
            )

        mlb_fallback_summary = (
            backfill_missing_finals_from_mlb(
                final_records_by_date=final_records_by_date,
                seen_by_game_id=seen_by_game_id,
                seen_by_fallback_key=seen_by_fallback_key,
                status_audit_rows=status_audit_rows,
                key_audit_rows=key_audit_rows,
            )
        )

        for date in sorted(final_records_by_date):
            records = final_records_by_date[date]

            bad_resolved = [
                record
                for record in records
                if (
                    str(
                        record.get(
                            "game_status",
                            "",
                        )
                        or ""
                    ).strip().lower()
                    == "final"
                )
                and (
                    not str(
                        record.get(
                            "game_id",
                            "",
                        )
                        or ""
                    ).strip()
                    or not str(
                        record.get(
                            "gamePk",
                            "",
                        )
                        or ""
                    ).strip()
                )
            ]

            if bad_resolved:
                fail(
                    "Resolved completed rows cannot be written "
                    "with blank game_id/gamePk; "
                    f"date={date} "
                    f"bad_rows={len(bad_resolved)}"
                )

            out = (
                FINAL_DIR
                / f"{date}_final_scores_MLB.csv"
            )

            rows = [
                [
                    record.get(col, "")
                    for col in FINAL_HEADER
                ]
                for record in records
            ]

            write_csv(
                out,
                FINAL_HEADER,
                rows,
                files_written,
                "final scores",
            )

        legacy_backfill_summary = (
            migrate_legacy_final_score_files(
                files_written,
                unresolved_completed_rows,
            )
        )

        verify_final_score_outputs_have_gamepk()
        verify_doubleheader_identity_integrity()

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
            if (
                str(
                    row.get(
                        "game_status",
                        "",
                    )
                ).strip().lower()
                == "unknown"
            )
        )

        total_parse_errors = len(parse_error_rows)

        unresolved_completed_rows_count = len(
            unresolved_completed_rows
        )

        log("--- SUMMARY ---")

        log(
            f"Raw files processed: {len(raw_files)}"
        )

        log(
            f"Files written: {len(files_written)}"
        )

        log(
            "Final-score dates written once: "
            f"{len(final_records_by_date)}"
        )

        log(
            "Final-score game_id primary-key rows: "
            f"{len(seen_by_game_id)}"
        )

        log(
            "Existing valid final-score rows preserved: "
            f"{existing_final_summary['rows_preserved']}"
        )

        log(
            "Existing final rows skipped for missing IDs: "
            f"{existing_final_summary['skipped_missing_ids']}"
        )

        log(
            "MLB fallback games checked: "
            f"{mlb_fallback_summary['api_checked']}"
        )

        log(
            "MLB fallback final rows added: "
            f"{mlb_fallback_summary['added']}"
        )

        log(
            "MLB fallback games not final: "
            f"{mlb_fallback_summary['api_not_final']}"
        )

        log(
            "MLB fallback API errors: "
            f"{mlb_fallback_summary['api_errors']}"
        )

        log(
            "MLB fallback final-score missing: "
            f"{mlb_fallback_summary['api_score_missing']}"
        )

        log(
            "MLB fallback team mismatches: "
            f"{mlb_fallback_summary['api_team_mismatch']}"
        )

        log(
            "Unresolved completed rows: "
            f"{unresolved_completed_rows_count}"
        )

        log(
            "Parse errors encountered: "
            f"{total_parse_errors}"
        )

        log(
            "Unknown status audit rows: "
            f"{unknown_status_count}"
        )

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

        log(
            f"Status audit: {STATUS_AUDIT_FILE}"
        )

        log(
            f"Key audit: {KEY_AUDIT_FILE}"
        )

        log(
            "Unresolved completed-game audit: "
            f"{UNRESOLVED_AUDIT_FILE}"
        )

        if total_parse_errors:
            fail(
                "Final-score build cannot report success because "
                f"parse_errors={total_parse_errors}"
            )
        else:
            log(
                "Parse-error review: "
                "no parse errors encountered."
            )

        if unresolved_completed_rows_count:
            log(
                "WARNING: Genuinely unresolved completed games "
                "were excluded from final-score outputs and "
                f"written to {UNRESOLVED_AUDIT_FILE}."
            )
        else:
            log(
                "Unresolved completed-game review: none."
            )

        for path, count in files_written:
            log(
                f"  FILE: {path} ({count} rows)"
            )

        log_review_rows(
            parse_error_rows,
            unresolved_completed_rows,
        )

        log("STATUS: SUCCESS")

    except Exception as e:
        log(
            f"FATAL ERROR: {e}\n"
            f"{traceback.format_exc()}"
        )

        log("STATUS: FAILED")
        raise

    print("MLB final-score build complete.")


if __name__ == "__main__":
    main()