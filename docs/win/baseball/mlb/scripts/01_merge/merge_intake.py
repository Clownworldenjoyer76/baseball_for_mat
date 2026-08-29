#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/01_merge/merge_intake.py
#
# Step 2 behavior:
#   - Duplicate prediction game_id remains fatal.
#   - Blank game_id in prediction/sportsbook/games remains fatal.
#   - Duplicate sportsbook game_id remains fatal.
#   - Team normalization and cross-source team mismatches remain fatal.
#   - Missing context for matched games remains fatal.
#   - Model-projection input is required for a sportsbook game to enter production output.
#   - Missing or invalid model-projection rows are written to rejection/audit,
#     skipped, and are non-fatal.
#
# Mapping behavior:
#   - Uses docs/win/baseball/mlb/maps/team_map_mlb.csv as the single MLB team map.
#   - Expected map columns:
#       league,team_id,alias,canonical_team
#   - Derives all of these from that one map:
#       alias -> canonical_team
#       team_id -> canonical_team
#       canonical_team -> team_id

import csv
import traceback
import re
import hashlib
import math
from pathlib import Path
from datetime import datetime, timezone

PRED_DIR = Path("docs/win/baseball/mlb/00_intake/predictions/model_projection")
BOOK_DIR = Path("docs/win/baseball/mlb/00_intake/sportsbook")
GAMES_DIR = Path("docs/win/baseball/mlb/00_intake/games")
CONTEXT_DIR = Path("docs/win/baseball/mlb/00_intake/mlb_raw")
OUT_DIR = Path("docs/win/baseball/mlb/01_merge")
LOG_DIR = Path("docs/win/baseball/mlb/errors/01_merge")
AUDIT_DIR = OUT_DIR / "audit"
REJECTION_DIR = OUT_DIR / "rejections"

TEAM_MAP_FILE = Path("docs/win/baseball/mlb/maps/team_map_mlb.csv")

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
REJECTION_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now(timezone.utc).isoformat()
LOG_FILE = LOG_DIR / "merge_intake.txt"
LEAKAGE_AUDIT_DIR = Path("docs/win/baseball/mlb/audit")
LEAKAGE_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
LEAKAGE_AUDIT_FILE = LEAKAGE_AUDIT_DIR / "leakage_audit.csv"
FORBIDDEN_READ_TOKENS = [
    "05_" + "final_scores",
    "final" + "_scores",
    "graded",
    "results",
    "reports",
]  # LEAKAGE_GUARD_ALLOWED_REFERENCE
SCRIPT_NAME = "merge_intake.py"
STAGE_NAME = "01_merge"

with open(LOG_FILE, "w", encoding="utf-8") as f:
    f.write(f"=== merge_intake RUN {RUN_TS} ===\n")


# ─────────────────────────────────────────────
# LOGGING / FAILURE
# ─────────────────────────────────────────────

def _safe_log_text(msg):
    text = str(msg)
    text = text.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")

    text = re.sub(
        r'(?i)\b(password|passwd|pwd|token|secret|api[_-]?key|authorization)\b\s*[:=]\s*([^\s,;]+)',
        r'\1=[REDACTED]',
        text,
    )

    text = re.sub(
        r'(?i)\bbearer\s+[A-Za-z0-9\-\._~\+/]+=*',
        'Bearer [REDACTED]',
        text,
    )

    max_len = 500

    if len(text) > max_len:
        text = text[:max_len] + "...[truncated]"

    return text


def log(msg):
    safe_msg = _safe_log_text(msg)
    msg_hash = hashlib.sha256(
        safe_msg.encode("utf-8")
    ).hexdigest()

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"{datetime.now(timezone.utc).isoformat()} | "
            f"event_hash={msg_hash} | event_len={len(safe_msg)}\n"
        )


def fail(msg):
    log(f"FATAL VALIDATION ERROR: {msg}")
    raise RuntimeError(msg)


def record_file_read(path, path_allowed, reason):
    path = Path(path)
    new_file = not LEAKAGE_AUDIT_FILE.exists()

    with open(
        LEAKAGE_AUDIT_FILE,
        "a",
        encoding="utf-8",
        newline="",
    ) as f:
        if new_file:
            f.write(
                "script,file_read,path_allowed,reason,stage,timestamp\n"
            )

        safe_path = str(path).replace('"', "''")

        f.write(
            f'{SCRIPT_NAME},"{safe_path}",'
            f'{1 if path_allowed else 0},'
            f'"{reason}",{STAGE_NAME},'
            f'{datetime.now(timezone.utc).isoformat()}\n'
        )


def assert_read_path_allowed(path):
    path = Path(path)

    lower_path = (
        str(path)
        .replace("\\", "/")
        .lower()
    )

    matched = [
        token
        for token in FORBIDDEN_READ_TOKENS
        if token in lower_path
    ]

    if matched:
        reason = (
            "forbidden_pre_selection_read:"
            + ";".join(matched)
        )

        record_file_read(
            path,
            False,
            reason,
        )

        fail(
            f"Blocked forbidden pre-selection read path: "
            f"{path} ({reason})"
        )

    record_file_read(
        path,
        True,
        "allowed",
    )


# ─────────────────────────────────────────────
# CSV HELPERS
# ─────────────────────────────────────────────

def duplicate_columns(header):
    seen = set()
    dupes = []

    for col in header:
        if col in seen and col not in dupes:
            dupes.append(col)

        seen.add(col)

    return dupes


def assert_no_duplicate_columns(header, label):
    dupes = duplicate_columns(header)

    if dupes:
        fail(
            f"{label} has duplicate columns: {dupes}"
        )


def assert_required_columns(
    path,
    header,
    required_cols,
    label,
):
    missing = [
        col
        for col in required_cols
        if col not in header
    ]

    if missing:
        fail(
            f"{label} missing required columns "
            f"in {path}: {missing}"
        )


def load_csv(
    path,
    required_cols=None,
    label=None,
    required_file=False,
):
    rows = []

    assert_read_path_allowed(path)

    if not path.exists():
        msg = f"MISSING FILE: {path}"

        if required_file:
            fail(msg)

        log(msg)
        return rows

    with open(
        path,
        newline="",
        encoding="utf-8-sig",
    ) as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []

        assert_no_duplicate_columns(
            header,
            f"{label or path} input",
        )

        if required_cols:
            assert_required_columns(
                path,
                header,
                required_cols,
                label or str(path),
            )

        for r in reader:
            rows.append(r)

    return rows


def write_csv(path, header, rows):
    assert_no_duplicate_columns(
        header,
        f"{path} output",
    )

    expected_width = len(header)
    bad_rows = []

    for i, row in enumerate(
        rows,
        start=1,
    ):
        if len(row) != expected_width:
            bad_rows.append(
                (
                    i,
                    len(row),
                    expected_width,
                )
            )

    if bad_rows:
        fail(
            f"{path} has row/header width mismatch: "
            f"{bad_rows[:10]}"
        )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    log(
        f"WROTE {path} ({len(rows)} rows)"
    )


def write_dict_csv(
    path,
    fieldnames,
    rows,
):
    assert_no_duplicate_columns(
        fieldnames,
        f"{path} output",
    )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()
        writer.writerows(rows)

    log(
        f"WROTE {path} ({len(rows)} rows)"
    )


# ─────────────────────────────────────────────
# OLD OUTPUT CLEANUP
# ─────────────────────────────────────────────

def clear_old_outputs():
    """
    Permanently deletes every root-level CSV in
    docs/win/baseball/mlb/01_merge before rebuilding.

    This intentionally does NOT delete files inside subfolders such as:
      docs/win/baseball/mlb/01_merge/01_merguiced/
      docs/win/baseball/mlb/01_merge/audit/
      docs/win/baseball/mlb/01_merge/rejections/
    """

    old_files = sorted(
        [
            p
            for p in OUT_DIR.glob("*.csv")
            if p.is_file()
        ]
    )

    deleted = 0

    for old_file in old_files:
        old_file.unlink()
        deleted += 1

        log(
            f"DELETED OLD ROOT MERGE OUTPUT: "
            f"{old_file}"
        )

    remaining = sorted(
        [
            p
            for p in OUT_DIR.glob("*.csv")
            if p.is_file()
        ]
    )

    if remaining:
        remaining_text = ", ".join(
            str(p)
            for p in remaining
        )

        raise RuntimeError(
            "FAILED TO DELETE ALL ROOT MERGE CSV OUTPUTS. "
            f"Remaining files: {remaining_text}"
        )

    log(
        "OLD ROOT MERGE CSV OUTPUTS "
        f"PERMANENTLY DELETED: {deleted}"
    )

    log(
        "CONFIRMED: docs/win/baseball/mlb/01_merge "
        "has zero root-level CSV files before rebuild"
    )


def clear_old_audit_and_rejection_outputs():
    deleted = 0

    for folder in [
        AUDIT_DIR,
        REJECTION_DIR,
    ]:
        for old_file in sorted(
            [
                p
                for p in folder.glob("*.csv")
                if p.is_file()
            ]
        ):
            old_file.unlink()
            deleted += 1

            log(
                f"DELETED OLD SUPPORT OUTPUT: "
                f"{old_file}"
            )

    log(
        "OLD AUDIT/REJECTION CSV OUTPUTS "
        f"DELETED: {deleted}"
    )


# ─────────────────────────────────────────────
# TEAM MAPS
# ─────────────────────────────────────────────

def _clean(value):
    return (
        value or ""
    ).strip()


def _key(value):
    return _clean(
        value
    ).lower()


def load_team_maps():
    rows = load_csv(
        TEAM_MAP_FILE,
        required_cols=[
            "league",
            "team_id",
            "alias",
            "canonical_team",
        ],
        label="team_map_mlb",
        required_file=True,
    )

    alias_map = {}
    team_id_to_canonical = {}
    canonical_to_team_id = {}
    duplicate_identical_alias_rows = 0

    for row_num, row in enumerate(
        rows,
        start=2,
    ):
        league = _key(
            row.get("league")
        )

        team_id = _clean(
            row.get("team_id")
        )

        alias_raw = _clean(
            row.get("alias")
        )

        alias = _key(
            alias_raw
        )

        canonical = _clean(
            row.get("canonical_team")
        )

        if (
            not league
            or not team_id
            or not alias
            or not canonical
        ):
            fail(
                "team_map_mlb has blank required value "
                f"at csv_row={row_num}: "
                f"league={league!r} "
                f"team_id={team_id!r} "
                f"alias={alias_raw!r} "
                f"canonical_team={canonical!r}"
            )

        if league != "mlb":
            continue

        existing_alias = alias_map.get(
            alias
        )

        if (
            existing_alias
            and existing_alias != canonical
        ):
            fail(
                "team_map_mlb alias maps to multiple canonical teams: "
                f"csv_row={row_num} "
                f"alias={alias_raw} "
                f"existing={existing_alias} "
                f"new={canonical}"
            )

        if existing_alias == canonical:
            duplicate_identical_alias_rows += 1

        alias_map[
            alias
        ] = canonical

        existing_canonical_for_id = (
            team_id_to_canonical.get(
                team_id
            )
        )

        if (
            existing_canonical_for_id
            and existing_canonical_for_id != canonical
        ):
            fail(
                "team_map_mlb team_id maps to multiple canonical teams: "
                f"csv_row={row_num} "
                f"team_id={team_id} "
                f"existing={existing_canonical_for_id} "
                f"new={canonical}"
            )

        existing_id_for_canonical = (
            canonical_to_team_id.get(
                canonical
            )
        )

        if (
            existing_id_for_canonical
            and existing_id_for_canonical != team_id
        ):
            fail(
                "team_map_mlb canonical_team maps to multiple team_ids: "
                f"csv_row={row_num} "
                f"canonical_team={canonical} "
                f"existing={existing_id_for_canonical} "
                f"new={team_id}"
            )

        team_id_to_canonical[
            team_id
        ] = canonical

        canonical_to_team_id[
            canonical
        ] = team_id

    if not alias_map:
        fail(
            f"No MLB alias mappings loaded from "
            f"{TEAM_MAP_FILE}"
        )

    if not team_id_to_canonical:
        fail(
            f"No MLB team_id mappings loaded from "
            f"{TEAM_MAP_FILE}"
        )

    log(
        f"Team map loaded from {TEAM_MAP_FILE}: "
        f"aliases={len(alias_map)} "
        f"team_ids={len(team_id_to_canonical)} "
        f"canonical_teams={len(canonical_to_team_id)} "
        f"duplicate_identical_alias_rows="
        f"{duplicate_identical_alias_rows}"
    )

    return (
        alias_map,
        team_id_to_canonical,
        canonical_to_team_id,
    )


def normalize_team_name(
    raw_name,
    alias_map,
    label,
):
    name = _clean(
        raw_name
    )

    if not name:
        fail(
            f"Blank team name for {label}"
        )

    canonical = alias_map.get(
        _key(name)
    )

    if not canonical:
        fail(
            f"No team_map_mlb mapping for "
            f"{label}: {name}"
        )

    return canonical


def canonical_from_team_id(
    team_id,
    team_id_to_canonical,
    label,
):
    team_id = _clean(
        team_id
    )

    if not team_id:
        fail(
            f"Blank team_id for {label}"
        )

    canonical = (
        team_id_to_canonical.get(
            team_id
        )
    )

    if not canonical:
        fail(
            f"No team_map_mlb team_id entry "
            f"for {label}: team_id={team_id}"
        )

    return canonical


# ─────────────────────────────────────────────
# REQUIRED INPUT SCHEMAS
# ─────────────────────────────────────────────

REQUIRED_PRED_COLS = [
    "game_id",
    "home_team",
    "away_team",
    "game_time",
    "home_pitcher",
    "away_pitcher",
    "dratings_home_prob",
    "dratings_away_prob",
    "dratings_home_projected_runs",
    "dratings_away_projected_runs",
    "dratings_total_projected_runs",
    "model_home_runs",
    "model_away_runs",
    "model_total_runs",
    "run_model_version",
    "run_model_feature_status",
]

MODEL_PROJECTION_OUTPUT_COLS = [
    "dratings_home_prob",
    "dratings_away_prob",
    "dratings_home_projected_runs",
    "dratings_away_projected_runs",
    "dratings_total_projected_runs",
    "model_home_runs",
    "model_away_runs",
    "model_total_runs",
    "run_model_version",
    "run_model_feature_status",
]

REQUIRED_BOOK_COLS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "away_run_line",
    "home_run_line",
    "total",
    "away_dk_moneyline_american",
    "home_dk_moneyline_american",
    "away_dk_moneyline_decimal",
    "home_dk_moneyline_decimal",
    "away_dk_run_line_american",
    "home_dk_run_line_american",
    "away_dk_run_line_decimal",
    "home_dk_run_line_decimal",
    "dk_total_over_american",
    "dk_total_under_american",
    "dk_total_over_decimal",
    "dk_total_under_decimal",
]

REQUIRED_GAMES_COLS = [
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
]

REQUIRED_CONTEXT_COLS = [
    "gamePk",
    "home_team_id",
    "away_team_id",
    "venue_id",
]


# ─────────────────────────────────────────────
# GAME CONTEXT LOADING
# ─────────────────────────────────────────────

CONTEXT_COLS = [
    "gamePk",
    "home_team_id",
    "away_team_id",
    "venue_id",
    "roof_type",
    "turf_type",
    "home_pitcher_id",
    "away_pitcher_id",
    "home_pitcher_hand",
    "away_pitcher_hand",
    "home_sp_xwoba",
    "away_sp_xwoba",
    "home_sp_k_pct",
    "away_sp_k_pct",
    "home_sp_bb_pct",
    "away_sp_bb_pct",
    "home_sp_barrel_pct",
    "away_sp_barrel_pct",
    "home_sp_whiff_pct",
    "away_sp_whiff_pct",
    "home_sp_sample_flag",
    "away_sp_sample_flag",
    "home_batters_found",
    "away_batters_found",
    "home_sp_found",
    "away_sp_found",
    "home_lineup_xwoba",
    "home_lineup_barrel_pct",
    "home_lineup_hard_hit_pct",
    "home_lineup_k_pct",
    "home_lineup_bb_pct",
    "home_lineup_exit_velo",
    "home_lineup_frv",
    "home_lineup_brv",
    "home_catcher_framing",
    "home_low_sample_count",
    "home_n_left",
    "home_n_right",
    "home_n_switch",
    "away_lineup_xwoba",
    "away_lineup_barrel_pct",
    "away_lineup_hard_hit_pct",
    "away_lineup_k_pct",
    "away_lineup_bb_pct",
    "away_lineup_exit_velo",
    "away_lineup_frv",
    "away_lineup_brv",
    "away_catcher_framing",
    "away_low_sample_count",
    "away_n_left",
    "away_n_right",
    "away_n_switch",
    "park_factor",
    "park_wOBAcon",
    "park_xwOBAcon",
    "park_HR",
    "park_R",
    "park_factor_B",
    "park_wOBAcon_B",
    "park_xwOBAcon_B",
    "park_HR_B",
    "park_R_B",
    "weather_applicable",
    "weather_time",
    "temp_f",
    "wind_mph",
    "wind_dir",
    "precip_in",
    "humidity",
    "will_it_rain",
    "wind_blowing_out",
    "air_pressure_at_sea_level",
    "dew_point_f",
    "symbol_code",
    "sp_data_available",
    "lineup_data_available",
]

NULL_CONTEXT = {
    col: ""
    for col in CONTEXT_COLS
}


def build_unique_index(
    rows,
    key_col,
    date,
    label,
    blank_is_fatal=True,
):
    idx = {}
    blanks = 0
    duplicates = []

    for row_num, row in enumerate(
        rows,
        start=2,
    ):
        key_value = _clean(
            row.get(key_col)
        )

        if not key_value:
            blanks += 1

            msg = (
                f"{date} | {label} row has blank "
                f"{key_col} at csv_row={row_num}: "
                f"{row}"
            )

            log(msg)

            if blank_is_fatal:
                duplicates.append(msg)

            continue

        if key_value in idx:
            duplicates.append(
                f"{date} | {label} duplicate "
                f"{key_col}={key_value}; "
                f"previous_row="
                f"{idx[key_value].get('_csv_row', '')} "
                f"duplicate_row={row_num}"
            )

        else:
            row["_csv_row"] = row_num
            idx[key_value] = row

    if duplicates:
        fail(
            f"{date} | {label} unique-index "
            f"validation failed: {duplicates[:20]}"
        )

    log(
        f"{date} | {label} unique index built "
        f"on {key_col}: indexed={len(idx)} "
        f"blanks={blanks}"
    )

    return idx


def load_games_index(
    date,
    alias_map,
    team_id_to_canonical,
):
    path = (
        GAMES_DIR
        / f"{date}_games.csv"
    )

    rows = load_csv(
        path,
        REQUIRED_GAMES_COLS,
        "games input",
        required_file=True,
    )

    game_id_to_row = {}
    game_id_to_game_pk = {}
    game_pk_to_game_id = {}

    game_id_to_game_pks = {}
    game_pk_to_game_ids = {}

    for row_num, row in enumerate(
        rows,
        start=2,
    ):
        game_id = _clean(
            row.get("game_id")
        )

        game_pk = _clean(
            row.get("gamePk")
        )

        if not game_id:
            fail(
                f"{date} | games row has blank "
                f"game_id at csv_row={row_num}: {row}"
            )

        if not game_pk:
            fail(
                f"{date} | games row has blank "
                f"gamePk at csv_row={row_num}: {row}"
            )

        game_id_to_game_pks.setdefault(
            game_id,
            set(),
        ).add(
            game_pk
        )

        game_pk_to_game_ids.setdefault(
            game_pk,
            set(),
        ).add(
            game_id
        )

        if game_id in game_id_to_row:
            fail(
                f"{date} | games duplicate "
                f"game_id={game_id}"
            )

        if game_pk in game_pk_to_game_id:
            fail(
                f"{date} | games duplicate "
                f"gamePk={game_pk}"
            )

        home_norm = normalize_team_name(
            row.get("home_team"),
            alias_map,
            f"games.home_team game_id={game_id}",
        )

        away_norm = normalize_team_name(
            row.get("away_team"),
            alias_map,
            f"games.away_team game_id={game_id}",
        )

        home_id_norm = canonical_from_team_id(
            row.get("home_team_id"),
            team_id_to_canonical,
            f"games.home_team_id game_id={game_id}",
        )

        away_id_norm = canonical_from_team_id(
            row.get("away_team_id"),
            team_id_to_canonical,
            f"games.away_team_id game_id={game_id}",
        )

        if home_norm != home_id_norm:
            fail(
                f"{date} | games home team/name ID mismatch "
                f"game_id={game_id}: "
                f"home_team={home_norm} "
                f"home_team_id={row.get('home_team_id')} "
                f"id_team={home_id_norm}"
            )

        if away_norm != away_id_norm:
            fail(
                f"{date} | games away team/name ID mismatch "
                f"game_id={game_id}: "
                f"away_team={away_norm} "
                f"away_team_id={row.get('away_team_id')} "
                f"id_team={away_id_norm}"
            )

        row["_csv_row"] = row_num
        row["_home_team_norm"] = home_norm
        row["_away_team_norm"] = away_norm

        game_id_to_row[
            game_id
        ] = row

        game_id_to_game_pk[
            game_id
        ] = game_pk

        game_pk_to_game_id[
            game_pk
        ] = game_id

    multi_pk = {
        game_id: sorted(game_pks)
        for game_id, game_pks in game_id_to_game_pks.items()
        if len(game_pks) > 1
    }

    multi_game_id = {
        game_pk: sorted(game_ids)
        for game_pk, game_ids in game_pk_to_game_ids.items()
        if len(game_ids) > 1
    }

    if multi_pk:
        fail(
            f"{date} | one game_id maps to more "
            f"than one gamePk: {multi_pk}"
        )

    if multi_game_id:
        fail(
            f"{date} | one gamePk maps to more "
            f"than one game_id: {multi_game_id}"
        )

    log(
        f"{date} | games rows loaded={len(rows)} "
        f"indexed_game_ids={len(game_id_to_row)}"
    )

    return (
        rows,
        game_id_to_row,
        game_id_to_game_pk,
        game_pk_to_game_id,
    )


def load_context_index(
    date,
    team_id_to_canonical,
):
    path = (
        CONTEXT_DIR
        / f"{date}_game_context.csv"
    )

    rows = load_csv(
        path,
        REQUIRED_CONTEXT_COLS,
        "game_context input",
        required_file=True,
    )

    idx = {}

    for row_num, row in enumerate(
        rows,
        start=2,
    ):
        game_pk = _clean(
            row.get("gamePk")
        )

        if not game_pk:
            fail(
                f"{date} | context row has blank "
                f"gamePk at csv_row={row_num}: {row}"
            )

        if game_pk in idx:
            fail(
                f"{date} | context duplicate "
                f"gamePk={game_pk}"
            )

        canonical_from_team_id(
            row.get("home_team_id"),
            team_id_to_canonical,
            f"context.home_team_id gamePk={game_pk}",
        )

        canonical_from_team_id(
            row.get("away_team_id"),
            team_id_to_canonical,
            f"context.away_team_id gamePk={game_pk}",
        )

        idx[
            game_pk
        ] = {
            col: row.get(col, "")
            for col in CONTEXT_COLS
        }

        idx[
            game_pk
        ][
            "_csv_row"
        ] = row_num

    log(
        f"{date} | context rows loaded={len(rows)} "
        f"indexed_gamePks={len(idx)}"
    )

    return (
        rows,
        idx,
    )


# ─────────────────────────────────────────────
# BETTING HELPERS
# ─────────────────────────────────────────────

def american_to_prob(odds):
    try:
        odds = float(odds)

        if odds > 0:
            return (
                100
                / (
                    odds
                    + 100
                )
            )

        return (
            -odds
            / (
                -odds
                + 100
            )
        )

    except Exception:
        return None


def normalize_probs(p1, p2):
    if (
        p1 is None
        or p2 is None
    ):
        return (
            "",
            "",
        )

    total = (
        p1
        + p2
    )

    if total == 0:
        return (
            "",
            "",
        )

    return (
        str(
            p1
            / total
        ),
        str(
            p2
            / total
        ),
    )


# ─────────────────────────────────────────────
# AUDIT / VALIDATION HELPERS
# ─────────────────────────────────────────────

def add_audit_row(
    audit_rows,
    date,
    game_id,
    away_team,
    home_team,
    pred,
    book,
    games,
    context,
    model_projection,
    status,
):
    audit_rows.append(
        {
            "date": date,
            "game_id": game_id,
            "away_team": away_team,
            "home_team": home_team,
            "source_present_pred":
                "1" if pred else "0",
            "source_present_model_projection":
                "1" if model_projection else "0",
            "source_present_book":
                "1" if book else "0",
            "source_present_games":
                "1" if games else "0",
            "source_present_context":
                "1" if context else "0",
            "status": status,
        }
    )


def choose_team_for_audit(
    pred_row,
    book_row,
    games_row,
    side,
):
    for row in [
        book_row,
        pred_row,
        games_row,
    ]:
        if row:
            value = _clean(
                row.get(
                    f"_{side}_team_norm"
                )
                or row.get(
                    f"{side}_team"
                )
            )

            if value:
                return value

    return ""


def validate_model_projection_row(
    date,
    game_id,
    pred_row,
):
    """
    Return an empty string for a valid model-projection row.

    Row-level projection problems are non-fatal. The caller records the reason,
    excludes the row from production merging, and continues processing the slate.
    Structural file/schema failures are still handled elsewhere as fatal errors.
    """

    missing = [
        col
        for col in MODEL_PROJECTION_OUTPUT_COLS
        if not _clean(
            pred_row.get(col)
        )
    ]

    if missing:
        return (
            "model_projection_missing_required_values:"
            + ",".join(missing)
        )

    try:
        model_home = float(
            pred_row[
                "model_home_runs"
            ]
        )

        model_away = float(
            pred_row[
                "model_away_runs"
            ]
        )

        model_total = float(
            pred_row[
                "model_total_runs"
            ]
        )

    except Exception:
        return (
            "model_projection_run_values_nonnumeric"
        )

    if not all(
        math.isfinite(v)
        for v in [
            model_home,
            model_away,
            model_total,
        ]
    ):
        return (
            "model_projection_run_values_nonfinite"
        )

    if (
        model_home < 0
        or model_away < 0
        or model_total < 0
    ):
        return (
            "model_projection_run_values_negative:"
            f"home={model_home},"
            f"away={model_away},"
            f"total={model_total}"
        )

    if abs(
        model_total
        - (
            model_home
            + model_away
        )
    ) > 1e-9:
        return (
            "model_projection_total_mismatch:"
            f"home={model_home},"
            f"away={model_away},"
            f"total={model_total}"
        )

    return ""


def validate_market_outputs(
    date,
    ml_rows,
    rl_rows,
    tot_rows,
):
    counts = {
        "moneyline":
            len(ml_rows),
        "run_line":
            len(rl_rows),
        "total":
            len(tot_rows),
    }

    if len(
        set(
            counts.values()
        )
    ) != 1:
        fail(
            f"{date} | merged market "
            f"game-count mismatch: {counts}"
        )

    ml_ids = {
        row[1]
        for row in ml_rows
    }

    rl_ids = {
        row[1]
        for row in rl_rows
    }

    tot_ids = {
        row[1]
        for row in tot_rows
    }

    if (
        ml_ids != rl_ids
        or ml_ids != tot_ids
    ):
        fail(
            f"{date} | merged market game_id set mismatch: "
            f"moneyline_only="
            f"{sorted(ml_ids - rl_ids - tot_ids)} "
            f"run_line_only="
            f"{sorted(rl_ids - ml_ids - tot_ids)} "
            f"total_only="
            f"{sorted(tot_ids - ml_ids - rl_ids)}"
        )

    for market, rows in [
        (
            "moneyline",
            ml_rows,
        ),
        (
            "run_line",
            rl_rows,
        ),
        (
            "total",
            tot_rows,
        ),
    ]:
        blank_ids = [
            i
            for i, row in enumerate(
                rows,
                start=1,
            )
            if not _clean(
                row[1]
            )
        ]

        if blank_ids:
            fail(
                f"{date} | {market} merged output "
                f"has blank game_id rows: "
                f"{blank_ids[:20]}"
            )

    log(
        f"{date} | merged market validation passed: "
        f"counts={counts} "
        f"game_ids={len(ml_ids)}"
    )


def validate_cross_source_teams(
    date,
    game_id,
    pred_row,
    book_row,
    games_row,
    context_row,
    team_id_to_canonical,
    errors,
):
    if (
        pred_row[
            "_home_team_norm"
        ]
        != book_row[
            "_home_team_norm"
        ]
    ):
        errors.append(
            f"{date} | home team mismatch pred/book "
            f"game_id={game_id}: "
            f"pred={pred_row['_home_team_norm']} "
            f"book={book_row['_home_team_norm']}"
        )

    if (
        pred_row[
            "_away_team_norm"
        ]
        != book_row[
            "_away_team_norm"
        ]
    ):
        errors.append(
            f"{date} | away team mismatch pred/book "
            f"game_id={game_id}: "
            f"pred={pred_row['_away_team_norm']} "
            f"book={book_row['_away_team_norm']}"
        )

    if games_row:
        if (
            book_row[
                "_home_team_norm"
            ]
            != games_row[
                "_home_team_norm"
            ]
        ):
            errors.append(
                f"{date} | home team mismatch book/games "
                f"game_id={game_id}: "
                f"book={book_row['_home_team_norm']} "
                f"games={games_row['_home_team_norm']}"
            )

        if (
            book_row[
                "_away_team_norm"
            ]
            != games_row[
                "_away_team_norm"
            ]
        ):
            errors.append(
                f"{date} | away team mismatch book/games "
                f"game_id={game_id}: "
                f"book={book_row['_away_team_norm']} "
                f"games={games_row['_away_team_norm']}"
            )

    if (
        games_row
        and context_row
    ):
        context_home_id = _clean(
            context_row.get(
                "home_team_id"
            )
        )

        context_away_id = _clean(
            context_row.get(
                "away_team_id"
            )
        )

        games_home_id = _clean(
            games_row.get(
                "home_team_id"
            )
        )

        games_away_id = _clean(
            games_row.get(
                "away_team_id"
            )
        )

        if (
            context_home_id
            != games_home_id
        ):
            errors.append(
                f"{date} | context/games home_team_id mismatch "
                f"game_id={game_id} "
                f"gamePk={games_row.get('gamePk')}: "
                f"context={context_home_id} "
                f"games={games_home_id}"
            )

        if (
            context_away_id
            != games_away_id
        ):
            errors.append(
                f"{date} | context/games away_team_id mismatch "
                f"game_id={game_id} "
                f"gamePk={games_row.get('gamePk')}: "
                f"context={context_away_id} "
                f"games={games_away_id}"
            )

        context_home_team = (
            team_id_to_canonical.get(
                context_home_id,
                "",
            )
        )

        context_away_team = (
            team_id_to_canonical.get(
                context_away_id,
                "",
            )
        )

        if (
            context_home_team
            != book_row[
                "_home_team_norm"
            ]
        ):
            errors.append(
                f"{date} | context home team mismatch "
                f"game_id={game_id}: "
                f"context_id={context_home_id} "
                f"context_team={context_home_team} "
                f"book={book_row['_home_team_norm']}"
            )

        if (
            context_away_team
            != book_row[
                "_away_team_norm"
            ]
        ):
            errors.append(
                f"{date} | context away team mismatch "
                f"game_id={game_id}: "
                f"context_id={context_away_id} "
                f"context_team={context_away_team} "
                f"book={book_row['_away_team_norm']}"
            )


def log_dropped_game_investigation(
    date,
    pred_idx,
    book_idx,
    games_idx,
    context_idx,
):
    target_date = (
        "2026_04_03"
    )

    target_game_id = (
        "e2025bc46ada7b7e499744ffe0251d13"
    )

    if date != target_date:
        return

    pred_row = pred_idx.get(
        target_game_id
    )

    book_row = book_idx.get(
        target_game_id
    )

    games_row = games_idx.get(
        target_game_id
    )

    game_pk = (
        _clean(
            games_row.get("gamePk")
        )
        if games_row
        else ""
    )

    context_row = (
        context_idx.get(
            game_pk
        )
        if game_pk
        else None
    )

    log(
        f"{date} | DROPPED GAME INVESTIGATION "
        f"game_id={target_game_id} "
        f"pred_present={1 if pred_row else 0} "
        f"book_present={1 if book_row else 0} "
        f"games_present={1 if games_row else 0} "
        f"gamePk={game_pk} "
        f"context_present={1 if context_row else 0} "
        f"away=Milwaukee Brewers "
        f"home=Kansas City Royals"
    )


# ─────────────────────────────────────────────
# PROCESS ONE DATE
# ─────────────────────────────────────────────

def process_date(
    date,
    summary,
    alias_map,
    team_id_to_canonical,
):
    pred_path = (
        PRED_DIR
        / f"{date}_MLB.csv"
    )

    book_path = (
        BOOK_DIR
        / f"{date}_MLB.csv"
    )

    preds = load_csv(
        pred_path,
        REQUIRED_PRED_COLS,
        "model projection input",
        required_file=True,
    )

    books = load_csv(
        book_path,
        REQUIRED_BOOK_COLS,
        "sportsbook input",
    )

    if not preds:
        log(
            f"SKIP {date}: "
            "no model projection rows"
        )

        summary[
            "skipped"
        ] += 1

        return

    if not books:
        log(
            f"SKIP {date}: "
            "no sportsbook"
        )

        summary[
            "skipped"
        ] += 1

        return

    (
        games_rows,
        games_idx,
        games_game_id_to_pk,
        games_pk_to_game_id,
    ) = load_games_index(
        date,
        alias_map,
        team_id_to_canonical,
    )

    (
        context_rows,
        context_idx,
    ) = load_context_index(
        date,
        team_id_to_canonical,
    )

    pred_source_idx = {}
    pred_idx = {}
    invalid_pred_idx = {}
    book_idx = {}

    for row_num, p in enumerate(
        preds,
        start=2,
    ):
        game_id = _clean(
            p.get("game_id")
        )

        if not game_id:
            fail(
                f"{date} | model projection row "
                f"has blank game_id at "
                f"csv_row={row_num}: {p}"
            )

        if game_id in pred_source_idx:
            fail(
                f"{date} | duplicate model "
                f"projection game_id={game_id}"
            )

        p[
            "_csv_row"
        ] = row_num

        p[
            "_home_team_norm"
        ] = normalize_team_name(
            p.get("home_team"),
            alias_map,
            (
                "model_projection.home_team "
                f"game_id={game_id}"
            ),
        )

        p[
            "_away_team_norm"
        ] = normalize_team_name(
            p.get("away_team"),
            alias_map,
            (
                "model_projection.away_team "
                f"game_id={game_id}"
            ),
        )

        pred_source_idx[
            game_id
        ] = p

        reject_reason = (
            validate_model_projection_row(
                date,
                game_id,
                p,
            )
        )

        if reject_reason:
            p[
                "_model_projection_reject_reason"
            ] = reject_reason

            invalid_pred_idx[
                game_id
            ] = p

            log(
                f"{date} | invalid model projection "
                f"row skipped "
                f"game_id={game_id} "
                f"csv_row={row_num} "
                f"reason={reject_reason}"
            )

            continue

        pred_idx[
            game_id
        ] = p

    for row_num, b in enumerate(
        books,
        start=2,
    ):
        game_id = _clean(
            b.get("game_id")
        )

        if not game_id:
            fail(
                f"{date} | sportsbook row "
                f"has blank game_id at "
                f"csv_row={row_num}: {b}"
            )

        if game_id in book_idx:
            fail(
                f"{date} | duplicate sportsbook "
                f"game_id={game_id}; "
                "one prediction row maps to more "
                "than one sportsbook row"
            )

        b[
            "_csv_row"
        ] = row_num

        b[
            "_home_team_norm"
        ] = normalize_team_name(
            b.get("home_team"),
            alias_map,
            (
                "sportsbook.home_team "
                f"game_id={game_id}"
            ),
        )

        b[
            "_away_team_norm"
        ] = normalize_team_name(
            b.get("away_team"),
            alias_map,
            (
                "sportsbook.away_team "
                f"game_id={game_id}"
            ),
        )

        book_idx[
            game_id
        ] = b

    log_dropped_game_investigation(
        date,
        pred_source_idx,
        book_idx,
        games_idx,
        context_idx,
    )

    pred_source_ids = set(
        pred_source_idx
    )

    pred_ids = set(
        pred_idx
    )

    invalid_model_projection_ids = set(
        invalid_pred_idx
    )

    book_ids = set(
        book_idx
    )

    games_ids = set(
        games_idx
    )

    matched_ids = (
        book_ids
        & pred_ids
    )

    unmatched_prediction_ids = (
        pred_ids
        - book_ids
    )

    unmatched_sportsbook_ids = (
        book_ids
        - pred_ids
    )

    unmatched_games_ids = (
        games_ids
        - matched_ids
    )

    rejection_rows = []

    for game_id in sorted(
        unmatched_sportsbook_ids
    ):
        row = dict(
            book_idx[
                game_id
            ]
        )

        row[
            "date"
        ] = date

        invalid_pred = (
            invalid_pred_idx.get(
                game_id
            )
        )

        if invalid_pred is not None:
            row[
                "reject_reason"
            ] = (
                "invalid_model_projection:"
                + _clean(
                    invalid_pred.get(
                        "_model_projection_reject_reason"
                    )
                )
            )

        else:
            row[
                "reject_reason"
            ] = (
                "sportsbook_game_id_not_found_"
                "in_model_projection"
            )

        rejection_rows.append(
            row
        )

    rejection_path = (
        REJECTION_DIR
        / f"{date}_unmatched_sportsbook_rows.csv"
    )

    rejection_header = [
        "date",
        "reject_reason",
    ] + REQUIRED_BOOK_COLS

    if rejection_rows:
        write_dict_csv(
            rejection_path,
            rejection_header,
            rejection_rows,
        )

    else:
        log(
            f"{date} | no unmatched sportsbook "
            "rejection rows; "
            "no rejection CSV written"
        )

    audit_rows = []

    all_audit_ids = sorted(
        pred_source_ids
        | book_ids
        | games_ids
    )

    missing_context_ids = set()

    game_id_to_game_pk_missing_context_count = 0

    for game_id in all_audit_ids:
        pred_row = (
            pred_source_idx.get(
                game_id
            )
        )

        book_row = (
            book_idx.get(
                game_id
            )
        )

        games_row = (
            games_idx.get(
                game_id
            )
        )

        game_pk = (
            _clean(
                games_row.get(
                    "gamePk"
                )
            )
            if games_row
            else ""
        )

        context_row = (
            context_idx.get(
                game_pk
            )
            if game_pk
            else None
        )

        if (
            games_row
            and not game_pk
        ):
            game_id_to_game_pk_missing_context_count += 1

        if (
            games_row
            and game_pk
            and context_row is None
            and game_id in matched_ids
        ):
            missing_context_ids.add(
                game_id
            )

        statuses = []

        if game_id in matched_ids:
            statuses.append(
                "merged"
            )

        if game_id in invalid_model_projection_ids:
            statuses.append(
                "invalid_model_projection"
            )

        if game_id in unmatched_prediction_ids:
            statuses.append(
                "unmatched_prediction"
            )

        if game_id in unmatched_sportsbook_ids:
            statuses.append(
                "unmatched_sportsbook"
            )

        if game_id in unmatched_games_ids:
            statuses.append(
                "unmatched_games"
            )

        if game_id in missing_context_ids:
            statuses.append(
                "missing_context"
            )

        if not statuses:
            statuses.append(
                "audit_only"
            )

        add_audit_row(
            audit_rows=audit_rows,
            date=date,
            game_id=game_id,
            away_team=choose_team_for_audit(
                pred_row,
                book_row,
                games_row,
                "away",
            ),
            home_team=choose_team_for_audit(
                pred_row,
                book_row,
                games_row,
                "home",
            ),
            pred=pred_row is not None,
            book=book_row is not None,
            games=games_row is not None,
            context=context_row is not None,
            model_projection=(
                game_id
                in pred_ids
            ),
            status=";".join(
                statuses
            ),
        )

    audit_path = (
        AUDIT_DIR
        / f"{date}_merge_audit.csv"
    )

    audit_header = [
        "date",
        "game_id",
        "away_team",
        "home_team",
        "source_present_pred",
        "source_present_model_projection",
        "source_present_book",
        "source_present_games",
        "source_present_context",
        "status",
    ]

    write_dict_csv(
        audit_path,
        audit_header,
        audit_rows,
    )

    log(
        f"{date} | ROW RECONCILIATION "
        f"sportsbook_rows={len(books)} "
        f"model_projection_rows={len(preds)} "
        f"games_rows={len(games_rows)} "
        f"context_rows={len(context_rows)} "
        f"merge_rows={len(matched_ids)} "
        f"invalid_model_projection_rows="
        f"{len(invalid_model_projection_ids)} "
        f"unmatched_model_projection_rows="
        f"{len(unmatched_prediction_ids)} "
        f"unmatched_sportsbook_rows="
        f"{len(unmatched_sportsbook_ids)} "
        f"unmatched_games_rows="
        f"{len(unmatched_games_ids)} "
        f"missing_context_rows="
        f"{len(missing_context_ids)} "
        f"game_id_to_gamePk_missing_context_count="
        f"{game_id_to_game_pk_missing_context_count}"
    )

    summary[
        "total_prediction_rows"
    ] += len(preds)

    summary[
        "total_sportsbook_rows"
    ] += len(books)

    summary[
        "total_games_rows"
    ] += len(games_rows)

    summary[
        "total_context_rows"
    ] += len(context_rows)

    summary[
        "total_matched"
    ] += len(matched_ids)

    summary[
        "total_invalid_model_projection"
    ] += len(
        invalid_model_projection_ids
    )

    summary[
        "total_unmatched_prediction"
    ] += len(
        unmatched_prediction_ids
    )

    summary[
        "total_unmatched_sportsbook"
    ] += len(
        unmatched_sportsbook_ids
    )

    summary[
        "total_unmatched_games"
    ] += len(
        unmatched_games_ids
    )

    summary[
        "total_missing_context"
    ] += len(
        missing_context_ids
    )

    summary[
        "total_game_id_to_gamePk_missing_context"
    ] += (
        game_id_to_game_pk_missing_context_count
    )

    fatal_errors = []

    if invalid_model_projection_ids:
        log(
            f"{date} | invalid model projection "
            "rows skipped and continuing: "
            f"count={len(invalid_model_projection_ids)} "
            f"game_ids="
            f"{sorted(invalid_model_projection_ids)}"
        )

    if unmatched_sportsbook_ids:
        log(
            f"{date} | sportsbook games without a "
            "valid model projection; "
            "skipping unmatched games and continuing: "
            f"count={len(unmatched_sportsbook_ids)} "
            f"game_ids={sorted(unmatched_sportsbook_ids)} "
            f"rejection_file={rejection_path}"
        )

    if missing_context_ids:
        fatal_errors.append(
            f"{date} | missing context rows hard failure: "
            f"count={len(missing_context_ids)} "
            f"game_ids={sorted(missing_context_ids)}"
        )

    ml_rows = []
    rl_rows = []
    tot_rows = []

    for game_id in sorted(
        matched_ids
    ):
        b = book_idx[
            game_id
        ]

        p = pred_idx[
            game_id
        ]

        games_row = (
            games_idx.get(
                game_id
            )
        )

        if not games_row:
            fatal_errors.append(
                f"{date} | matched game_id missing "
                f"from games file: game_id={game_id}"
            )

            continue

        game_pk = _clean(
            games_row.get(
                "gamePk"
            )
        )

        if not game_pk:
            fatal_errors.append(
                f"{date} | blank gamePk for matched "
                f"game_id={game_id}"
            )

            continue

        ctx = context_idx.get(
            game_pk
        )

        if ctx is None:
            fatal_errors.append(
                f"{date} | context missing for matched "
                f"game_id={game_id} gamePk={game_pk}"
            )

            continue

        validate_cross_source_teams(
            date=date,
            game_id=game_id,
            pred_row=p,
            book_row=b,
            games_row=games_row,
            context_row=ctx,
            team_id_to_canonical=team_id_to_canonical,
            errors=fatal_errors,
        )

        ctx_vals = [
            ctx.get(
                col,
                "",
            )
            for col in CONTEXT_COLS
        ]

        ml_rows.append(
            [
                RUN_TS,
                game_id,
                b.get("sport", ""),
                b.get("league", ""),
                b.get("game_date", ""),
                b.get("game_time", ""),
                b.get("_home_team_norm", ""),
                b.get("_away_team_norm", ""),
                b.get("away_run_line", ""),
                b.get("home_run_line", ""),
                b.get("total", ""),
                b.get(
                    "away_dk_moneyline_american",
                    "",
                ),
                b.get(
                    "home_dk_moneyline_american",
                    "",
                ),
                b.get(
                    "away_dk_moneyline_decimal",
                    "",
                ),
                b.get(
                    "home_dk_moneyline_decimal",
                    "",
                ),
                p.get(
                    "home_pitcher",
                    "",
                ),
                p.get(
                    "away_pitcher",
                    "",
                ),
                p.get(
                    "dratings_home_prob",
                    "",
                ),
                p.get(
                    "dratings_away_prob",
                    "",
                ),
                p.get(
                    "dratings_home_projected_runs",
                    "",
                ),
                p.get(
                    "dratings_away_projected_runs",
                    "",
                ),
                p.get(
                    "dratings_total_projected_runs",
                    "",
                ),
                p.get(
                    "model_home_runs",
                    "",
                ),
                p.get(
                    "model_away_runs",
                    "",
                ),
                p.get(
                    "model_total_runs",
                    "",
                ),
                p.get(
                    "run_model_version",
                    "",
                ),
                p.get(
                    "run_model_feature_status",
                    "",
                ),
            ]
            + ctx_vals
        )

        rl_rows.append(
            [
                RUN_TS,
                game_id,
                b.get("sport", ""),
                b.get("league", ""),
                b.get("game_date", ""),
                b.get("game_time", ""),
                b.get("_home_team_norm", ""),
                b.get("_away_team_norm", ""),
                b.get("away_run_line", ""),
                b.get("home_run_line", ""),
                b.get("total", ""),
                b.get(
                    "away_dk_run_line_american",
                    "",
                ),
                b.get(
                    "home_dk_run_line_american",
                    "",
                ),
                b.get(
                    "away_dk_run_line_decimal",
                    "",
                ),
                b.get(
                    "home_dk_run_line_decimal",
                    "",
                ),
                p.get(
                    "home_pitcher",
                    "",
                ),
                p.get(
                    "away_pitcher",
                    "",
                ),
                p.get(
                    "dratings_home_prob",
                    "",
                ),
                p.get(
                    "dratings_away_prob",
                    "",
                ),
                p.get(
                    "dratings_home_projected_runs",
                    "",
                ),
                p.get(
                    "dratings_away_projected_runs",
                    "",
                ),
                p.get(
                    "dratings_total_projected_runs",
                    "",
                ),
                p.get(
                    "model_home_runs",
                    "",
                ),
                p.get(
                    "model_away_runs",
                    "",
                ),
                p.get(
                    "model_total_runs",
                    "",
                ),
                p.get(
                    "run_model_version",
                    "",
                ),
                p.get(
                    "run_model_feature_status",
                    "",
                ),
            ]
            + ctx_vals
        )

        over_raw = american_to_prob(
            b.get(
                "dk_total_over_american",
                "",
            )
        )

        under_raw = american_to_prob(
            b.get(
                "dk_total_under_american",
                "",
            )
        )

        (
            over_prob,
            under_prob,
        ) = normalize_probs(
            over_raw,
            under_raw,
        )

        tot_rows.append(
            [
                RUN_TS,
                game_id,
                b.get("sport", ""),
                b.get("league", ""),
                b.get("game_date", ""),
                b.get("game_time", ""),
                b.get("_home_team_norm", ""),
                b.get("_away_team_norm", ""),
                b.get("away_run_line", ""),
                b.get("home_run_line", ""),
                b.get("total", ""),
                b.get(
                    "dk_total_over_american",
                    "",
                ),
                b.get(
                    "dk_total_under_american",
                    "",
                ),
                b.get(
                    "dk_total_over_decimal",
                    "",
                ),
                b.get(
                    "dk_total_under_decimal",
                    "",
                ),
                p.get(
                    "home_pitcher",
                    "",
                ),
                p.get(
                    "away_pitcher",
                    "",
                ),
                p.get(
                    "dratings_home_prob",
                    "",
                ),
                p.get(
                    "dratings_away_prob",
                    "",
                ),
                p.get(
                    "dratings_home_projected_runs",
                    "",
                ),
                p.get(
                    "dratings_away_projected_runs",
                    "",
                ),
                p.get(
                    "dratings_total_projected_runs",
                    "",
                ),
                p.get(
                    "model_home_runs",
                    "",
                ),
                p.get(
                    "model_away_runs",
                    "",
                ),
                p.get(
                    "model_total_runs",
                    "",
                ),
                p.get(
                    "run_model_version",
                    "",
                ),
                p.get(
                    "run_model_feature_status",
                    "",
                ),
                over_prob,
                under_prob,
            ]
            + ctx_vals
        )

    if fatal_errors:
        fail(
            f"{date} | merge validation failed: "
            f"{fatal_errors[:50]}"
        )

    validate_market_outputs(
        date,
        ml_rows,
        rl_rows,
        tot_rows,
    )

    base_ml_header = [
        "last_run",
        "game_id",
        "sport",
        "league",
        "game_date",
        "game_time",
        "home_team",
        "away_team",
        "away_run_line",
        "home_run_line",
        "total",
        "away_dk_moneyline_american",
        "home_dk_moneyline_american",
        "away_dk_moneyline_decimal",
        "home_dk_moneyline_decimal",
        "home_pitcher",
        "away_pitcher",
        "dratings_home_prob",
        "dratings_away_prob",
        "dratings_home_projected_runs",
        "dratings_away_projected_runs",
        "dratings_total_projected_runs",
        "model_home_runs",
        "model_away_runs",
        "model_total_runs",
        "run_model_version",
        "run_model_feature_status",
    ]

    base_rl_header = [
        "last_run",
        "game_id",
        "sport",
        "league",
        "game_date",
        "game_time",
        "home_team",
        "away_team",
        "away_run_line",
        "home_run_line",
        "total",
        "away_dk_run_line_american",
        "home_dk_run_line_american",
        "away_dk_run_line_decimal",
        "home_dk_run_line_decimal",
        "home_pitcher",
        "away_pitcher",
        "dratings_home_prob",
        "dratings_away_prob",
        "dratings_home_projected_runs",
        "dratings_away_projected_runs",
        "dratings_total_projected_runs",
        "model_home_runs",
        "model_away_runs",
        "model_total_runs",
        "run_model_version",
        "run_model_feature_status",
    ]

    base_tot_header = [
        "last_run",
        "game_id",
        "sport",
        "league",
        "game_date",
        "game_time",
        "home_team",
        "away_team",
        "away_run_line",
        "home_run_line",
        "total",
        "dk_total_over_american",
        "dk_total_under_american",
        "dk_total_over_decimal",
        "dk_total_under_decimal",
        "home_pitcher",
        "away_pitcher",
        "dratings_home_prob",
        "dratings_away_prob",
        "dratings_home_projected_runs",
        "dratings_away_projected_runs",
        "dratings_total_projected_runs",
        "model_home_runs",
        "model_away_runs",
        "model_total_runs",
        "run_model_version",
        "run_model_feature_status",
        "total_runs_over_prob",
        "total_runs_under_prob",
    ]

    write_csv(
        OUT_DIR
        / f"{date}_mlb_moneyline.csv",
        base_ml_header
        + CONTEXT_COLS,
        ml_rows,
    )

    write_csv(
        OUT_DIR
        / f"{date}_mlb_run_line.csv",
        base_rl_header
        + CONTEXT_COLS,
        rl_rows,
    )

    write_csv(
        OUT_DIR
        / f"{date}_mlb_total.csv",
        base_tot_header
        + CONTEXT_COLS,
        tot_rows,
    )

    summary[
        "files_written"
    ] += 3

    summary[
        "slates_written"
    ] += 1


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":
    summary = {
        "slates_processed": 0,
        "slates_written": 0,
        "skipped": 0,
        "files_written": 0,
        "total_prediction_rows": 0,
        "total_sportsbook_rows": 0,
        "total_games_rows": 0,
        "total_context_rows": 0,
        "total_matched": 0,
        "total_invalid_model_projection": 0,
        "total_unmatched_prediction": 0,
        "total_unmatched_sportsbook": 0,
        "total_unmatched_games": 0,
        "total_missing_context": 0,
        "total_game_id_to_gamePk_missing_context": 0,
    }

    try:
        clear_old_outputs()
        clear_old_audit_and_rejection_outputs()

        (
            alias_map,
            team_id_to_canonical,
            _canonical_to_team_id,
        ) = load_team_maps()

        pred_files = sorted(
            PRED_DIR.glob(
                "*_MLB.csv"
            )
        )

        log(
            f"Model projection files found: "
            f"{len(pred_files)}"
        )

        for file in pred_files:
            date = (
                file.stem
                .replace(
                    "_MLB",
                    "",
                )
            )

            summary[
                "slates_processed"
            ] += 1

            process_date(
                date,
                summary,
                alias_map,
                team_id_to_canonical,
            )

        log(
            "--- SUMMARY ---"
        )

        log(
            f"Last run timestamp: "
            f"{RUN_TS}"
        )

        log(
            f"Slates processed: "
            f"{summary['slates_processed']}"
        )

        log(
            f"Slates written: "
            f"{summary['slates_written']}"
        )

        log(
            f"Slates skipped: "
            f"{summary['skipped']}"
        )

        log(
            f"Files written: "
            f"{summary['files_written']}"
        )

        log(
            f"Total model projection rows: "
            f"{summary['total_prediction_rows']}"
        )

        log(
            f"Total sportsbook rows: "
            f"{summary['total_sportsbook_rows']}"
        )

        log(
            f"Total games rows: "
            f"{summary['total_games_rows']}"
        )

        log(
            f"Total context rows: "
            f"{summary['total_context_rows']}"
        )

        log(
            f"Total matched: "
            f"{summary['total_matched']}"
        )

        log(
            "Total invalid model projection rows skipped: "
            f"{summary['total_invalid_model_projection']}"
        )

        log(
            "Total unmatched model projection rows: "
            f"{summary['total_unmatched_prediction']}"
        )

        log(
            f"Total unmatched sportsbook rows: "
            f"{summary['total_unmatched_sportsbook']}"
        )

        log(
            f"Total unmatched games rows: "
            f"{summary['total_unmatched_games']}"
        )

        log(
            f"Total missing context rows: "
            f"{summary['total_missing_context']}"
        )

        log(
            "Total game_id_to_gamePk_missing_context_count: "
            f"{summary['total_game_id_to_gamePk_missing_context']}"
        )

        log(
            "STATUS: SUCCESS"
        )

        print(
            f"merge_intake complete. "
            f"last_run={RUN_TS} "
            f"files_written={summary['files_written']} "
            f"matched={summary['total_matched']} "
            f"unmatched_sportsbook="
            f"{summary['total_unmatched_sportsbook']} "
            f"missing_context="
            f"{summary['total_missing_context']} "
            f"Status: SUCCESS"
        )

    except Exception as e:
        log(
            f"FATAL ERROR: {e}\n"
            f"{traceback.format_exc()}"
        )

        log(
            "STATUS: FAILED"
        )

        raise