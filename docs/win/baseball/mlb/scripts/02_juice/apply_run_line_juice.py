#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/02_juice/apply_run_line_juice.py

import glob
import math
import sys
import traceback
from pathlib import Path

import pandas as pd

from juice_common import (
    append_summary_status,
    fail_on_summary_errors,
    load_juice_config,
    log_stage_inputs,
    make_logger,
    read_csv_validated,
    require_nonempty_columns,
    utc_now,
    validate_fav_ud_venue_juice_config,
    validate_normalized_probability_pair,
    validate_stale_source,
    write_audit,
    write_csv_validated,
)

INPUT_DIR = Path("docs/win/baseball/mlb/01_merge/01_merguiced")
SOURCE_MERGE_DIR = INPUT_DIR.parent
OUTPUT_DIR = Path("docs/win/baseball/mlb/02_juice")
AUDIT_DIR = OUTPUT_DIR / "audit"
JUICE_FILE = Path("docs/win/baseball/mlb/config/juice/mlb_run_line_juice.csv")

ERROR_DIR = Path("docs/win/baseball/mlb/errors/02_juice")
LOG_FILE = ERROR_DIR / "apply_run_line_juice.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

NORMALIZATION_TOLERANCE = 0.000001

REQUIRED_JUICE_COLUMNS = [
    "band_min",
    "band_max",
    "venue",
    "fav_ud",
    "extra_juice",
]

REQUIRED_RUN_LINE_COLUMNS = [
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
    "home_prob",
    "away_prob",
    "away_projected_runs",
    "home_projected_runs",
    "total_projected_runs",
    "home_prob_run_line",
    "away_prob_run_line",
]

DK_ODDS_COLUMNS = [
    "away_dk_run_line_american",
    "home_dk_run_line_american",
    "away_dk_run_line_decimal",
    "home_dk_run_line_decimal",
]

FORBIDDEN_RUN_LINE_COLUMNS = [
    "home_run_line_prob",
    "away_run_line_prob",
]

OUTPUT_PROB_COLUMNS = [
    "home_juiced_prob_run_line",
    "away_juiced_prob_run_line",
    "home_normalized_prob_run_line",
    "away_normalized_prob_run_line",
]

# =========================
# LOGGING
# =========================

_now = utc_now
_log = make_logger(LOG_FILE)


def _write_summary(summary: dict, per_file: list) -> None:
    lines = [
        "",
        "=" * 60,
        f"SUMMARY  {_now()}",
        "=" * 60,
        f"  files_found                         : {summary['files_found']}",
        f"  files_written                       : {summary['files_written']}",
        f"  total_rows                          : {summary['total_rows']}",
        f"  applied                             : {summary['applied']}",
        f"  skipped_bad                         : {summary['skipped_bad']}",
        f"  skipped_noband                      : {summary['skipped_noband']}",
        f"  missing_home_run_line_dk            : {summary['missing_home_run_line_dk']}",
        f"  missing_away_run_line_dk            : {summary['missing_away_run_line_dk']}",
        f"  missing_home_run_line_price_dk      : {summary['missing_home_run_line_price_dk']}",
        f"  missing_away_run_line_price_dk      : {summary['missing_away_run_line_price_dk']}",
        f"  missing_any_run_line_dk             : {summary['missing_any_run_line_dk']}",
        f"  stale_input_errors                  : {summary['stale_input_errors']}",
        f"  normalization_errors                : {summary['normalization_errors']}",
        f"  schema_errors                       : {summary['schema_errors']}",
        f"  errors                              : {summary['errors']}",
        "",
        f"  {'file':<45} {'rows':>5} {'applied':>8} {'bad':>5} {'noband':>7} {'miss_home':>10} {'miss_away':>10} {'miss_any':>9} {'schema':>7}",
    ]

    for pf in per_file:
        lines.append(
            f"  {pf['name']:<45} {pf['rows']:>5} {pf['applied']:>8} "
            f"{pf['skipped_bad']:>5} {pf['skipped_noband']:>7} "
            f"{pf['missing_home_run_line_dk']:>10} {pf['missing_away_run_line_dk']:>10} "
            f"{pf['missing_any_run_line_dk']:>9} {pf['schema_errors']:>7}"
        )

    append_summary_status(
        lines,
        summary,
        LOG_FILE,
    )


# =========================
# SCHEMA VALIDATION
# =========================

def validate_forbidden_columns(df: pd.DataFrame, forbidden_columns: list, label: str) -> None:
    present = [col for col in forbidden_columns if col in df.columns]
    if present:
        raise ValueError(
            f"{label} contains forbidden obsolete columns: {present}. "
            f"Use home_prob_run_line and away_prob_run_line instead."
        )


# =========================
# JUICE CONFIG VALIDATION
# =========================

# =========================
# JUICE LOOKUP
# =========================

def find_band(juice_df, odds, venue, fav_ud):
    band = juice_df[
        (juice_df["band_min"] <= odds) &
        (odds < juice_df["band_max"]) &
        (juice_df["venue"] == venue) &
        (juice_df["fav_ud"] == fav_ud)
    ]

    if len(band) != 1:
        return None

    return float(band.iloc[0]["extra_juice"])


# =========================
# AUDIT
# =========================

def append_audit_rows(audit_rows: list, row: pd.Series, side: str, status: str, values: dict | None = None) -> None:
    values = values or {}
    audit_rows.append({
        "date": row.get("game_date", pd.NA),
        "game_id": row.get("game_id", pd.NA),
        "market": "run_line",
        "side": side,
        "dk_american": values.get("dk_american", pd.NA),
        "dk_decimal": values.get("dk_decimal", pd.NA),
        "fair_decimal": values.get("fair_decimal", pd.NA),
        "juiced_decimal": values.get("juiced_decimal", pd.NA),
        "juiced_prob": values.get("juiced_prob", pd.NA),
        "normalized_prob": values.get("normalized_prob", pd.NA),
        "status": status,
    })


# =========================
# ROW PROCESSOR
# =========================

def _audit_run_line_both(audit_rows, row, reason, home_extra=None, away_extra=None):
    append_audit_rows(audit_rows, row, "home", reason, home_extra or {})
    append_audit_rows(audit_rows, row, "away", reason, away_extra or {})


def _parse_run_line_row(row):
    return {
        "home_base": float(row["home_prob_run_line"]),
        "away_base": float(row["away_prob_run_line"]),
        "home_odds": float(row["home_dk_run_line_american"]),
        "away_odds": float(row["away_dk_run_line_american"]),
        "home_decimal": float(row["home_dk_run_line_decimal"]),
        "away_decimal": float(row["away_dk_run_line_decimal"]),
    }


def _invalid_run_line_numeric(values):
    for label, value in values.items():
        if not math.isfinite(value):
            return label, value
    return None


def process_row(df, juice_df, idx, row, audit_rows):
    if any(pd.isna(row[col]) for col in DK_ODDS_COLUMNS):
        _audit_run_line_both(audit_rows, row, "missing_dk_odds")
        _log(f"row={idx} reason=missing_dk_odds", "SKIP")
        return df, "bad"
    try:
        values = _parse_run_line_row(row)
    except (TypeError, ValueError, KeyError):
        _audit_run_line_both(audit_rows, row, "bad_parse")
        _log(f"row={idx} reason=conversion_failed", "SKIP")
        return df, "bad"
    invalid = _invalid_run_line_numeric(values)
    if invalid is not None:
        label, value = invalid
        _audit_run_line_both(audit_rows, row, "invalid_numeric")
        _log(f"row={idx} reason=invalid_numeric {label}={value}", "SKIP")
        return df, "bad"
    home_base = values["home_base"]
    away_base = values["away_base"]
    home_odds = values["home_odds"]
    away_odds = values["away_odds"]
    home_decimal = values["home_decimal"]
    away_decimal = values["away_decimal"]
    if home_base < 0 or away_base < 0 or home_decimal <= 1 or away_decimal <= 1:
        _audit_run_line_both(audit_rows, row, "invalid_decimal")
        _log(f"row={idx} reason=invalid_run_line_inputs", "SKIP")
        return df, "bad"
    home_type = "favorite" if home_odds < 0 else "underdog"
    away_type = "favorite" if away_odds < 0 else "underdog"
    home_extra = find_band(juice_df, home_odds, "home", home_type)
    away_extra = find_band(juice_df, away_odds, "away", away_type)
    if home_extra is None or away_extra is None:
        _audit_run_line_both(
            audit_rows,
            row,
            "missing_band",
            {"dk_american": home_odds, "dk_decimal": home_decimal, "fair_decimal": pd.NA},
            {"dk_american": away_odds, "dk_decimal": away_decimal, "fair_decimal": pd.NA},
        )
        _log(f"row={idx} reason=no_band home_odds={home_odds} away_odds={away_odds}", "SKIP")
        return df, "noband"
    home_juiced = max(min(home_base + home_extra, 0.95), 0.01)
    away_juiced = max(min(away_base + away_extra, 0.95), 0.01)
    total = home_juiced + away_juiced
    if not math.isfinite(total) or total <= 0:
        _audit_run_line_both(audit_rows, row, "invalid_normalization_total")
        _log(f"row={idx} reason=invalid_normalization_total total={total}", "SKIP")
        return df, "bad"
    home_normalized = home_juiced / total
    away_normalized = away_juiced / total
    df.at[idx, "home_juiced_prob_run_line"] = home_juiced
    df.at[idx, "away_juiced_prob_run_line"] = away_juiced
    df.at[idx, "home_normalized_prob_run_line"] = home_normalized
    df.at[idx, "away_normalized_prob_run_line"] = away_normalized
    append_audit_rows(audit_rows, row, "home", "juiced", {
        "dk_american": home_odds, "dk_decimal": home_decimal,
        "fair_decimal": pd.NA, "juiced_decimal": pd.NA,
        "juiced_prob": home_juiced, "normalized_prob": home_normalized,
    })
    append_audit_rows(audit_rows, row, "away", "juiced", {
        "dk_american": away_odds, "dk_decimal": away_decimal,
        "fair_decimal": pd.NA, "juiced_decimal": pd.NA,
        "juiced_prob": away_juiced, "normalized_prob": away_normalized,
    })
    return df, "ok"

def _new_run_line_summary():
    return {
        "files_found": 0, "files_written": 0, "total_rows": 0,
        "applied": 0, "skipped_bad": 0, "skipped_noband": 0,
        "missing_home_run_line_dk": 0, "missing_away_run_line_dk": 0,
        "missing_home_run_line_price_dk": 0, "missing_away_run_line_price_dk": 0,
        "missing_any_run_line_dk": 0, "stale_input_errors": 0,
        "normalization_errors": 0, "schema_errors": 0, "errors": 0,
    }


def _new_run_line_file_summary(name):
    return {
        "name": name, "rows": 0, "applied": 0, "skipped_bad": 0,
        "skipped_noband": 0, "missing_home_run_line_dk": 0,
        "missing_away_run_line_dk": 0, "missing_home_run_line_price_dk": 0,
        "missing_away_run_line_price_dk": 0, "missing_any_run_line_dk": 0,
        "schema_errors": 0,
    }


def _process_run_line_file(in_path, juice_df, audit_rows, summary):
    out_path = OUTPUT_DIR / in_path.name
    pf = _new_run_line_file_summary(in_path.name)
    _log(f"--- FILE: {in_path.name}")
    validate_stale_source(in_path, SOURCE_MERGE_DIR, _log)
    df = read_csv_validated(
        in_path, REQUIRED_RUN_LINE_COLUMNS, f"run-line input {in_path.name}"
    )
    validate_forbidden_columns(
        df, FORBIDDEN_RUN_LINE_COLUMNS, f"run-line input {in_path.name}"
    )
    require_nonempty_columns(df, DK_ODDS_COLUMNS, f"run-line input {in_path.name}")
    if df.empty:
        _log(f"{in_path.name} empty ??? skipping")
        return pf
    numeric_cols = [
        "home_prob_run_line", "away_prob_run_line",
        "home_dk_run_line_american", "away_dk_run_line_american",
        "home_dk_run_line_decimal", "away_dk_run_line_decimal",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    pf["missing_home_run_line_dk"] = int(df["home_dk_run_line_american"].isna().sum())
    pf["missing_away_run_line_dk"] = int(df["away_dk_run_line_american"].isna().sum())
    pf["missing_home_run_line_price_dk"] = int(df["home_dk_run_line_decimal"].isna().sum())
    pf["missing_away_run_line_price_dk"] = int(df["away_dk_run_line_decimal"].isna().sum())
    pf["missing_any_run_line_dk"] = int(df[DK_ODDS_COLUMNS].isna().any(axis=1).sum())
    for col in OUTPUT_PROB_COLUMNS:
        df[col] = pd.NA
    pf["rows"] = len(df)
    summary["total_rows"] += len(df)
    for key in (
        "missing_home_run_line_dk", "missing_away_run_line_dk",
        "missing_home_run_line_price_dk", "missing_away_run_line_price_dk",
        "missing_any_run_line_dk",
    ):
        summary[key] += pf[key]
    result_key = {"ok": "applied", "noband": "skipped_noband"}
    for idx, row in df.iterrows():
        df, result = process_row(df, juice_df, idx, row, audit_rows)
        pf[result_key.get(result, "skipped_bad")] += 1
    norm_bad = validate_normalized_probability_pair(
        df,
        "home_normalized_prob_run_line",
        "away_normalized_prob_run_line",
        f"{in_path.name} run_line",
        NORMALIZATION_TOLERANCE,
        _log,
    )
    if norm_bad:
        summary["normalization_errors"] += norm_bad
        raise ValueError(
            f"{in_path.name} has {norm_bad} invalid normalized run-line probability rows"
        )
    write_csv_validated(df, out_path, f"run-line output {out_path.name}")
    summary["files_written"] += 1
    for key in ("applied", "skipped_bad", "skipped_noband"):
        summary[key] += pf[key]
    _log(
        f"{in_path.name} | rows={pf['rows']} applied={pf['applied']} "
        f"skipped_bad={pf['skipped_bad']} skipped_noband={pf['skipped_noband']} "
        f"missing_any_run_line_dk={pf['missing_any_run_line_dk']}"
    )
    _log(f"WROTE: {out_path}")
    return pf


def main():
    with open(LOG_FILE, "w", encoding="utf-8") as handle:
        handle.write(f"=== apply_run_line_juice RUN {_now()} ===\n")
    summary = _new_run_line_summary()
    per_file = []
    audit_rows = []
    for old in OUTPUT_DIR.glob("*run_line.csv"):
        old.unlink()
    for old in AUDIT_DIR.glob("*run_line*post_juice_audit.csv"):
        old.unlink()
    try:
        log_stage_inputs(_log, INPUT_DIR, SOURCE_MERGE_DIR, JUICE_FILE)
        juice_df = load_juice_config(
            JUICE_FILE, REQUIRED_JUICE_COLUMNS, categorical_columns=("venue", "fav_ud")
        )
        validate_fav_ud_venue_juice_config(juice_df, "run-line")
        files = sorted(glob.glob(str(INPUT_DIR / "*_mlb_run_line.csv")))
        summary["files_found"] = len(files)
        _log(f"Files found: {len(files)}")
        if not files:
            _log("No run-line files found ??? exiting", "WARN")
            _write_summary(summary, per_file)
            return
        for file_path in files:
            in_path = Path(file_path)
            try:
                pf = _process_run_line_file(in_path, juice_df, audit_rows, summary)
            except ValueError as exc:
                pf = _new_run_line_file_summary(in_path.name)
                if "stale 01_merguiced input" in str(exc):
                    summary["stale_input_errors"] += 1
                pf["schema_errors"] += 1
                summary["schema_errors"] += 1
                summary["errors"] += 1
                _log(f"{in_path.name} SCHEMA FAILED: {exc}\n{traceback.format_exc()}", "ERROR")
            except Exception as exc:
                pf = _new_run_line_file_summary(in_path.name)
                summary["errors"] += 1
                _log(f"{in_path.name} FAILED: {exc}\n{traceback.format_exc()}", "ERROR")
            per_file.append(pf)
    except Exception as exc:
        _log(f"FATAL: {exc}\n{traceback.format_exc()}", "ERROR")
        summary["errors"] += 1
        _write_summary(summary, per_file)
        sys.exit(1)
    audit_path = AUDIT_DIR / "run_line_post_juice_audit.csv"
    write_audit(audit_rows, audit_path, _log)
    _write_summary(summary, per_file)
    fail_on_summary_errors(summary, "apply_run_line_juice")
    print(
        f"apply_run_line_juice complete. files_written={summary['files_written']} "
        f"applied={summary['applied']} missing_any_run_line_dk={summary['missing_any_run_line_dk']} "
        f"skipped_bad={summary['skipped_bad']} skipped_noband={summary['skipped_noband']} "
        f"schema_errors={summary['schema_errors']} errors={summary['errors']} STATUS: SUCCESS"
    )
