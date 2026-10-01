#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/02_juice/apply_moneyline_juice.py
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
    normalize_pair,
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
JUICE_FILE = Path("docs/win/baseball/mlb/config/juice/mlb_ml_juice.csv")

ERROR_DIR = Path("docs/win/baseball/mlb/errors/02_juice")
LOG_FILE = ERROR_DIR / "apply_moneyline_juice.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

NORMALIZATION_TOLERANCE = 0.000001

REQUIRED_INPUT_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_prob",
    "away_prob",
    "home_dk_moneyline_american",
    "away_dk_moneyline_american",
    "home_dk_moneyline_decimal",
    "away_dk_moneyline_decimal",
    "home_fair_decimal_moneyline",
    "away_fair_decimal_moneyline",
]

DK_ODDS_COLUMNS = [
    "home_dk_moneyline_american",
    "away_dk_moneyline_american",
    "home_dk_moneyline_decimal",
    "away_dk_moneyline_decimal",
]

REQUIRED_JUICE_COLUMNS = [
    "band_min",
    "band_max",
    "extra_juice",
    "fav_ud",
    "venue",
]

OUTPUT_ADDED_COLUMNS = [
    "home_juiced_prob_moneyline",
    "away_juiced_prob_moneyline",
    "home_juiced_decimal_moneyline",
    "away_juiced_decimal_moneyline",
    "home_normalized_prob_moneyline",
    "away_normalized_prob_moneyline",
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
        f"  files_found                       : {summary['files_found']}",
        f"  files_written                     : {summary['files_written']}",
        f"  total_rows                        : {summary['total_rows']}",
        f"  applied                           : {summary['applied']}",
        f"  skipped_bad                       : {summary['skipped_bad']}",
        f"  skipped_noband                    : {summary['skipped_noband']}",
        f"  missing_home_moneyline_dk         : {summary['missing_home_moneyline_dk']}",
        f"  missing_away_moneyline_dk         : {summary['missing_away_moneyline_dk']}",
        f"  missing_any_moneyline_dk          : {summary['missing_any_moneyline_dk']}",
        f"  stale_input_errors                : {summary['stale_input_errors']}",
        f"  normalization_errors              : {summary['normalization_errors']}",
        f"  schema_errors                     : {summary['schema_errors']}",
        f"  errors                            : {summary['errors']}",
        "",
        f"  {'file':<45} {'rows':>5} {'applied':>8} {'bad':>5} {'noband':>7} {'miss_home':>10} {'miss_away':>10} {'miss_any':>9} {'schema':>7}",
    ]

    for pf in per_file:
        lines.append(
            f"  {pf['name']:<45} {pf['rows']:>5} {pf['applied']:>8} "
            f"{pf['skipped_bad']:>5} {pf['skipped_noband']:>7} "
            f"{pf['missing_home_moneyline_dk']:>10} {pf['missing_away_moneyline_dk']:>10} "
            f"{pf['missing_any_moneyline_dk']:>9} {pf['schema_errors']:>7}"
        )

    append_summary_status(
        lines,
        summary,
        LOG_FILE,
    )


# =========================
# SCHEMA GUARDS
# =========================

# =========================
# JUICE CONFIG VALIDATION
# =========================

# =========================
# JUICE LOOKUP
# =========================

def find_band_row(juice_df, american, fav_ud, venue):
    band = juice_df[
        (juice_df["band_min"] <= american) &
        (american < juice_df["band_max"]) &
        (juice_df["fav_ud"] == fav_ud) &
        (juice_df["venue"] == venue)
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
        "market": "moneyline",
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

def _audit_moneyline_both(audit_rows, row, reason, home_extra=None, away_extra=None):
    append_audit_rows(audit_rows, row, "home", reason, home_extra or {})
    append_audit_rows(audit_rows, row, "away", reason, away_extra or {})


def _parse_moneyline_row(row):
    return {
        "home_american": float(row["home_dk_moneyline_american"]),
        "away_american": float(row["away_dk_moneyline_american"]),
        "home_decimal": float(row["home_dk_moneyline_decimal"]),
        "away_decimal": float(row["away_dk_moneyline_decimal"]),
        "home_fair": float(row["home_fair_decimal_moneyline"]),
        "away_fair": float(row["away_fair_decimal_moneyline"]),
    }


def _invalid_moneyline_numeric(values):
    for label, value in values.items():
        if not math.isfinite(value):
            return label, value
    return None


def process_row(df, juice_df, idx, row, audit_rows):
    if any(pd.isna(row[col]) for col in DK_ODDS_COLUMNS):
        _audit_moneyline_both(audit_rows, row, "missing_dk_odds")
        _log(f"row={idx} reason=missing_dk_odds", "SKIP")
        return df, "bad"
    try:
        values = _parse_moneyline_row(row)
    except (TypeError, ValueError, KeyError):
        _audit_moneyline_both(audit_rows, row, "bad_parse")
        _log(f"row={idx} reason=conversion_failed", "SKIP")
        return df, "bad"
    invalid = _invalid_moneyline_numeric(values)
    if invalid is not None:
        label, value = invalid
        _audit_moneyline_both(audit_rows, row, "invalid_numeric")
        _log(f"row={idx} reason=invalid_numeric {label}={value}", "SKIP")
        return df, "bad"
    ha, aa = values["home_american"], values["away_american"]
    hd, ad = values["home_decimal"], values["away_decimal"]
    hf, af = values["home_fair"], values["away_fair"]
    if hf <= 1 or af <= 1 or hd <= 1 or ad <= 1:
        _audit_moneyline_both(audit_rows, row, "invalid_decimal")
        _log(f"row={idx} reason=invalid_decimal", "SKIP")
        return df, "bad"
    home_type = "favorite" if ha < 0 else "underdog"
    away_type = "favorite" if aa < 0 else "underdog"
    home_extra = find_band_row(juice_df, ha, home_type, "home")
    away_extra = find_band_row(juice_df, aa, away_type, "away")
    if home_extra is None or away_extra is None:
        _audit_moneyline_both(
            audit_rows, row, "missing_band",
            {"dk_american": ha, "dk_decimal": hd, "fair_decimal": hf},
            {"dk_american": aa, "dk_decimal": ad, "fair_decimal": af},
        )
        _log(f"row={idx} reason=band_lookup_failed home={ha} away={aa}", "SKIP")
        return df, "noband"
    home_juiced_decimal = hf * (1 - home_extra)
    away_juiced_decimal = af * (1 - away_extra)
    juiced = (home_juiced_decimal, away_juiced_decimal)
    if not all(math.isfinite(value) for value in juiced):
        _audit_moneyline_both(audit_rows, row, "invalid_juiced_decimal")
        _log(
            f"row={idx} reason=nonfinite_juiced_decimal "
            f"home={home_juiced_decimal} away={away_juiced_decimal}", "SKIP"
        )
        return df, "bad"
    if home_juiced_decimal <= 1 or away_juiced_decimal <= 1:
        _audit_moneyline_both(audit_rows, row, "invalid_juiced_decimal")
        _log(
            f"row={idx} reason=invalid_juiced_decimal "
            f"home={home_juiced_decimal} away={away_juiced_decimal}", "SKIP"
        )
        return df, "bad"
    home_prob = 1 / home_juiced_decimal
    away_prob = 1 / away_juiced_decimal
    normalized = normalize_pair(home_prob, away_prob)
    if normalized is None:
        _audit_moneyline_both(audit_rows, row, "invalid_normalization_total")
        _log(
            f"row={idx} reason=invalid_normalization_total val={home_prob + away_prob}",
            "SKIP",
        )
        return df, "bad"
    home_norm, away_norm = normalized
    df.at[idx, "home_juiced_decimal_moneyline"] = home_juiced_decimal
    df.at[idx, "away_juiced_decimal_moneyline"] = away_juiced_decimal
    df.at[idx, "home_juiced_prob_moneyline"] = home_prob
    df.at[idx, "away_juiced_prob_moneyline"] = away_prob
    df.at[idx, "home_normalized_prob_moneyline"] = home_norm
    df.at[idx, "away_normalized_prob_moneyline"] = away_norm
    append_audit_rows(audit_rows, row, "home", "juiced", {
        "dk_american": ha, "dk_decimal": hd, "fair_decimal": hf,
        "juiced_decimal": home_juiced_decimal, "juiced_prob": home_prob,
        "normalized_prob": home_norm,
    })
    append_audit_rows(audit_rows, row, "away", "juiced", {
        "dk_american": aa, "dk_decimal": ad, "fair_decimal": af,
        "juiced_decimal": away_juiced_decimal, "juiced_prob": away_prob,
        "normalized_prob": away_norm,
    })
    return df, "ok"

def _new_moneyline_summary():
    return {
        "files_found": 0, "files_written": 0, "total_rows": 0,
        "applied": 0, "skipped_bad": 0, "skipped_noband": 0,
        "missing_home_moneyline_dk": 0, "missing_away_moneyline_dk": 0,
        "missing_any_moneyline_dk": 0, "stale_input_errors": 0,
        "normalization_errors": 0, "schema_errors": 0, "errors": 0,
    }


def _new_moneyline_file_summary(name):
    return {
        "name": name, "rows": 0, "applied": 0, "skipped_bad": 0,
        "skipped_noband": 0, "missing_home_moneyline_dk": 0,
        "missing_away_moneyline_dk": 0, "missing_any_moneyline_dk": 0,
        "schema_errors": 0,
    }


def _process_moneyline_file(in_path, juice_df, audit_rows, summary):
    out_path = OUTPUT_DIR / in_path.name
    pf = _new_moneyline_file_summary(in_path.name)
    _log(f"--- FILE: {in_path.name}")
    validate_stale_source(in_path, SOURCE_MERGE_DIR, _log)
    df = read_csv_validated(in_path, REQUIRED_INPUT_COLUMNS, f"{in_path.name} input")
    require_nonempty_columns(df, DK_ODDS_COLUMNS, f"{in_path.name} input")
    if df.empty:
        _log(f"{in_path.name} empty ??? skipping")
        return pf
    numeric_cols = [
        "home_dk_moneyline_american", "away_dk_moneyline_american",
        "home_dk_moneyline_decimal", "away_dk_moneyline_decimal",
        "home_fair_decimal_moneyline", "away_fair_decimal_moneyline",
        "home_prob", "away_prob",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    pf["missing_home_moneyline_dk"] = int(
        df[["home_dk_moneyline_american", "home_dk_moneyline_decimal"]].isna().any(axis=1).sum()
    )
    pf["missing_away_moneyline_dk"] = int(
        df[["away_dk_moneyline_american", "away_dk_moneyline_decimal"]].isna().any(axis=1).sum()
    )
    pf["missing_any_moneyline_dk"] = int(df[DK_ODDS_COLUMNS].isna().any(axis=1).sum())
    for col in OUTPUT_ADDED_COLUMNS:
        df[col] = pd.NA
    pf["rows"] = len(df)
    summary["total_rows"] += len(df)
    for key in (
        "missing_home_moneyline_dk", "missing_away_moneyline_dk",
        "missing_any_moneyline_dk",
    ):
        summary[key] += pf[key]
    result_key = {"ok": "applied", "noband": "skipped_noband"}
    for idx, row in df.iterrows():
        df, result = process_row(df, juice_df, idx, row, audit_rows)
        pf[result_key.get(result, "skipped_bad")] += 1
    norm_bad = validate_normalized_probability_pair(
        df,
        "home_normalized_prob_moneyline",
        "away_normalized_prob_moneyline",
        f"{in_path.name} moneyline",
        NORMALIZATION_TOLERANCE,
        _log,
    )
    if norm_bad:
        summary["normalization_errors"] += norm_bad
        raise ValueError(
            f"{in_path.name} has {norm_bad} invalid normalized moneyline probability rows"
        )
    write_csv_validated(df, out_path, f"{in_path.name} output")
    summary["files_written"] += 1
    for key in ("applied", "skipped_bad", "skipped_noband"):
        summary[key] += pf[key]
    _log(
        f"{in_path.name} | rows={pf['rows']} applied={pf['applied']} "
        f"skipped_bad={pf['skipped_bad']} skipped_noband={pf['skipped_noband']} "
        f"missing_any_moneyline_dk={pf['missing_any_moneyline_dk']}"
    )
    _log(f"WROTE: {out_path}")
    return pf


def main():
    with open(LOG_FILE, "w", encoding="utf-8") as handle:
        handle.write(f"=== apply_moneyline_juice RUN {_now()} ===\n")
    summary = _new_moneyline_summary()
    per_file = []
    audit_rows = []
    for old in OUTPUT_DIR.glob("*moneyline.csv"):
        old.unlink()
    for old in AUDIT_DIR.glob("*moneyline*post_juice_audit.csv"):
        old.unlink()
    try:
        log_stage_inputs(_log, INPUT_DIR, SOURCE_MERGE_DIR, JUICE_FILE)
        juice_df = load_juice_config(
            JUICE_FILE, REQUIRED_JUICE_COLUMNS, categorical_columns=("fav_ud", "venue")
        )
        validate_fav_ud_venue_juice_config(juice_df, "moneyline")
        files = sorted(glob.glob(str(INPUT_DIR / "*_mlb_moneyline.csv")))
        summary["files_found"] = len(files)
        _log(f"Files found: {len(files)}")
        if not files:
            _log("No moneyline files found ??? exiting", "WARN")
            _write_summary(summary, per_file)
            return
        for file_path in files:
            in_path = Path(file_path)
            try:
                pf = _process_moneyline_file(in_path, juice_df, audit_rows, summary)
            except ValueError as exc:
                pf = _new_moneyline_file_summary(in_path.name)
                if "stale 01_merguiced input" in str(exc):
                    summary["stale_input_errors"] += 1
                pf["schema_errors"] += 1
                summary["schema_errors"] += 1
                summary["errors"] += 1
                _log(f"{in_path.name} FAILED: {exc}\n{traceback.format_exc()}", "ERROR")
            except Exception as exc:
                pf = _new_moneyline_file_summary(in_path.name)
                summary["errors"] += 1
                _log(f"{in_path.name} FAILED: {exc}\n{traceback.format_exc()}", "ERROR")
            per_file.append(pf)
    except Exception as exc:
        _log(f"FATAL: {exc}\n{traceback.format_exc()}", "ERROR")
        summary["errors"] += 1
        _write_summary(summary, per_file)
        sys.exit(1)
    audit_path = AUDIT_DIR / "moneyline_post_juice_audit.csv"
    write_audit(audit_rows, audit_path, _log)
    _write_summary(summary, per_file)
    fail_on_summary_errors(summary, "apply_moneyline_juice")
    print(
        f"apply_moneyline_juice complete. files_written={summary['files_written']} "
        f"applied={summary['applied']} missing_any_moneyline_dk={summary['missing_any_moneyline_dk']} "
        f"skipped_bad={summary['skipped_bad']} skipped_noband={summary['skipped_noband']} "
        f"schema_errors={summary['schema_errors']} errors={summary['errors']} STATUS: SUCCESS"
    )
