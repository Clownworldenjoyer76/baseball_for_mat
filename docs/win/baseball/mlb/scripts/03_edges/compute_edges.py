#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/03_edges/compute_edges.py

import traceback
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

INPUT_DIR = Path("docs/win/baseball/mlb/02_juice")
OUTPUT_DIR = Path("docs/win/baseball/mlb/03_edges")
ERROR_DIR = Path("docs/win/baseball/mlb/errors/03_edges")
LOG_FILE = ERROR_DIR / "compute_edges.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_DIR = Path("docs/win/baseball/mlb/audit")
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
LEAKAGE_AUDIT_FILE = AUDIT_DIR / "leakage_audit.csv"

FORBIDDEN_READ_TOKENS = [
    "05_" + "final_scores",
    "final" + "_scores",
    "graded",
    "results",
    "reports",
]  # LEAKAGE_GUARD_ALLOWED_REFERENCE

SCRIPT_NAME = "compute_edges.py"
STAGE_NAME = "03_edges"
PROB_TOLERANCE = 1e-6


MONEYLINE_REQUIRED_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_model_prob_moneyline",
    "away_model_prob_moneyline",
    "home_dk_decimal_moneyline",
    "away_dk_decimal_moneyline",
]

RUN_LINE_REQUIRED_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_model_prob_run_line",
    "away_model_prob_run_line",
    "home_dk_run_line_decimal",
    "away_dk_run_line_decimal",
]

TOTAL_REQUIRED_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "over_model_prob_total_win",
    "over_model_prob_total_loss",
    "under_model_prob_total_win",
    "under_model_prob_total_loss",
    "total_model_prob_push",
    "dk_total_over_decimal",
    "dk_total_under_decimal",
]


# =========================
# LEAKAGE / READ GUARDS
# =========================

def record_file_read(path: Path, path_allowed: bool, reason: str) -> None:
    path = Path(path)
    new_file = not LEAKAGE_AUDIT_FILE.exists()

    with open(LEAKAGE_AUDIT_FILE, "a", encoding="utf-8", newline="") as f:
        if new_file:
            f.write("script,file_read,path_allowed,reason,stage,timestamp\n")

        safe_path = str(path).replace('"', "''")
        f.write(
            f'{SCRIPT_NAME},"{safe_path}",{1 if path_allowed else 0},'
            f'"{reason}",{STAGE_NAME},{datetime.now(UTC).isoformat()}\n'
        )


def assert_read_path_allowed(path: Path) -> None:
    path = Path(path)
    lower_path = str(path).replace("\\", "/").lower()
    matched = [token for token in FORBIDDEN_READ_TOKENS if token in lower_path]

    if matched:
        reason = "forbidden_pre_selection_read:" + ";".join(matched)
        record_file_read(path, False, reason)
        raise RuntimeError(
            f"Blocked forbidden pre-selection read path: {path} ({reason})"
        )

    record_file_read(path, True, "allowed")


def read_csv_guarded(path: Path) -> pd.DataFrame:
    assert_read_path_allowed(path)
    return pd.read_csv(path)


# =========================
# LOGGING
# =========================

def _now():
    return datetime.now(UTC).isoformat()


def _log(msg: str, level: str = "INFO") -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level:<5} | {msg.rstrip()}\n")


def _write_summary(summary: dict, per_file: list) -> None:
    lines = [
        "",
        "=" * 60,
        f"SUMMARY  {_now()}",
        "=" * 60,
        f"  files_processed  : {summary['files_processed']}",
        f"  rows_processed   : {summary['rows_processed']}",
        f"  moneyline_files  : {summary['moneyline_files']}",
        f"  run_line_files   : {summary['run_line_files']}",
        f"  total_files      : {summary['total_files']}",
        f"  skipped          : {summary['skipped']}",
        f"  null_edges       : {summary['null_edges']}",
        f"  schema_errors    : {summary['schema_errors']}",
        f"  errors           : {summary['errors']}",
        "",
        f"  {'file':<48} {'market':<12} {'rows':>5} {'null_edges':>10} {'status':>10}",
    ]

    for pf in per_file:
        lines.append(
            f"  {pf['name']:<48} {pf['market']:<12} {pf['rows']:>5} "
            f"{pf['null_edges']:>10} {pf['status']:>10}"
        )

    status = (
        "SUCCESS"
        if summary["errors"] == 0 and summary["schema_errors"] == 0
        else "COMPLETED WITH ERRORS"
    )
    lines += ["", f"STATUS: {status}", "=" * 60]

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# =========================
# SCHEMA GUARDS
# =========================

def duplicate_columns(columns) -> list:
    seen = set()
    duplicates = []

    for col in columns:
        if col in seen and col not in duplicates:
            duplicates.append(col)
        seen.add(col)

    return duplicates


def validate_no_duplicate_columns(df: pd.DataFrame, label: str) -> None:
    dupes = duplicate_columns(list(df.columns))

    if dupes:
        raise ValueError(f"{label} has duplicate columns: {dupes}")


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: list,
    label: str,
) -> None:
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def validate_probability_values(
    df: pd.DataFrame,
    columns: list,
    label: str,
) -> None:
    bad = pd.Series(False, index=df.index)

    for col in columns:
        values = pd.to_numeric(df[col], errors="coerce")
        bad = (
            bad
            | values.isna()
            | ~np.isfinite(values)
            | (values < 0)
            | (values > 1)
        )

    if bad.any():
        sample = df.loc[bad, ["game_id"] + columns].head(10).to_dict("records")
        raise ValueError(
            f"{label} has missing, non-finite, or out-of-range canonical probabilities; "
            f"bad_rows={int(bad.sum())}; sample={sample}"
        )


def validate_probability_sum(
    df: pd.DataFrame,
    columns: list,
    label: str,
) -> None:
    total = sum(pd.to_numeric(df[col], errors="coerce") for col in columns)
    bad = (total - 1.0).abs() > PROB_TOLERANCE

    if bad.any():
        sample = df.loc[bad, ["game_id"] + columns].head(10).to_dict("records")
        raise ValueError(
            f"{label} canonical probabilities do not sum to 1 within "
            f"{PROB_TOLERANCE}; bad_rows={int(bad.sum())}; sample={sample}"
        )


def validate_probability_contract(
    df: pd.DataFrame,
    market: str,
    label: str,
) -> None:
    if market == "moneyline":
        cols = [
            "home_model_prob_moneyline",
            "away_model_prob_moneyline",
        ]
        validate_probability_values(df, cols, label)
        validate_probability_sum(df, cols, label)
        return

    if market == "run_line":
        cols = [
            "home_model_prob_run_line",
            "away_model_prob_run_line",
        ]
        validate_probability_values(df, cols, label)
        validate_probability_sum(df, cols, label)
        return

    if market == "total":
        cols = [
            "over_model_prob_total_win",
            "over_model_prob_total_loss",
            "under_model_prob_total_win",
            "under_model_prob_total_loss",
            "total_model_prob_push",
        ]
        validate_probability_values(df, cols, label)

        validate_probability_sum(
            df,
            [
                "over_model_prob_total_win",
                "over_model_prob_total_loss",
                "total_model_prob_push",
            ],
            label,
        )
        validate_probability_sum(
            df,
            [
                "under_model_prob_total_win",
                "under_model_prob_total_loss",
                "total_model_prob_push",
            ],
            label,
        )

        over_win = pd.to_numeric(
            df["over_model_prob_total_win"], errors="coerce"
        )
        over_loss = pd.to_numeric(
            df["over_model_prob_total_loss"], errors="coerce"
        )
        under_win = pd.to_numeric(
            df["under_model_prob_total_win"], errors="coerce"
        )
        under_loss = pd.to_numeric(
            df["under_model_prob_total_loss"], errors="coerce"
        )

        bad = (
            (under_win - over_loss).abs() > PROB_TOLERANCE
        ) | (
            (under_loss - over_win).abs() > PROB_TOLERANCE
        )

        if bad.any():
            sample = df.loc[
                bad,
                ["game_id"] + cols,
            ].head(10).to_dict("records")
            raise ValueError(
                f"{label} totals win/loss identities are inconsistent; "
                f"bad_rows={int(bad.sum())}; sample={sample}"
            )
        return

    raise ValueError(
        f"{label} unknown market for probability validation: {market}"
    )


def validate_decimal_odds(
    df: pd.DataFrame,
    columns: list,
    label: str,
) -> None:
    bad = pd.Series(False, index=df.index)

    for col in columns:
        values = pd.to_numeric(df[col], errors="coerce")
        bad = bad | values.isna() | ~np.isfinite(values) | (values <= 1.0)

    if bad.any():
        sample = df.loc[bad, ["game_id"] + columns].head(10).to_dict("records")
        raise ValueError(
            f"{label} has missing, non-finite, or invalid decimal odds; "
            f"bad_rows={int(bad.sum())}; sample={sample}"
        )


def validate_input_schema(
    df: pd.DataFrame,
    market: str,
    file_name: str,
) -> None:
    validate_no_duplicate_columns(df, f"{file_name} input")

    if market == "moneyline":
        validate_required_columns(
            df,
            MONEYLINE_REQUIRED_COLUMNS,
            f"{file_name} moneyline input",
        )
        validate_decimal_odds(
            df,
            [
                "home_dk_decimal_moneyline",
                "away_dk_decimal_moneyline",
            ],
            f"{file_name} moneyline input",
        )

    elif market == "run_line":
        validate_required_columns(
            df,
            RUN_LINE_REQUIRED_COLUMNS,
            f"{file_name} run_line input",
        )
        validate_decimal_odds(
            df,
            [
                "home_dk_run_line_decimal",
                "away_dk_run_line_decimal",
            ],
            f"{file_name} run_line input",
        )

    elif market == "total":
        validate_required_columns(
            df,
            TOTAL_REQUIRED_COLUMNS,
            f"{file_name} total input",
        )
        validate_decimal_odds(
            df,
            [
                "dk_total_over_decimal",
                "dk_total_under_decimal",
            ],
            f"{file_name} total input",
        )

    else:
        raise ValueError(
            f"{file_name} unknown market for schema validation: {market}"
        )

    validate_probability_contract(
        df,
        market,
        f"{file_name} {market} input",
    )


def write_csv_checked(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    validate_no_duplicate_columns(df, f"{output_path} output")
    df.to_csv(output_path, index=False)


# =========================
# EDGE HELPERS
# =========================

def market_break_even_probability(decimal_odds) -> pd.Series:
    decimal_odds = pd.to_numeric(decimal_odds, errors="coerce")
    out = pd.Series(np.nan, index=decimal_odds.index, dtype="float64")

    valid = (
        decimal_odds.notna()
        & np.isfinite(decimal_odds)
        & (decimal_odds > 1.0)
    )
    out.loc[valid] = 1.0 / decimal_odds.loc[valid]
    return out


def probability_edge(model_probability, market_break_even_prob) -> pd.Series:
    model_probability = pd.to_numeric(
        model_probability,
        errors="coerce",
    )
    market_break_even_prob = pd.to_numeric(
        market_break_even_prob,
        errors="coerce",
    )

    out = pd.Series(
        np.nan,
        index=model_probability.index,
        dtype="float64",
    )

    valid = (
        model_probability.notna()
        & market_break_even_prob.notna()
        & np.isfinite(model_probability)
        & np.isfinite(market_break_even_prob)
    )

    out.loc[valid] = (
        model_probability.loc[valid]
        - market_break_even_prob.loc[valid]
    )
    return out


def conditional_win_probability(
    win_probability,
    loss_probability,
) -> pd.Series:
    win_probability = pd.to_numeric(
        win_probability,
        errors="coerce",
    )
    loss_probability = pd.to_numeric(
        loss_probability,
        errors="coerce",
    )

    resolved = win_probability + loss_probability
    out = pd.Series(
        np.nan,
        index=win_probability.index,
        dtype="float64",
    )

    valid = (
        win_probability.notna()
        & loss_probability.notna()
        & resolved.notna()
        & np.isfinite(win_probability)
        & np.isfinite(loss_probability)
        & np.isfinite(resolved)
        & (resolved > 0)
    )

    out.loc[valid] = (
        win_probability.loc[valid]
        / resolved.loc[valid]
    )
    return out


def count_null_edges(df: pd.DataFrame, columns: list) -> int:
    return sum(
        int(df[col].isna().sum())
        for col in columns
        if col in df.columns
    )


# =========================
# PROBABILITY EDGE COMPUTATION
# =========================

def compute_moneyline(df: pd.DataFrame):
    df["home_market_break_even_prob"] = market_break_even_probability(
        df["home_dk_decimal_moneyline"]
    )
    df["away_market_break_even_prob"] = market_break_even_probability(
        df["away_dk_decimal_moneyline"]
    )

    df["home_edge_prob_moneyline"] = probability_edge(
        df["home_model_prob_moneyline"],
        df["home_market_break_even_prob"],
    )
    df["away_edge_prob_moneyline"] = probability_edge(
        df["away_model_prob_moneyline"],
        df["away_market_break_even_prob"],
    )

    edge_columns = [
        "home_edge_prob_moneyline",
        "away_edge_prob_moneyline",
    ]
    return df, count_null_edges(df, edge_columns)


def compute_run_line(df: pd.DataFrame):
    df["home_market_break_even_prob_run_line"] = market_break_even_probability(
        df["home_dk_run_line_decimal"]
    )
    df["away_market_break_even_prob_run_line"] = market_break_even_probability(
        df["away_dk_run_line_decimal"]
    )

    df["home_edge_prob_run_line"] = probability_edge(
        df["home_model_prob_run_line"],
        df["home_market_break_even_prob_run_line"],
    )
    df["away_edge_prob_run_line"] = probability_edge(
        df["away_model_prob_run_line"],
        df["away_market_break_even_prob_run_line"],
    )

    edge_columns = [
        "home_edge_prob_run_line",
        "away_edge_prob_run_line",
    ]
    return df, count_null_edges(df, edge_columns)


def compute_total(df: pd.DataFrame):
    df["over_conditional_win_prob"] = conditional_win_probability(
        df["over_model_prob_total_win"],
        df["over_model_prob_total_loss"],
    )
    df["under_conditional_win_prob"] = conditional_win_probability(
        df["under_model_prob_total_win"],
        df["under_model_prob_total_loss"],
    )

    df["over_market_break_even_prob_total"] = market_break_even_probability(
        df["dk_total_over_decimal"]
    )
    df["under_market_break_even_prob_total"] = market_break_even_probability(
        df["dk_total_under_decimal"]
    )

    df["over_edge_prob_total"] = probability_edge(
        df["over_conditional_win_prob"],
        df["over_market_break_even_prob_total"],
    )
    df["under_edge_prob_total"] = probability_edge(
        df["under_conditional_win_prob"],
        df["under_market_break_even_prob_total"],
    )

    edge_columns = [
        "over_edge_prob_total",
        "under_edge_prob_total",
    ]
    return df, count_null_edges(df, edge_columns)


# =========================
# MAIN
# =========================

def main():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== compute_edges RUN {_now()} ===\n")

    summary = {
        "files_processed": 0,
        "rows_processed": 0,
        "moneyline_files": 0,
        "run_line_files": 0,
        "total_files": 0,
        "skipped": 0,
        "null_edges": 0,
        "schema_errors": 0,
        "errors": 0,
    }
    per_file = []

    _log(f"INPUT_DIR : {INPUT_DIR}")
    _log(f"OUTPUT_DIR: {OUTPUT_DIR}")
    _log(
        "EDGE DEFINITION: model probability minus sportsbook break-even probability"
    )

    input_files = sorted(INPUT_DIR.glob("*.csv"))
    _log(f"Files found: {len(input_files)}")

    for out_file in OUTPUT_DIR.glob("*.csv"):
        out_file.unlink()

    for input_file in input_files:
        name = input_file.name.lower()
        market = None
        pf = {
            "name": input_file.name,
            "market": "unknown",
            "rows": 0,
            "null_edges": 0,
            "status": "ok",
        }

        if "moneyline" in name:
            market = "moneyline"
        elif "run_line" in name:
            market = "run_line"
        elif "total" in name:
            market = "total"
        else:
            _log(f"SKIP unrecognized file: {input_file.name}")
            pf["status"] = "skipped"
            summary["skipped"] += 1
            per_file.append(pf)
            continue

        pf["market"] = market
        _log(f"--- FILE: {input_file.name}  market={market}")

        try:
            df = read_csv_guarded(input_file)

            if df.empty:
                _log(f"{input_file.name} empty — skipping")
                pf["status"] = "empty"
                summary["skipped"] += 1
                per_file.append(pf)
                continue

            try:
                validate_input_schema(
                    df,
                    market,
                    input_file.name,
                )
            except Exception as schema_error:
                _log(
                    f"{input_file.name} SCHEMA FAILED: {schema_error}",
                    "ERROR",
                )
                pf["status"] = "schema_error"
                summary["schema_errors"] += 1
                per_file.append(pf)
                continue

            pf["rows"] = len(df)
            summary["rows_processed"] += len(df)

            if market == "moneyline":
                df, null_edges = compute_moneyline(df)
                summary["moneyline_files"] += 1
            elif market == "run_line":
                df, null_edges = compute_run_line(df)
                summary["run_line_files"] += 1
            else:
                df, null_edges = compute_total(df)
                summary["total_files"] += 1

            pf["null_edges"] = null_edges
            summary["null_edges"] += null_edges

            if null_edges > 0:
                _log(
                    f"{input_file.name} | {null_edges} null probability edges",
                    "WARN",
                )

            output_path = OUTPUT_DIR / input_file.name
            write_csv_checked(df, output_path)

            summary["files_processed"] += 1
            _log(
                f"WROTE: {output_path} "
                f"({len(df)} rows, {null_edges} null probability edges)"
            )

        except Exception as e:
            _log(
                f"{input_file.name} FAILED: {e}\n{traceback.format_exc()}",
                "ERROR",
            )
            pf["status"] = "error"
            summary["errors"] += 1

        per_file.append(pf)

    _write_summary(summary, per_file)

    if summary["errors"] > 0 or summary["schema_errors"] > 0:
        print(
            "compute_edges completed with errors. "
            f"errors={summary['errors']} "
            f"schema_errors={summary['schema_errors']}"
        )
        raise SystemExit(1)

    print("compute_edges complete.")


if __name__ == "__main__":
    main()
