from pathlib import Path
import py_compile
import importlib.util
import pandas as pd
import numpy as np

src = Path("/mnt/data/compute_ev_kelly.py")
out_dir = Path("/mnt/data/mlb_todo5_updated")
out_dir.mkdir(parents=True, exist_ok=True)
out = out_dir / "compute_ev_kelly.py"

updated = r'''#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/03_edges/compute_ev_kelly.py

import traceback
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

INPUT_DIR = Path("docs/win/baseball/mlb/03_edges")
OUTPUT_DIR = Path("docs/win/baseball/mlb/03_edges/ev_kelly")
AUDIT_DIR = OUTPUT_DIR / "audit"
ERROR_DIR = Path("docs/win/baseball/mlb/errors/03_edges")
LOG_FILE = ERROR_DIR / "compute_ev_kelly.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

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

SCRIPT_NAME = "compute_ev_kelly.py"
STAGE_NAME = "03_edges_ev_kelly"
PROB_TOLERANCE = 1e-6
SIGN_TOLERANCE = 1e-10


PROBABILITY_SOURCES = {
    "moneyline": {
        "home": "home_model_prob_moneyline",
        "away": "away_model_prob_moneyline",
    },
    "run_line": {
        "home": "home_model_prob_run_line",
        "away": "away_model_prob_run_line",
    },
    "total": {
        "over": "over_model_prob_total_win",
        "under": "under_model_prob_total_win",
    },
}


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

FORBIDDEN_RUN_LINE_COLUMNS = [
    "home_run_line_prob",
    "away_run_line_prob",
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
    with open(LOG_FILE, "a", encoding="utf-8") as log_f:
        log_f.write(f"{_now()} | {level:<5} | {msg.rstrip()}\n")


def _write_summary(summary: dict, per_file: list) -> None:
    lines = [
        "",
        "=" * 60,
        f"SUMMARY  {_now()}",
        "=" * 60,
        f"  files_processed              : {summary['files_processed']}",
        f"  rows_processed               : {summary['rows_processed']}",
        f"  moneyline_files              : {summary['moneyline_files']}",
        f"  run_line_files               : {summary['run_line_files']}",
        f"  total_files                  : {summary['total_files']}",
        f"  skipped                      : {summary['skipped']}",
        f"  schema_errors                : {summary['schema_errors']}",
        f"  neg_kelly_clipped            : {summary['neg_kelly_clipped']}",
        f"  probability_source_mismatches: "
        f"{summary['probability_source_mismatches']}",
        f"  audit_rows                   : {summary['audit_rows']}",
        f"  errors                       : {summary['errors']}",
        "",
        f"  {'file':<48} {'market':<12} {'rows':>5} "
        f"{'neg_kelly':>10} {'status':>14}",
    ]

    for pf in per_file:
        lines.append(
            f"  {pf['name']:<48} {pf['market']:<12} {pf['rows']:>5} "
            f"{pf['neg_kelly']:>10} {pf['status']:>14}"
        )

    status = (
        "SUCCESS"
        if summary["errors"] == 0 and summary["schema_errors"] == 0
        else "COMPLETED WITH ERRORS"
    )
    lines += ["", f"STATUS: {status}", "=" * 60]

    with open(LOG_FILE, "a", encoding="utf-8") as log_f:
        log_f.write("\n".join(lines) + "\n")


# =========================
# SCHEMA GUARDS
# =========================

def duplicate_columns(columns) -> list:
    seen = set()
    dupes = []

    for col in columns:
        if col in seen and col not in dupes:
            dupes.append(col)
        seen.add(col)

    return dupes


def assert_no_duplicate_columns(df: pd.DataFrame, label: str) -> None:
    dupes = duplicate_columns(list(df.columns))

    if dupes:
        raise ValueError(f"{label} has duplicate columns: {dupes}")


def assert_required_columns(
    df: pd.DataFrame,
    required_columns: list,
    label: str,
) -> None:
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def assert_forbidden_columns_absent(
    df: pd.DataFrame,
    forbidden_columns: list,
    label: str,
) -> None:
    present = [col for col in forbidden_columns if col in df.columns]

    if present:
        raise ValueError(
            f"{label} contains obsolete forbidden columns: {present}. "
            "Use home_model_prob_run_line / away_model_prob_run_line."
        )


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
        sample = (
            df.loc[bad, ["game_id"] + columns]
            .head(10)
            .to_dict("records")
        )
        raise ValueError(
            f"{label} has missing, non-finite, or out-of-range canonical "
            f"probabilities; bad_rows={int(bad.sum())}; sample={sample}"
        )


def validate_probability_pair(
    df: pd.DataFrame,
    col_a: str,
    col_b: str,
    label: str,
) -> None:
    validate_probability_values(df, [col_a, col_b], label)

    a = pd.to_numeric(df[col_a], errors="coerce")
    b = pd.to_numeric(df[col_b], errors="coerce")
    bad = (a + b - 1.0).abs() > PROB_TOLERANCE

    if bad.any():
        sample = (
            df.loc[bad, ["game_id", col_a, col_b]]
            .head(10)
            .to_dict("records")
        )
        raise ValueError(
            f"{label} canonical probability pair does not sum to 1.0 "
            f"within tolerance; bad_rows={int(bad.sum())}; sample={sample}"
        )


def validate_total_probability_contract(
    df: pd.DataFrame,
    label: str,
) -> None:
    cols = [
        "over_model_prob_total_win",
        "over_model_prob_total_loss",
        "under_model_prob_total_win",
        "under_model_prob_total_loss",
        "total_model_prob_push",
    ]
    validate_probability_values(df, cols, label)

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
    push = pd.to_numeric(
        df["total_model_prob_push"], errors="coerce"
    )

    bad = (
        ((over_win + over_loss + push - 1.0).abs() > PROB_TOLERANCE)
        | ((under_win + under_loss + push - 1.0).abs() > PROB_TOLERANCE)
        | ((under_win - over_loss).abs() > PROB_TOLERANCE)
        | ((under_loss - over_win).abs() > PROB_TOLERANCE)
    )

    if bad.any():
        sample = (
            df.loc[bad, ["game_id"] + cols]
            .head(10)
            .to_dict("records")
        )
        raise ValueError(
            f"{label} totals canonical probability contract failed; "
            f"bad_rows={int(bad.sum())}; sample={sample}"
        )

    over_resolved = over_win + over_loss
    under_resolved = under_win + under_loss
    invalid_resolved = (
        ~np.isfinite(over_resolved)
        | ~np.isfinite(under_resolved)
        | (over_resolved <= 0)
        | (under_resolved <= 0)
    )

    if invalid_resolved.any():
        sample = (
            df.loc[
                invalid_resolved,
                [
                    "game_id",
                    "over_model_prob_total_win",
                    "over_model_prob_total_loss",
                    "under_model_prob_total_win",
                    "under_model_prob_total_loss",
                    "total_model_prob_push",
                ],
            ]
            .head(10)
            .to_dict("records")
        )
        raise ValueError(
            f"{label} totals have no resolved probability mass "
            f"(p_win + p_loss must be > 0); "
            f"bad_rows={int(invalid_resolved.sum())}; sample={sample}"
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
        sample = (
            df.loc[bad, ["game_id"] + columns]
            .head(10)
            .to_dict("records")
        )
        raise ValueError(
            f"{label} has missing, non-finite, or invalid decimal odds; "
            f"bad_rows={int(bad.sum())}; sample={sample}"
        )


def validate_input_schema(
    df: pd.DataFrame,
    market: str,
    file_name: str,
) -> None:
    assert_no_duplicate_columns(df, f"{file_name} input")

    if market == "moneyline":
        assert_required_columns(
            df,
            MONEYLINE_REQUIRED_COLUMNS,
            f"{file_name} moneyline input",
        )
        validate_probability_pair(
            df,
            "home_model_prob_moneyline",
            "away_model_prob_moneyline",
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
        assert_required_columns(
            df,
            RUN_LINE_REQUIRED_COLUMNS,
            f"{file_name} run_line input",
        )
        assert_forbidden_columns_absent(
            df,
            FORBIDDEN_RUN_LINE_COLUMNS,
            f"{file_name} run_line input",
        )
        validate_probability_pair(
            df,
            "home_model_prob_run_line",
            "away_model_prob_run_line",
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
        assert_required_columns(
            df,
            TOTAL_REQUIRED_COLUMNS,
            f"{file_name} total input",
        )
        validate_total_probability_contract(
            df,
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


def write_csv_checked(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    assert_no_duplicate_columns(df, f"{output_path} output")
    df.to_csv(output_path, index=False)


# =========================
# EV / KELLY HELPERS
# =========================

def compute_binary_ev(probability, decimal_odds) -> pd.Series:
    probability = pd.to_numeric(probability, errors="coerce")
    decimal_odds = pd.to_numeric(decimal_odds, errors="coerce")
    return (probability * decimal_odds) - 1.0


def compute_binary_kelly_raw(
    probability,
    decimal_odds,
) -> pd.Series:
    probability = pd.to_numeric(probability, errors="coerce")
    decimal_odds = pd.to_numeric(decimal_odds, errors="coerce")

    b = decimal_odds - 1.0
    q = 1.0 - probability

    raw_kelly = pd.Series(
        np.nan,
        index=probability.index,
        dtype="float64",
    )
    valid = (
        probability.notna()
        & decimal_odds.notna()
        & np.isfinite(probability)
        & np.isfinite(decimal_odds)
        & (decimal_odds > 1.0)
    )

    raw_kelly.loc[valid] = (
        (b.loc[valid] * probability.loc[valid])
        - q.loc[valid]
    ) / b.loc[valid]

    return raw_kelly


def compute_total_ev(
    p_win,
    p_loss,
    decimal_odds,
) -> pd.Series:
    p_win = pd.to_numeric(p_win, errors="coerce")
    p_loss = pd.to_numeric(p_loss, errors="coerce")
    decimal_odds = pd.to_numeric(decimal_odds, errors="coerce")

    b = decimal_odds - 1.0
    return (p_win * b) - p_loss


def compute_total_kelly_raw(
    p_win,
    p_loss,
    decimal_odds,
    game_id,
    label: str,
) -> pd.Series:
    p_win = pd.to_numeric(p_win, errors="coerce")
    p_loss = pd.to_numeric(p_loss, errors="coerce")
    decimal_odds = pd.to_numeric(decimal_odds, errors="coerce")
    resolved = p_win + p_loss
    b = decimal_odds - 1.0

    invalid = (
        p_win.isna()
        | p_loss.isna()
        | decimal_odds.isna()
        | ~np.isfinite(p_win)
        | ~np.isfinite(p_loss)
        | ~np.isfinite(decimal_odds)
        | ~np.isfinite(resolved)
        | (decimal_odds <= 1.0)
        | (resolved <= 0)
    )

    if invalid.any():
        audit = pd.DataFrame(
            {
                "game_id": game_id,
                "p_win": p_win,
                "p_loss": p_loss,
                "decimal_odds": decimal_odds,
            }
        )
        sample = audit.loc[invalid].head(10).to_dict("records")
        raise ValueError(
            f"{label} invalid totals Kelly inputs; "
            f"p_win + p_loss must be > 0 and decimal odds must be > 1; "
            f"bad_rows={int(invalid.sum())}; sample={sample}"
        )

    numerator = (p_win * b) - p_loss
    denominator = b * resolved
    return numerator / denominator


def clip_negative_kelly(
    raw_kelly: pd.Series,
    file_name: str,
    label: str,
) -> tuple[pd.Series, int]:
    raw_kelly = pd.to_numeric(raw_kelly, errors="coerce")
    negative = raw_kelly.notna() & (raw_kelly < 0)
    count = int(negative.sum())

    if count:
        _log(
            f"{file_name} | {label} | {count} negative raw Kelly values "
            f"clipped to 0 after audit calculation",
            "INFO",
        )

    return raw_kelly.clip(lower=0), count


def validate_ev_kelly_sign_consistency(
    game_id,
    ev,
    raw_kelly,
    label: str,
) -> None:
    ev = pd.to_numeric(ev, errors="coerce")
    raw_kelly = pd.to_numeric(raw_kelly, errors="coerce")

    invalid_numeric = (
        ev.isna()
        | raw_kelly.isna()
        | ~np.isfinite(ev)
        | ~np.isfinite(raw_kelly)
    )

    positive_bad = (ev > SIGN_TOLERANCE) & (raw_kelly <= SIGN_TOLERANCE)
    negative_bad = (ev < -SIGN_TOLERANCE) & (raw_kelly >= -SIGN_TOLERANCE)
    zero_bad = (
        (ev.abs() <= SIGN_TOLERANCE)
        & (raw_kelly.abs() > SIGN_TOLERANCE)
    )

    bad = invalid_numeric | positive_bad | negative_bad | zero_bad

    if bad.any():
        audit = pd.DataFrame(
            {
                "game_id": game_id,
                "ev": ev,
                "raw_kelly": raw_kelly,
            }
        )
        sample = audit.loc[bad].head(10).to_dict("records")
        raise ValueError(
            f"{label} EV/raw-Kelly sign consistency failed; "
            f"bad_rows={int(bad.sum())}; sample={sample}"
        )


def probability_basis_columns(
    df: pd.DataFrame,
    side: str,
    source_col: str,
) -> tuple[pd.Series, dict]:
    probability = pd.to_numeric(df[source_col], errors="coerce")

    columns = {
        f"{side}_prob_for_ev": probability,
        f"{side}_prob_for_kelly": probability,
        f"{side}_ev_probability_source": source_col,
        f"{side}_kelly_probability_source": source_col,
    }

    return probability, columns


def probability_source_mismatch_count(
    df: pd.DataFrame,
    sides: list,
) -> int:
    mismatch = pd.Series(False, index=df.index)

    for side in sides:
        ev_source = df[f"{side}_ev_probability_source"].astype(str)
        kelly_source = df[f"{side}_kelly_probability_source"].astype(str)

        prob_ev = pd.to_numeric(
            df[f"{side}_prob_for_ev"],
            errors="coerce",
        )
        prob_kelly = pd.to_numeric(
            df[f"{side}_prob_for_kelly"],
            errors="coerce",
        )

        mismatch = (
            mismatch
            | (ev_source != kelly_source)
            | prob_ev.isna()
            | prob_kelly.isna()
            | ((prob_ev - prob_kelly).abs() > PROB_TOLERANCE)
        )

    return int(mismatch.sum())


def add_columns_at_once(
    df: pd.DataFrame,
    columns: dict,
) -> pd.DataFrame:
    calculated = pd.DataFrame(columns, index=df.index)
    overlapping = [
        col for col in calculated.columns if col in df.columns
    ]

    if overlapping:
        df = df.drop(columns=overlapping)

    return pd.concat([df, calculated], axis=1).copy()


def audit_rows(
    df: pd.DataFrame,
    market: str,
    sides: list,
    decimal_cols: dict,
    ev_cols: dict,
    raw_kelly_cols: dict,
    kelly_cols: dict,
) -> list:
    records = []

    for side in sides:
        for _, row in df.iterrows():
            records.append(
                {
                    "date": row.get("game_date"),
                    "game_id": row.get("game_id"),
                    "market": market,
                    "side": side,
                    "prob_for_ev": row.get(f"{side}_prob_for_ev"),
                    "prob_for_kelly": row.get(f"{side}_prob_for_kelly"),
                    "dk_decimal": row.get(decimal_cols[side]),
                    "ev": row.get(ev_cols[side]),
                    "raw_kelly": row.get(raw_kelly_cols[side]),
                    "kelly": row.get(kelly_cols[side]),
                    "ev_probability_source": row.get(
                        f"{side}_ev_probability_source"
                    ),
                    "kelly_probability_source": row.get(
                        f"{side}_kelly_probability_source"
                    ),
                    "status": "ok",
                }
            )

    return records


# =========================
# MARKET PROCESSORS
# =========================

def process_moneyline(
    df: pd.DataFrame,
    file_name: str,
):
    home_prob, home_basis = probability_basis_columns(
        df,
        "home",
        PROBABILITY_SOURCES["moneyline"]["home"],
    )
    away_prob, away_basis = probability_basis_columns(
        df,
        "away",
        PROBABILITY_SOURCES["moneyline"]["away"],
    )

    home_ev = compute_binary_ev(
        home_prob,
        df["home_dk_decimal_moneyline"],
    )
    away_ev = compute_binary_ev(
        away_prob,
        df["away_dk_decimal_moneyline"],
    )

    home_raw_kelly = compute_binary_kelly_raw(
        home_prob,
        df["home_dk_decimal_moneyline"],
    )
    away_raw_kelly = compute_binary_kelly_raw(
        away_prob,
        df["away_dk_decimal_moneyline"],
    )

    validate_ev_kelly_sign_consistency(
        df["game_id"],
        home_ev,
        home_raw_kelly,
        f"{file_name} home moneyline",
    )
    validate_ev_kelly_sign_consistency(
        df["game_id"],
        away_ev,
        away_raw_kelly,
        f"{file_name} away moneyline",
    )

    home_kelly, home_neg = clip_negative_kelly(
        home_raw_kelly,
        file_name,
        "home moneyline",
    )
    away_kelly, away_neg = clip_negative_kelly(
        away_raw_kelly,
        file_name,
        "away moneyline",
    )

    df = add_columns_at_once(
        df,
        {
            **home_basis,
            **away_basis,
            "home_ml_ev": home_ev,
            "away_ml_ev": away_ev,
            "home_ml_kelly_raw": home_raw_kelly,
            "away_ml_kelly_raw": away_raw_kelly,
            "home_ml_kelly": home_kelly,
            "away_ml_kelly": away_kelly,
        },
    )

    mismatch_count = probability_source_mismatch_count(
        df,
        ["home", "away"],
    )
    if mismatch_count:
        raise ValueError(
            f"{file_name} EV/Kelly probability basis mismatch rows="
            f"{mismatch_count}"
        )

    audit = audit_rows(
        df,
        "moneyline",
        ["home", "away"],
        {
            "home": "home_dk_decimal_moneyline",
            "away": "away_dk_decimal_moneyline",
        },
        {
            "home": "home_ml_ev",
            "away": "away_ml_ev",
        },
        {
            "home": "home_ml_kelly_raw",
            "away": "away_ml_kelly_raw",
        },
        {
            "home": "home_ml_kelly",
            "away": "away_ml_kelly",
        },
    )

    return df, home_neg + away_neg, mismatch_count, audit


def process_run_line(
    df: pd.DataFrame,
    file_name: str,
):
    home_prob, home_basis = probability_basis_columns(
        df,
        "home",
        PROBABILITY_SOURCES["run_line"]["home"],
    )
    away_prob, away_basis = probability_basis_columns(
        df,
        "away",
        PROBABILITY_SOURCES["run_line"]["away"],
    )

    home_ev = compute_binary_ev(
        home_prob,
        df["home_dk_run_line_decimal"],
    )
    away_ev = compute_binary_ev(
        away_prob,
        df["away_dk_run_line_decimal"],
    )

    home_raw_kelly = compute_binary_kelly_raw(
        home_prob,
        df["home_dk_run_line_decimal"],
    )
    away_raw_kelly = compute_binary_kelly_raw(
        away_prob,
        df["away_dk_run_line_decimal"],
    )

    validate_ev_kelly_sign_consistency(
        df["game_id"],
        home_ev,
        home_raw_kelly,
        f"{file_name} home run line",
    )
    validate_ev_kelly_sign_consistency(
        df["game_id"],
        away_ev,
        away_raw_kelly,
        f"{file_name} away run line",
    )

    home_kelly, home_neg = clip_negative_kelly(
        home_raw_kelly,
        file_name,
        "home run line",
    )
    away_kelly, away_neg = clip_negative_kelly(
        away_raw_kelly,
        file_name,
        "away run line",
    )

    df = add_columns_at_once(
        df,
        {
            **home_basis,
            **away_basis,
            "home_rl_ev": home_ev,
            "away_rl_ev": away_ev,
            "home_rl_kelly_raw": home_raw_kelly,
            "away_rl_kelly_raw": away_raw_kelly,
            "home_rl_kelly": home_kelly,
            "away_rl_kelly": away_kelly,
        },
    )

    mismatch_count = probability_source_mismatch_count(
        df,
        ["home", "away"],
    )
    if mismatch_count:
        raise ValueError(
            f"{file_name} EV/Kelly probability basis mismatch rows="
            f"{mismatch_count}"
        )

    audit = audit_rows(
        df,
        "run_line",
        ["home", "away"],
        {
            "home": "home_dk_run_line_decimal",
            "away": "away_dk_run_line_decimal",
        },
        {
            "home": "home_rl_ev",
            "away": "away_rl_ev",
        },
        {
            "home": "home_rl_kelly_raw",
            "away": "away_rl_kelly_raw",
        },
        {
            "home": "home_rl_kelly",
            "away": "away_rl_kelly",
        },
    )

    return df, home_neg + away_neg, mismatch_count, audit


def process_total(
    df: pd.DataFrame,
    file_name: str,
):
    over_prob, over_basis = probability_basis_columns(
        df,
        "over",
        PROBABILITY_SOURCES["total"]["over"],
    )
    under_prob, under_basis = probability_basis_columns(
        df,
        "under",
        PROBABILITY_SOURCES["total"]["under"],
    )

    over_loss = pd.to_numeric(
        df["over_model_prob_total_loss"],
        errors="coerce",
    )
    under_loss = pd.to_numeric(
        df["under_model_prob_total_loss"],
        errors="coerce",
    )

    over_ev = compute_total_ev(
        over_prob,
        over_loss,
        df["dk_total_over_decimal"],
    )
    under_ev = compute_total_ev(
        under_prob,
        under_loss,
        df["dk_total_under_decimal"],
    )

    over_raw_kelly = compute_total_kelly_raw(
        over_prob,
        over_loss,
        df["dk_total_over_decimal"],
        df["game_id"],
        f"{file_name} over total",
    )
    under_raw_kelly = compute_total_kelly_raw(
        under_prob,
        under_loss,
        df["dk_total_under_decimal"],
        df["game_id"],
        f"{file_name} under total",
    )

    validate_ev_kelly_sign_consistency(
        df["game_id"],
        over_ev,
        over_raw_kelly,
        f"{file_name} over total",
    )
    validate_ev_kelly_sign_consistency(
        df["game_id"],
        under_ev,
        under_raw_kelly,
        f"{file_name} under total",
    )

    over_kelly, over_neg = clip_negative_kelly(
        over_raw_kelly,
        file_name,
        "over total",
    )
    under_kelly, under_neg = clip_negative_kelly(
        under_raw_kelly,
        file_name,
        "under total",
    )

    df = add_columns_at_once(
        df,
        {
            **over_basis,
            **under_basis,
            "over_ev": over_ev,
            "under_ev": under_ev,
            "over_kelly_raw": over_raw_kelly,
            "under_kelly_raw": under_raw_kelly,
            "over_kelly": over_kelly,
            "under_kelly": under_kelly,
        },
    )

    mismatch_count = probability_source_mismatch_count(
        df,
        ["over", "under"],
    )
    if mismatch_count:
        raise ValueError(
            f"{file_name} EV/Kelly probability basis mismatch rows="
            f"{mismatch_count}"
        )

    audit = audit_rows(
        df,
        "total",
        ["over", "under"],
        {
            "over": "dk_total_over_decimal",
            "under": "dk_total_under_decimal",
        },
        {
            "over": "over_ev",
            "under": "under_ev",
        },
        {
            "over": "over_kelly_raw",
            "under": "under_kelly_raw",
        },
        {
            "over": "over_kelly",
            "under": "under_kelly",
        },
    )

    return df, over_neg + under_neg, mismatch_count, audit


# =========================
# MAIN
# =========================

def main():
    with open(LOG_FILE, "w", encoding="utf-8") as log_f:
        log_f.write(f"=== compute_ev_kelly RUN {_now()} ===\n")

    summary = {
        "files_processed": 0,
        "rows_processed": 0,
        "moneyline_files": 0,
        "run_line_files": 0,
        "total_files": 0,
        "skipped": 0,
        "schema_errors": 0,
        "neg_kelly_clipped": 0,
        "probability_source_mismatches": 0,
        "audit_rows": 0,
        "errors": 0,
    }

    per_file = []
    all_audit_rows = []

    _log(f"INPUT_DIR : {INPUT_DIR}")
    _log(f"OUTPUT_DIR: {OUTPUT_DIR}")
    _log(
        "EV AND KELLY USE THE SAME CANONICAL MODEL PROBABILITY BASIS"
    )
    _log(
        "Totals EV/Kelly are push-aware: pushes are neither wins nor losses."
    )

    input_files = sorted(INPUT_DIR.glob("*.csv"))
    _log(f"Files found: {len(input_files)}")

    for out_file in OUTPUT_DIR.glob("*.csv"):
        out_file.unlink()

    for old_audit in AUDIT_DIR.glob("*.csv"):
        old_audit.unlink()

    for input_file in input_files:
        name = input_file.name.lower()
        market = None

        pf = {
            "name": input_file.name,
            "market": "unknown",
            "rows": 0,
            "neg_kelly": 0,
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
                (
                    df,
                    neg_kelly,
                    mismatch_count,
                    audit,
                ) = process_moneyline(
                    df,
                    input_file.name,
                )
                summary["moneyline_files"] += 1

            elif market == "run_line":
                (
                    df,
                    neg_kelly,
                    mismatch_count,
                    audit,
                ) = process_run_line(
                    df,
                    input_file.name,
                )
                summary["run_line_files"] += 1

            else:
                (
                    df,
                    neg_kelly,
                    mismatch_count,
                    audit,
                ) = process_total(
                    df,
                    input_file.name,
                )
                summary["total_files"] += 1

            pf["neg_kelly"] = neg_kelly
            summary["neg_kelly_clipped"] += neg_kelly
            summary["probability_source_mismatches"] += mismatch_count
            summary["audit_rows"] += len(audit)
            all_audit_rows.extend(audit)

            output_path = OUTPUT_DIR / input_file.name
            write_csv_checked(
                df,
                output_path,
            )

            summary["files_processed"] += 1
            _log(
                f"WROTE: {output_path} "
                f"({len(df)} rows, {neg_kelly} negative Kelly values clipped)"
            )

        except Exception as e:
            _log(
                f"{input_file.name} FAILED: {e}\n"
                f"{traceback.format_exc()}",
                "ERROR",
            )
            pf["status"] = "error"
            summary["errors"] += 1

        per_file.append(pf)

    audit_path = AUDIT_DIR / "post_ev_kelly_audit.csv"
    audit_columns = [
        "date",
        "game_id",
        "market",
        "side",
        "prob_for_ev",
        "prob_for_kelly",
        "dk_decimal",
        "ev",
        "raw_kelly",
        "kelly",
        "ev_probability_source",
        "kelly_probability_source",
        "status",
    ]

    audit_df = pd.DataFrame(
        all_audit_rows,
        columns=audit_columns,
    )

    write_csv_checked(
        audit_df,
        audit_path,
    )

    _log(
        f"WROTE AUDIT: {audit_path} "
        f"rows={len(audit_df)}"
    )

    _write_summary(
        summary,
        per_file,
    )

    if (
        summary["errors"] > 0
        or summary["schema_errors"] > 0
        or summary["probability_source_mismatches"] > 0
    ):
        print(
            "compute_ev_kelly completed with errors. "
            f"errors={summary['errors']} "
            f"schema_errors={summary['schema_errors']} "
            f"probability_source_mismatches="
            f"{summary['probability_source_mismatches']}"
        )
        raise SystemExit(1)

    print(
        "compute_ev_kelly complete. "
        f"files_processed={summary['files_processed']} "
        f"rows_processed={summary['rows_processed']} "
        f"neg_kelly_clipped={summary['neg_kelly_clipped']} "
        f"probability_source_mismatches="
        f"{summary['probability_source_mismatches']} "
        f"schema_errors={summary['schema_errors']} "
        f"errors={summary['errors']}"
    )


if __name__ == "__main__":
    main()
'''

out.write_text(updated, encoding="utf-8")

# ---------- Validation ----------
py_compile.compile(str(out), doraise=True)

spec = importlib.util.spec_from_file_location("compute_ev_kelly_todo5", out)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

# Binary market positive/negative/zero EV and Kelly sign consistency.
p = pd.Series([0.60, 0.40, 0.50])
d = pd.Series([2.00, 2.00, 2.00])
ev = mod.compute_binary_ev(p, d)
raw_kelly = mod.compute_binary_kelly_raw(p, d)
mod.validate_ev_kelly_sign_consistency(
    pd.Series(["pos", "neg", "zero"]),
    ev,
    raw_kelly,
    "binary test",
)
assert ev.iloc[0] > 0 and raw_kelly.iloc[0] > 0
assert ev.iloc[1] < 0 and raw_kelly.iloc[1] < 0
assert abs(ev.iloc[2]) < 1e-12 and abs(raw_kelly.iloc[2]) < 1e-12

# Clipping occurs after raw Kelly is available.
clipped, neg_count = mod.clip_negative_kelly(
    raw_kelly,
    "test.csv",
    "binary test",
)
assert neg_count == 1
assert raw_kelly.iloc[1] < 0
assert clipped.iloc[1] == 0

# Totals with push: EV = p_win*b - p_loss; Kelly denominator excludes push.
p_win = pd.Series([0.40])
p_loss = pd.Series([0.50])
dec = pd.Series([2.40])
game_id = pd.Series(["g1"])
total_ev = mod.compute_total_ev(p_win, p_loss, dec)
total_raw_kelly = mod.compute_total_kelly_raw(
    p_win,
    p_loss,
    dec,
    game_id,
    "totals test",
)
expected_ev = 0.40 * 1.40 - 0.50
expected_kelly = expected_ev / (1.40 * 0.90)
assert abs(total_ev.iloc[0] - expected_ev) < 1e-12
assert abs(total_raw_kelly.iloc[0] - expected_kelly) < 1e-12
mod.validate_ev_kelly_sign_consistency(
    game_id,
    total_ev,
    total_raw_kelly,
    "totals test",
)

# Invalid totals resolved mass hard-fails.
try:
    mod.compute_total_kelly_raw(
        pd.Series([0.0]),
        pd.Series([0.0]),
        pd.Series([2.0]),
        pd.Series(["bad"]),
        "invalid totals test",
    )
    raise AssertionError("p_win + p_loss == 0 did not hard-fail")
except ValueError:
    pass

# Processor source-basis checks.
ml_df = pd.DataFrame({
    "game_id": ["m1"],
    "game_date": ["2026-08-20"],
    "home_model_prob_moneyline": [0.60],
    "away_model_prob_moneyline": [0.40],
    "home_dk_decimal_moneyline": [2.00],
    "away_dk_decimal_moneyline": [2.50],
})
ml_out, _, mismatch, _ = mod.process_moneyline(ml_df, "ml.csv")
assert mismatch == 0
assert ml_out.loc[0, "home_ev_probability_source"] == "home_model_prob_moneyline"
assert ml_out.loc[0, "home_kelly_probability_source"] == "home_model_prob_moneyline"
assert ml_out.loc[0, "home_prob_for_ev"] == ml_out.loc[0, "home_prob_for_kelly"]

tot_df = pd.DataFrame({
    "game_id": ["t1"],
    "game_date": ["2026-08-20"],
    "over_model_prob_total_win": [0.40],
    "over_model_prob_total_loss": [0.50],
    "under_model_prob_total_win": [0.50],
    "under_model_prob_total_loss": [0.40],
    "total_model_prob_push": [0.10],
    "dk_total_over_decimal": [2.40],
    "dk_total_under_decimal": [2.00],
})
tot_out, _, mismatch, _ = mod.process_total(tot_df, "tot.csv")
assert mismatch == 0
assert abs(tot_out.loc[0, "over_ev"] - expected_ev) < 1e-12

# Static removal checks.
text = out.read_text(encoding="utf-8")
for forbidden in [
    "adjusted_ev(",
    "home_ml_raw_ev",
    "away_ml_raw_ev",
    "home_ml_adjusted_ev",
    "away_ml_adjusted_ev",
    "home_rl_raw_ev",
    "away_rl_raw_ev",
    "home_rl_adjusted_ev",
    "away_rl_adjusted_ev",
    "over_raw_ev",
    "under_raw_ev",
    "over_adjusted_ev",
    "under_adjusted_ev",
    "positive_ev_zero_kelly",
    "raw_adjusted_ev_sign_flips",
    "adjusted_only_positive",
    "missing_adj_ev",
    "home_edge_decimal_moneyline",
    "away_edge_decimal_moneyline",
    "home_edge_decimal_run_line",
    "away_edge_decimal_run_line",
    "over_edge_decimal_total",
    "under_edge_decimal_total",
]:
    assert forbidden not in text, f"Forbidden TODO 5 element remains: {forbidden}"

for required in [
    "home_ml_ev",
    "away_ml_ev",
    "home_ml_kelly",
    "away_ml_kelly",
    "home_rl_ev",
    "away_rl_ev",
    "home_rl_kelly",
    "away_rl_kelly",
    "over_ev",
    "under_ev",
    "over_kelly",
    "under_kelly",
    "validate_ev_kelly_sign_consistency",
    "prob_for_ev",
    "prob_for_kelly",
    "ev_probability_source",
    "kelly_probability_source",
]:
    assert required in text, f"Required TODO 5 element missing: {required}"

print("TODO 5 validation passed")
print("- Python syntax")
print("- binary EV/Kelly same canonical probability basis")
print("- negative raw Kelly audited before clipping")
print("- push-aware totals EV/Kelly")
print("- p_win + p_loss == 0 hard-fails")
print("- EV/raw-Kelly sign consistency hard-fails")
print("- adjusted/raw EV system removed")
print("- probability source audit columns retained")
print(f"\nUpdated file: {out}")
