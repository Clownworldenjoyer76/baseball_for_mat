#!/usr/bin/env python3
# Shared helpers for MLB juice application stages.

from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

import math
import pandas as pd


AUDIT_COLUMNS = [
    "date",
    "game_id",
    "market",
    "side",
    "dk_american",
    "dk_decimal",
    "fair_decimal",
    "juiced_decimal",
    "juiced_prob",
    "normalized_prob",
    "status",
]


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def make_logger(log_file: Path) -> Callable[[str, str], None]:
    def log(msg: str, level: str = "INFO") -> None:
        with open(log_file, "a", encoding="utf-8") as handle:
            handle.write(
                f"{utc_now()} | {level:<5} | {msg.rstrip()}\n"
            )
    return log


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


def read_csv_validated(
    path: Path,
    required_columns: list,
    label: str,
) -> pd.DataFrame:
    df = pd.read_csv(path)
    validate_no_duplicate_columns(df, label)
    validate_required_columns(df, required_columns, label)
    return df


def write_csv_validated(
    df: pd.DataFrame,
    path: Path,
    label: str,
) -> None:
    validate_no_duplicate_columns(df, label)
    df.to_csv(path, index=False)


def require_nonempty_columns(
    df: pd.DataFrame,
    columns: list,
    label: str,
) -> None:
    fully_empty = []
    for col in columns:
        if col not in df.columns:
            raise ValueError(
                f"{label} missing required DK odds column: {col}"
            )
        values = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        )
        if df[col].isna().all() or values.isna().all():
            fully_empty.append(col)
    if fully_empty:
        raise ValueError(
            f"{label} has fully empty DK odds columns: {fully_empty}"
        )


def log_stage_inputs(
    log,
    input_dir: Path,
    source_merge_dir: Path,
    juice_file: Path,
) -> None:
    log(f"INPUT_DIR : {input_dir}")
    log(f"SOURCE_MERGE_DIR: {source_merge_dir}")
    log(f"JUICE_FILE: {juice_file}")


def load_juice_config(
    path: Path,
    required_columns: list,
    *,
    categorical_columns: tuple[str, ...],
) -> pd.DataFrame:
    label = f"juice file {path}"
    df = read_csv_validated(path, required_columns, label)
    for col in ("band_min", "band_max", "extra_juice"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in categorical_columns:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
            )
    return df


def normalize_pair(
    left_prob: float,
    right_prob: float,
) -> tuple[float, float] | None:
    total = left_prob + right_prob
    if not math.isfinite(total) or total <= 0:
        return None
    return left_prob / total, right_prob / total


def write_audit(
    audit_rows: list,
    audit_path: Path,
    log,
) -> None:
    pd.DataFrame(
        audit_rows,
        columns=AUDIT_COLUMNS,
    ).to_csv(audit_path, index=False)
    log(f"WROTE AUDIT: {audit_path}")


def fail_on_summary_errors(
    summary: dict,
    process_name: str,
) -> None:
    if summary["errors"] > 0 or summary["schema_errors"] > 0:
        print(
            f"{process_name} completed with errors. "
            f"errors={summary['errors']} "
            f"schema_errors={summary['schema_errors']}"
        )
        raise SystemExit(1)
