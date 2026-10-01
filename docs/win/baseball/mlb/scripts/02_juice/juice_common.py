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
    allowed_root = Path(
        "docs/win/baseball/mlb/errors/02_juice"
    ).resolve()

    safe_log_file = Path(
        log_file
    ).resolve()

    if not safe_log_file.is_relative_to(
        allowed_root
    ):
        raise ValueError(
            "Refusing juice log path outside "
            f"trusted directory: {log_file}"
        )

    if safe_log_file.suffix.lower() != ".txt":
        raise ValueError(
            f"Refusing unexpected juice log file: {log_file}"
        )

    def log(
        msg: str,
        level: str = "INFO",
    ) -> None:
        with safe_log_file.open(
            "a",
            encoding="utf-8",
        ) as handle:
            handle.write(
                f"{utc_now()} | {level:<5} | "
                f"{msg.rstrip()}\n"
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


def validate_stale_source(
    input_path: Path,
    source_merge_dir: Path,
    log,
) -> None:
    source_path = source_merge_dir / input_path.name
    if not source_path.exists():
        log(
            f"stale_check source_missing source={source_path} "
            f"input={input_path}; continuing",
            "WARN",
        )
        return

    if input_path.stat().st_mtime < source_path.stat().st_mtime:
        raise ValueError(
            f"stale 01_merguiced input: {input_path} "
            f"is older than source merge file {source_path}"
        )


def validate_normalized_probability_pair(
    df: pd.DataFrame,
    left_col: str,
    right_col: str,
    label: str,
    tolerance: float,
    log,
) -> int:
    bad = 0

    for idx, row in df.iterrows():
        left = pd.to_numeric(
            pd.Series([row[left_col]]),
            errors="coerce",
        ).iloc[0]
        right = pd.to_numeric(
            pd.Series([row[right_col]]),
            errors="coerce",
        ).iloc[0]

        if pd.isna(left) and pd.isna(right):
            continue

        if pd.isna(left) or pd.isna(right):
            bad += 1
            log(
                f"{label} row={idx} "
                f"reason=incomplete_normalized_pair "
                f"{left_col}={left} {right_col}={right}",
                "ERROR",
            )
            continue

        total = float(left) + float(right)

        if (
            not math.isfinite(total)
            or abs(total - 1.0) > tolerance
        ):
            bad += 1
            log(
                f"{label} row={idx} "
                f"reason=normalized_sum_invalid total={total}",
                "ERROR",
            )

    return bad


def append_summary_status(
    lines: list[str],
    summary: dict,
    log_file: Path,
) -> None:
    status = (
        "SUCCESS"
        if (
            summary["errors"] == 0
            and summary["schema_errors"] == 0
        )
        else "COMPLETED WITH ERRORS"
    )
    lines.extend(
        [
            "",
            f"STATUS: {status}",
            "=" * 60,
        ]
    )

    with log_file.open(
        "a",
        encoding="utf-8",
    ) as handle:
        handle.write(
            "\n".join(lines) + "\n"
        )


def validate_fav_ud_venue_juice_config(
    juice_df: pd.DataFrame,
    market_label: str,
) -> None:
    invalid = juice_df[
        juice_df["band_min"].isna()
        | juice_df["band_max"].isna()
        | juice_df["extra_juice"].isna()
        | (juice_df["band_min"] >= juice_df["band_max"])
        | (~juice_df["fav_ud"].isin(["favorite", "underdog"]))
        | (~juice_df["venue"].isin(["home", "away"]))
    ]

    if not invalid.empty:
        raise ValueError(
            f"{market_label} juice config contains invalid rows: "
            f"{len(invalid)}"
        )

    duplicate_mask = juice_df.duplicated(
        subset=[
            "band_min",
            "band_max",
            "fav_ud",
            "venue",
        ],
        keep=False,
    )
    if duplicate_mask.any():
        raise ValueError(
            f"{market_label} juice config contains duplicate bands: "
            f"{int(duplicate_mask.sum())}"
        )

    required_combos = {
        (fav_ud, venue)
        for fav_ud in ["favorite", "underdog"]
        for venue in ["home", "away"]
    }
    present_combos = set(
        zip(
            juice_df["fav_ud"],
            juice_df["venue"],
        )
    )
    missing_combos = sorted(
        required_combos - present_combos
    )
    if missing_combos:
        raise ValueError(
            f"{market_label} juice config missing "
            f"fav_ud/venue combinations: {missing_combos}"
        )

    overlap_count = 0
    for _, group in juice_df.groupby(
        ["fav_ud", "venue"]
    ):
        ordered = group.sort_values(
            ["band_min", "band_max"]
        )
        previous_max = None

        for _, row in ordered.iterrows():
            band_min = float(
                row["band_min"]
            )
            band_max = float(
                row["band_max"]
            )

            if (
                previous_max is not None
                and band_min < previous_max
            ):
                overlap_count += 1

            previous_max = (
                band_max
                if previous_max is None
                else max(
                    previous_max,
                    band_max,
                )
            )

    if overlap_count:
        raise ValueError(
            f"{market_label} juice config contains "
            f"overlapping bands: {overlap_count}"
        )


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
