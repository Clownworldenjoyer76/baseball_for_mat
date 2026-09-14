#!/usr/bin/env python3
"""
Item #10: validate MLB total probabilities separately for integer and half-run lines.

This is reporting-only. It does not modify production probabilities or models.

Default evaluation period:
    2026-08-14 through 2026-09-10

Inputs:
- docs/win/baseball/mlb/02_juice/*_mlb_total.csv
- docs/win/baseball/mlb/modeling/data/mlb_run_training_set.csv

Outputs:
- docs/win/baseball/mlb/modeling/total_line_validation/
    total_line_type_metrics.csv
    total_line_metrics.csv
    total_line_validation_rows.csv
    total_line_validation_summary.md
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path("docs/win/baseball/mlb")
DEFAULT_JUICE_DIR = BASE_DIR / "02_juice"
DEFAULT_TRAINING = BASE_DIR / "modeling/data/mlb_run_training_set.csv"
DEFAULT_OUTPUT_DIR = BASE_DIR / "modeling/total_line_validation"

DEFAULT_START_DATE = "2026-08-14"
DEFAULT_END_DATE = "2026-09-10"

EPS = 1e-12
PROB_TOLERANCE = 1e-6
BINS = np.linspace(0.0, 1.0, 11)


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / BASE_DIR).is_dir():
                return candidate
    raise RuntimeError("Could not locate repository root")


def normalize_game_id(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    out = out.mask(out.eq(""))
    return out


def binary_log_loss(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    y = y[valid]
    p = np.clip(p[valid], EPS, 1.0 - EPS)
    if len(y) == 0:
        return float("nan")
    return float(
        np.mean(
            -(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
        )
    )


def ece(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    y = y[valid]
    p = np.clip(p[valid], 0.0, 1.0)
    if len(y) == 0:
        return float("nan")

    bin_id = np.digitize(
        p,
        BINS[1:-1],
        right=False,
    )
    value = 0.0
    n = len(y)
    for idx in range(10):
        mask = bin_id == idx
        if not np.any(mask):
            continue
        value += (
            np.sum(mask)
            / n
            * abs(float(np.mean(p[mask])) - float(np.mean(y[mask])))
        )
    return float(value)


def brier(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    if not np.any(valid):
        return float("nan")
    return float(np.mean((p[valid] - y[valid]) ** 2))


def line_type(value: float) -> str:
    frac = abs(float(value) - round(float(value)))
    if frac < 1e-9:
        return "integer"
    if abs(frac - 0.5) < 1e-9:
        return "half_run"
    return "unsupported"


def load_total_outputs(juice_dir: Path) -> pd.DataFrame:
    paths = sorted(juice_dir.glob("*_mlb_total.csv"))
    if not paths:
        raise FileNotFoundError(
            f"No total juice files found in {juice_dir}"
        )

    required = {
        "game_id",
        "game_date",
        "total",
        "over_model_prob_total_win",
        "over_model_prob_total_loss",
        "under_model_prob_total_win",
        "under_model_prob_total_loss",
        "total_model_prob_push",
    }

    frames = []
    for path in paths:
        frame = pd.read_csv(path)
        missing = sorted(required - set(frame.columns))
        if missing:
            raise RuntimeError(
                f"{path} missing required columns: {missing}"
            )

        match = re.fullmatch(
            r"(\d{4})_(\d{2})_(\d{2})_mlb_total\.csv",
            path.name,
        )
        if match is None:
            raise RuntimeError(
                f"Could not parse slate date from total filename: {path.name}"
            )

        slate_date = pd.Timestamp(
            year=int(match.group(1)),
            month=int(match.group(2)),
            day=int(match.group(3)),
        )

        selected = frame[list(required)].copy()
        selected["game_date_raw"] = selected["game_date"]
        selected["game_date"] = slate_date
        frames.append(selected)

    out = pd.concat(frames, ignore_index=True)
    out["game_id"] = normalize_game_id(out["game_id"])

    if out["game_id"].isna().any():
        raise RuntimeError("Total outputs contain blank game_id")

    duplicates = out["game_id"].duplicated(keep=False)
    if duplicates.any():
        sample = out.loc[
            duplicates,
            ["game_id", "game_date", "total"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            f"Duplicate game_id across total outputs; sample={sample}"
        )

    numeric = [
        "total",
        "over_model_prob_total_win",
        "over_model_prob_total_loss",
        "under_model_prob_total_win",
        "under_model_prob_total_loss",
        "total_model_prob_push",
    ]
    for col in numeric:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        if out[col].isna().any() or (~np.isfinite(out[col])).any():
            raise RuntimeError(
                f"Invalid numeric values in total output column {col}"
            )

    return out


def load_outcomes(training_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(training_path)
    required = {
        "game_id",
        "target_home_runs",
        "target_away_runs",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"{training_path} missing required columns: {missing}"
        )

    out = frame[
        ["game_id", "target_home_runs", "target_away_runs"]
    ].copy()
    out["game_id"] = normalize_game_id(out["game_id"])

    for col in ["target_home_runs", "target_away_runs"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(
        subset=[
            "game_id",
            "target_home_runs",
            "target_away_runs",
        ]
    ).copy()

    duplicates = out["game_id"].duplicated(keep=False)
    if duplicates.any():
        sample = out.loc[
            duplicates,
            ["game_id"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            f"Training outcomes contain duplicate game_id; sample={sample}"
        )

    return out


def validate_probability_contract(frame: pd.DataFrame) -> None:
    cols = [
        "over_model_prob_total_win",
        "over_model_prob_total_loss",
        "under_model_prob_total_win",
        "under_model_prob_total_loss",
        "total_model_prob_push",
    ]
    for col in cols:
        values = frame[col].to_numpy(dtype=float)
        if (
            np.any(~np.isfinite(values))
            or np.any(values < -PROB_TOLERANCE)
            or np.any(values > 1.0 + PROB_TOLERANCE)
        ):
            raise RuntimeError(
                f"Invalid total probability values in {col}"
            )

    over_sum = (
        frame["over_model_prob_total_win"]
        + frame["over_model_prob_total_loss"]
        + frame["total_model_prob_push"]
    )
    under_sum = (
        frame["under_model_prob_total_win"]
        + frame["under_model_prob_total_loss"]
        + frame["total_model_prob_push"]
    )
    complement_error_1 = (
        frame["under_model_prob_total_win"]
        - frame["over_model_prob_total_loss"]
    ).abs()
    complement_error_2 = (
        frame["under_model_prob_total_loss"]
        - frame["over_model_prob_total_win"]
    ).abs()

    max_error = max(
        float((over_sum - 1.0).abs().max()),
        float((under_sum - 1.0).abs().max()),
        float(complement_error_1.max()),
        float(complement_error_2.max()),
    )
    if max_error > PROB_TOLERANCE:
        raise RuntimeError(
            "Total probability contract failed; "
            f"max_error={max_error}"
        )


def prepare_validation_rows(
    totals: pd.DataFrame,
    outcomes: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    frame = totals[
        (totals["game_date"] >= start)
        & (totals["game_date"] <= end)
    ].copy()
    if frame.empty:
        raise RuntimeError(
            f"No total outputs in {start_date} through {end_date}"
        )

    frame = frame.merge(
        outcomes,
        on="game_id",
        how="left",
        validate="one_to_one",
        indicator=True,
    )
    missing = frame["_merge"] != "both"
    if missing.any():
        sample = frame.loc[
            missing,
            ["game_id", "game_date", "total"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            "Missing completed-game outcomes for validation rows; "
            f"sample={sample}"
        )
    frame = frame.drop(columns=["_merge"])

    frame["actual_total"] = (
        frame["target_home_runs"]
        + frame["target_away_runs"]
    )
    frame["line_type"] = frame["total"].map(line_type)

    unsupported = frame["line_type"].eq("unsupported")
    if unsupported.any():
        sample = frame.loc[
            unsupported,
            ["game_id", "total"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            f"Unsupported total lines found; sample={sample}"
        )

    frame["observed_push"] = np.isclose(
        frame["actual_total"],
        frame["total"],
        atol=PROB_TOLERANCE,
        rtol=0.0,
    ).astype(int)

    frame["observed_over_win"] = np.where(
        frame["observed_push"].eq(1),
        np.nan,
        (frame["actual_total"] > frame["total"]).astype(float),
    )
    frame["observed_under_win"] = np.where(
        frame["observed_push"].eq(1),
        np.nan,
        (frame["actual_total"] < frame["total"]).astype(float),
    )

    frame["resolved_model_mass"] = (
        frame["over_model_prob_total_win"]
        + frame["under_model_prob_total_win"]
    )

    bad_resolved = (
        ~np.isfinite(frame["resolved_model_mass"])
        | (frame["resolved_model_mass"] <= 0.0)
    )
    if bad_resolved.any():
        raise RuntimeError(
            "Invalid resolved probability mass in totals"
        )

    frame["over_conditional_prob_resolved"] = (
        frame["over_model_prob_total_win"]
        / frame["resolved_model_mass"]
    )
    frame["under_conditional_prob_resolved"] = (
        frame["under_model_prob_total_win"]
        / frame["resolved_model_mass"]
    )

    cond_sum_error = (
        frame["over_conditional_prob_resolved"]
        + frame["under_conditional_prob_resolved"]
        - 1.0
    ).abs()
    if float(cond_sum_error.max()) > PROB_TOLERANCE:
        raise RuntimeError(
            "Resolved conditional probabilities do not sum to 1"
        )

    # A half-run line cannot push under integer-valued baseball scoring.
    half = frame["line_type"].eq("half_run")
    if frame.loc[half, "observed_push"].sum() != 0:
        raise RuntimeError(
            "Observed push found on a half-run total line"
        )
    if (
        frame.loc[half, "total_model_prob_push"].abs().max()
        > PROB_TOLERANCE
    ):
        raise RuntimeError(
            "Model assigned nonzero push probability to a half-run total"
        )

    return frame


def summarize_group(
    group: pd.DataFrame,
    *,
    grouping: str,
    group_value: str,
) -> dict:
    resolved = group["observed_push"].eq(0)
    r = group.loc[resolved]

    observed_push_frequency = float(
        group["observed_push"].mean()
    )
    predicted_push_frequency = float(
        group["total_model_prob_push"].mean()
    )

    return {
        "grouping": grouping,
        "group": group_value,
        "rows": int(len(group)),
        "resolved_rows": int(resolved.sum()),
        "observed_pushes": int(group["observed_push"].sum()),
        "observed_push_frequency": observed_push_frequency,
        "mean_predicted_push_probability": predicted_push_frequency,
        "push_frequency_error_pred_minus_obs": (
            predicted_push_frequency - observed_push_frequency
        ),
        "push_brier": brier(
            group["observed_push"],
            group["total_model_prob_push"],
        ),
        "resolved_over_log_loss": binary_log_loss(
            r["observed_over_win"],
            r["over_conditional_prob_resolved"],
        ),
        "resolved_over_ece": ece(
            r["observed_over_win"],
            r["over_conditional_prob_resolved"],
        ),
        "mean_over_conditional_probability": (
            float(r["over_conditional_prob_resolved"].mean())
            if len(r)
            else float("nan")
        ),
        "observed_over_rate_resolved": (
            float(r["observed_over_win"].mean())
            if len(r)
            else float("nan")
        ),
        "max_probability_mass_error": float(
            (
                group["over_model_prob_total_win"]
                + group["over_model_prob_total_loss"]
                + group["total_model_prob_push"]
                - 1.0
            ).abs().max()
        ),
        "max_resolved_conditional_sum_error": float(
            (
                group["over_conditional_prob_resolved"]
                + group["under_conditional_prob_resolved"]
                - 1.0
            ).abs().max()
        ),
    }


def build_metrics(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    type_rows = []
    for kind in ("integer", "half_run"):
        group = frame[frame["line_type"] == kind]
        if group.empty:
            continue
        type_rows.append(
            summarize_group(
                group,
                grouping="line_type",
                group_value=kind,
            )
        )

    line_rows = []
    for value, group in frame.groupby("total", sort=True):
        line_rows.append(
            summarize_group(
                group,
                grouping="total_line",
                group_value=f"{float(value):.1f}",
            )
        )

    return (
        pd.DataFrame(type_rows),
        pd.DataFrame(line_rows),
    )


def write_summary(
    path: Path,
    start_date: str,
    end_date: str,
    type_metrics: pd.DataFrame,
    line_metrics: pd.DataFrame,
) -> None:
    lines = [
        "# Total Line Validation",
        "",
        f"Evaluation period: `{start_date}` through `{end_date}`.",
        "",
        "Integer and half-run totals are validated separately. Push frequency is",
        "scored directly. Over/under calibration and log loss use conditional",
        "probabilities on resolved bets, excluding observed pushes.",
        "",
        "## By line type",
        "",
        type_metrics.to_markdown(index=False),
        "",
        "## By total line",
        "",
        line_metrics.to_markdown(index=False),
        "",
    ]
    path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate integer and half-run MLB total probabilities separately"
        )
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
    )
    parser.add_argument(
        "--end-date",
        default=DEFAULT_END_DATE,
    )
    parser.add_argument(
        "--juice-dir",
        type=Path,
        default=DEFAULT_JUICE_DIR,
    )
    parser.add_argument(
        "--training",
        type=Path,
        default=DEFAULT_TRAINING,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = find_repo_root()

    juice_dir = (
        args.juice_dir
        if args.juice_dir.is_absolute()
        else root / args.juice_dir
    )
    training = (
        args.training
        if args.training.is_absolute()
        else root / args.training
    )
    output_dir = (
        args.output_dir
        if args.output_dir.is_absolute()
        else root / args.output_dir
    )

    totals = load_total_outputs(juice_dir)
    validate_probability_contract(totals)
    outcomes = load_outcomes(training)

    rows = prepare_validation_rows(
        totals,
        outcomes,
        args.start_date,
        args.end_date,
    )
    type_metrics, line_metrics = build_metrics(rows)

    output_dir.mkdir(parents=True, exist_ok=True)

    rows_path = output_dir / "total_line_validation_rows.csv"
    type_path = output_dir / "total_line_type_metrics.csv"
    line_path = output_dir / "total_line_metrics.csv"
    summary_path = output_dir / "total_line_validation_summary.md"

    rows.to_csv(rows_path, index=False)
    type_metrics.to_csv(type_path, index=False)
    line_metrics.to_csv(line_path, index=False)
    write_summary(
        summary_path,
        args.start_date,
        args.end_date,
        type_metrics,
        line_metrics,
    )

    print("Total-line validation complete.")
    print(
        f"Period: {args.start_date} through {args.end_date}"
    )
    print(f"Rows: {len(rows)}")
    print()
    print(type_metrics.to_string(index=False))
    print()
    print(f"Type metrics: {type_path}")
    print(f"Line metrics: {line_path}")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
