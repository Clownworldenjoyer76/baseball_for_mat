#!/usr/bin/env python3
"""
Fit market-specific probability calibrators from leakage-safe OOS Poisson predictions.

Input is the game_probabilities.csv produced by:
    docs/win/baseball/mlb/scripts/modeling/backtest_count_distributions.py

The run model is NOT refit here. This script uses only the chronological OOS
Poisson/Skellam predictions already produced by item #6.

Three calibrators are fit independently:
- moneyline: home win probability, ties excluded
- run line: home cover probability, pooling home -1.5 and home +1.5
- totals: over probability, pooling the established common half-run total lines

Calibration model:
    monotone beta-logistic calibration
    q = sigmoid(c + a*log(p) - b*log(1-p)), with a>0 and b>0

Evaluation:
- chronological calibration cross-fit on CV folds 2-4:
  each fold's calibrator is fit only on earlier OOS folds
- final_test is reported as a reference evaluation using a calibrator fit on all
  CV OOS predictions; it is NOT used to fit the production calibrator

Production artifact:
    docs/win/baseball/mlb/models/probability_calibration/market_calibrators.json
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

EPS = 1e-12
COMMON_TOTAL_LINES = (6.5, 7.5, 8.5, 9.5, 10.5, 11.5)
PROBABILITY_BINS = np.linspace(0.0, 1.0, 11)

BASE_DIR = Path("docs/win/baseball/mlb")
DEFAULT_INPUT = (
    BASE_DIR
    / "modeling/count_distribution_backtest/game_probabilities.csv"
)
DEFAULT_OUTPUT_DIR = (
    BASE_DIR
    / "modeling/probability_calibration"
)
DEFAULT_ARTIFACT = (
    BASE_DIR
    / "models/probability_calibration/market_calibrators.json"
)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / BASE_DIR).is_dir():
                return candidate
    raise RuntimeError("Could not locate repository root")


def binary_log_loss(y: np.ndarray, p: np.ndarray) -> float:
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    valid = np.isfinite(y) & np.isfinite(p)
    if not np.any(valid):
        return float("nan")
    y = y[valid]
    p = p[valid]
    return float(np.mean(-(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))))


def ece(y: np.ndarray, p: np.ndarray) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    y = y[valid]
    p = np.clip(p[valid], 0.0, 1.0)
    if len(y) == 0:
        return float("nan")

    bins = np.digitize(
        p,
        PROBABILITY_BINS[1:-1],
        right=False,
    )
    total = len(y)
    value = 0.0
    for idx in range(10):
        mask = bins == idx
        if not np.any(mask):
            continue
        value += (
            float(np.sum(mask))
            / float(total)
            * abs(float(np.mean(p[mask])) - float(np.mean(y[mask])))
        )
    return float(value)


def _sigmoid_scalar(z: float) -> float:
    if z >= 0.0:
        return 1.0 / (1.0 + math.exp(-z))
    ez = math.exp(z)
    return ez / (1.0 + ez)


def beta_predict(
    p: np.ndarray,
    log_a: float,
    log_b: float,
    intercept: float,
) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    a = math.exp(float(log_a))
    b = math.exp(float(log_b))
    z = (
        float(intercept)
        + a * np.log(p)
        - b * np.log1p(-p)
    )

    out = np.empty_like(z, dtype=float)
    positive = z >= 0.0
    out[positive] = 1.0 / (1.0 + np.exp(-z[positive]))
    ez = np.exp(z[~positive])
    out[~positive] = ez / (1.0 + ez)
    return np.clip(out, EPS, 1.0 - EPS)


def fit_beta_calibrator(y: np.ndarray, p: np.ndarray) -> dict:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    y = y[valid]
    p = np.clip(p[valid], EPS, 1.0 - EPS)

    if len(y) < 50:
        raise RuntimeError(
            f"Insufficient calibration observations: {len(y)}"
        )
    if len(np.unique(y)) < 2:
        raise RuntimeError(
            "Calibration outcomes contain only one class"
        )

    def objective(theta: np.ndarray) -> float:
        q = beta_predict(
            p,
            log_a=float(theta[0]),
            log_b=float(theta[1]),
            intercept=float(theta[2]),
        )
        return float(
            np.sum(
                -(y * np.log(q) + (1.0 - y) * np.log(1.0 - q))
            )
        )

    # Identity calibration is a=b=1, intercept=0.
    result = minimize(
        objective,
        x0=np.array([0.0, 0.0, 0.0], dtype=float),
        method="L-BFGS-B",
        bounds=[
            (-4.0, 4.0),  # log(a)
            (-4.0, 4.0),  # log(b)
            (-10.0, 10.0),
        ],
        options={
            "maxiter": 5000,
            "ftol": 1e-12,
            "gtol": 1e-8,
        },
    )

    if not result.success:
        raise RuntimeError(
            f"Calibration optimization failed: {result.message}"
        )

    log_a = float(result.x[0])
    log_b = float(result.x[1])
    intercept = float(result.x[2])

    return {
        "method": "beta_logistic",
        "log_a": log_a,
        "log_b": log_b,
        "intercept": intercept,
        "a": float(math.exp(log_a)),
        "b": float(math.exp(log_b)),
        "observations": int(len(y)),
        "positive_rate": float(np.mean(y)),
        "raw_log_loss": binary_log_loss(y, p),
        "raw_ece": ece(y, p),
        "fitted_log_loss": binary_log_loss(
            y,
            beta_predict(p, log_a, log_b, intercept),
        ),
        "fitted_ece": ece(
            y,
            beta_predict(p, log_a, log_b, intercept),
        ),
    }


def apply_calibrator(frame: pd.DataFrame, calibrator: dict) -> np.ndarray:
    return beta_predict(
        frame["p"].to_numpy(dtype=float),
        log_a=float(calibrator["log_a"]),
        log_b=float(calibrator["log_b"]),
        intercept=float(calibrator["intercept"]),
    )


def make_market_frame(
    poisson_games: pd.DataFrame,
    market: str,
) -> pd.DataFrame:
    base_cols = ["period", "game_date", "game_id"]

    if market == "moneyline":
        frame = poisson_games[
            base_cols
            + [
                "moneyline_home_prob",
                "moneyline_home_win",
            ]
        ].copy()
        frame = frame.rename(
            columns={
                "moneyline_home_prob": "p",
                "moneyline_home_win": "y",
            }
        )
        frame["contract"] = "home_moneyline"
        return frame.dropna(subset=["p", "y"]).reset_index(drop=True)

    if market == "run_line":
        minus = poisson_games[
            base_cols
            + [
                "runline_home_minus_1_5_prob",
                "runline_home_minus_1_5_win",
            ]
        ].copy()
        minus = minus.rename(
            columns={
                "runline_home_minus_1_5_prob": "p",
                "runline_home_minus_1_5_win": "y",
            }
        )
        minus["contract"] = "home_-1.5"

        plus = poisson_games[
            base_cols
            + [
                "runline_home_plus_1_5_prob",
                "runline_home_plus_1_5_win",
            ]
        ].copy()
        plus = plus.rename(
            columns={
                "runline_home_plus_1_5_prob": "p",
                "runline_home_plus_1_5_win": "y",
            }
        )
        plus["contract"] = "home_+1.5"

        return pd.concat(
            [minus, plus],
            ignore_index=True,
        ).dropna(subset=["p", "y"]).reset_index(drop=True)

    if market == "total":
        pieces = []
        for line in COMMON_TOTAL_LINES:
            key = f"{line:.1f}".replace(".", "_")
            piece = poisson_games[
                base_cols
                + [
                    f"over_{key}_prob",
                    f"over_{key}_win",
                ]
            ].copy()
            piece = piece.rename(
                columns={
                    f"over_{key}_prob": "p",
                    f"over_{key}_win": "y",
                }
            )
            piece["contract"] = f"over_{line:.1f}"
            pieces.append(piece)

        return pd.concat(
            pieces,
            ignore_index=True,
        ).dropna(subset=["p", "y"]).reset_index(drop=True)

    raise ValueError(f"Unknown market: {market}")


def fold_number(period: str) -> int:
    prefix = "cv_fold_"
    if not period.startswith(prefix):
        raise ValueError(period)
    return int(period[len(prefix):])


def metric_row(
    market: str,
    period: str,
    y: np.ndarray,
    raw_p: np.ndarray,
    calibrated_p: np.ndarray,
    train_observations: int,
) -> dict:
    raw_ll = binary_log_loss(y, raw_p)
    cal_ll = binary_log_loss(y, calibrated_p)
    raw_ece = ece(y, raw_p)
    cal_ece = ece(y, calibrated_p)
    return {
        "market": market,
        "period": period,
        "observations": int(len(y)),
        "calibrator_train_observations": int(train_observations),
        "raw_log_loss": raw_ll,
        "calibrated_log_loss": cal_ll,
        "log_loss_delta_cal_minus_raw": cal_ll - raw_ll,
        "raw_ece": raw_ece,
        "calibrated_ece": cal_ece,
        "ece_delta_cal_minus_raw": cal_ece - raw_ece,
    }


def evaluate_market(
    market_frame: pd.DataFrame,
    market: str,
) -> tuple[dict, list[dict], pd.DataFrame]:
    cv_periods = sorted(
        [
            p
            for p in market_frame["period"].dropna().unique().tolist()
            if str(p).startswith("cv_fold_")
        ],
        key=fold_number,
    )
    if len(cv_periods) < 2:
        raise RuntimeError(
            f"{market}: need at least two CV folds; found {cv_periods}"
        )

    metric_rows: list[dict] = []
    prediction_rows: list[pd.DataFrame] = []

    # Chronological calibration cross-fit: each evaluated fold is calibrated
    # only using prior OOS folds.
    for idx in range(1, len(cv_periods)):
        train_periods = cv_periods[:idx]
        eval_period = cv_periods[idx]

        train = market_frame[
            market_frame["period"].isin(train_periods)
        ].copy()
        test = market_frame[
            market_frame["period"] == eval_period
        ].copy()

        calibrator = fit_beta_calibrator(
            train["y"].to_numpy(dtype=float),
            train["p"].to_numpy(dtype=float),
        )
        calibrated = apply_calibrator(test, calibrator)

        metric_rows.append(
            metric_row(
                market,
                eval_period,
                test["y"].to_numpy(dtype=float),
                test["p"].to_numpy(dtype=float),
                calibrated,
                len(train),
            )
        )

        pred = test.copy()
        pred["market"] = market
        pred["raw_p"] = pred["p"]
        pred["calibrated_p"] = calibrated
        pred["calibrator_train_periods"] = ",".join(train_periods)
        prediction_rows.append(pred)

    crossfit = pd.concat(
        prediction_rows,
        ignore_index=True,
    )
    metric_rows.append(
        metric_row(
            market,
            "crossfit_combined",
            crossfit["y"].to_numpy(dtype=float),
            crossfit["raw_p"].to_numpy(dtype=float),
            crossfit["calibrated_p"].to_numpy(dtype=float),
            int(crossfit["calibrator_train_periods"].nunique()),
        )
    )

    all_cv = market_frame[
        market_frame["period"].isin(cv_periods)
    ].copy()
    production_calibrator = fit_beta_calibrator(
        all_cv["y"].to_numpy(dtype=float),
        all_cv["p"].to_numpy(dtype=float),
    )

    final_test = market_frame[
        market_frame["period"] == "final_test"
    ].copy()
    if not final_test.empty:
        final_calibrated = apply_calibrator(
            final_test,
            production_calibrator,
        )
        metric_rows.append(
            metric_row(
                market,
                "final_test_reference",
                final_test["y"].to_numpy(dtype=float),
                final_test["p"].to_numpy(dtype=float),
                final_calibrated,
                len(all_cv),
            )
        )
        pred = final_test.copy()
        pred["market"] = market
        pred["raw_p"] = pred["p"]
        pred["calibrated_p"] = final_calibrated
        pred["calibrator_train_periods"] = ",".join(cv_periods)
        prediction_rows.append(pred)

    production_calibrator["fit_periods"] = cv_periods
    production_calibrator["fit_observations"] = int(len(all_cv))

    crossfit_row = next(
        row
        for row in metric_rows
        if row["period"] == "crossfit_combined"
    )
    enabled = (
        crossfit_row["calibrated_log_loss"]
        <= crossfit_row["raw_log_loss"]
        and crossfit_row["calibrated_ece"]
        <= crossfit_row["raw_ece"]
    )
    production_calibrator["enabled"] = bool(enabled)
    production_calibrator["promotion_rule"] = (
        "enable only when chronological cross-fit calibration "
        "does not worsen either log loss or ECE"
    )
    production_calibrator["crossfit_raw_log_loss"] = float(
        crossfit_row["raw_log_loss"]
    )
    production_calibrator["crossfit_calibrated_log_loss"] = float(
        crossfit_row["calibrated_log_loss"]
    )
    production_calibrator["crossfit_raw_ece"] = float(
        crossfit_row["raw_ece"]
    )
    production_calibrator["crossfit_calibrated_ece"] = float(
        crossfit_row["calibrated_ece"]
    )

    predictions = pd.concat(
        prediction_rows,
        ignore_index=True,
    )
    return production_calibrator, metric_rows, predictions


def write_summary(
    path: Path,
    metrics: pd.DataFrame,
    artifact: dict,
) -> None:
    lines = [
        "# Probability Calibration",
        "",
        f"Generated: `{artifact['generated_at_utc']}`",
        "",
        "Calibration source: chronological OOS Poisson/Skellam predictions from item #6.",
        "",
        "Production calibrators are fit on all four OOS CV folds. The `final_test_reference`",
        "row is evaluation only and is not used to fit the production calibrators.",
        "",
        "## Metrics",
        "",
        metrics.to_markdown(index=False),
        "",
        "## Production calibrators",
        "",
    ]
    for market, cal in artifact["markets"].items():
        lines.extend(
            [
                f"### {market}",
                "",
                f"- method: `{cal['method']}`",
                f"- enabled: `{cal['enabled']}`",
                f"- fit observations: `{cal['fit_observations']}`",
                f"- fit periods: `{', '.join(cal['fit_periods'])}`",
                f"- a: `{cal['a']:.12f}`",
                f"- b: `{cal['b']:.12f}`",
                f"- intercept: `{cal['intercept']:.12f}`",
                "",
            ]
        )

    path.write_text(
        "\n".join(lines).rstrip() + "\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fit market-specific OOS probability calibrators"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    parser.add_argument(
        "--artifact",
        type=Path,
        default=DEFAULT_ARTIFACT,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = find_repo_root()

    input_path = (
        args.input
        if args.input.is_absolute()
        else root / args.input
    )
    output_dir = (
        args.output_dir
        if args.output_dir.is_absolute()
        else root / args.output_dir
    )
    artifact_path = (
        args.artifact
        if args.artifact.is_absolute()
        else root / args.artifact
    )

    if not input_path.exists():
        raise FileNotFoundError(
            f"Missing item #6 probability output: {input_path}"
        )

    games = pd.read_csv(input_path)
    required = {
        "period",
        "distribution",
        "game_date",
        "game_id",
        "moneyline_home_prob",
        "moneyline_home_win",
        "runline_home_minus_1_5_prob",
        "runline_home_minus_1_5_win",
        "runline_home_plus_1_5_prob",
        "runline_home_plus_1_5_win",
    }
    for line in COMMON_TOTAL_LINES:
        key = f"{line:.1f}".replace(".", "_")
        required.add(f"over_{key}_prob")
        required.add(f"over_{key}_win")

    missing = sorted(required - set(games.columns))
    if missing:
        raise RuntimeError(
            f"game_probabilities.csv missing required columns: {missing}"
        )

    poisson_games = games[
        games["distribution"] == "poisson_skellam"
    ].copy()
    if poisson_games.empty:
        raise RuntimeError(
            "No poisson_skellam rows found in game_probabilities.csv"
        )

    artifact = {
        "schema_version": 1,
        "generated_at_utc": now_iso(),
        "method": "beta_logistic",
        "source_file": str(input_path),
        "source_distribution": "poisson_skellam",
        "markets": {},
    }
    all_metric_rows: list[dict] = []
    all_predictions: list[pd.DataFrame] = []

    for market in ("moneyline", "run_line", "total"):
        frame = make_market_frame(
            poisson_games,
            market,
        )
        calibrator, metric_rows, predictions = evaluate_market(
            frame,
            market,
        )
        artifact["markets"][market] = calibrator
        all_metric_rows.extend(metric_rows)
        all_predictions.append(predictions)

    metrics = pd.DataFrame(all_metric_rows)
    predictions = pd.concat(
        all_predictions,
        ignore_index=True,
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )
    artifact_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    metrics_path = output_dir / "calibration_metrics.csv"
    predictions_path = output_dir / "calibration_predictions.csv"
    summary_path = output_dir / "calibration_summary.md"

    metrics.to_csv(
        metrics_path,
        index=False,
        encoding="utf-8",
    )
    predictions.to_csv(
        predictions_path,
        index=False,
        encoding="utf-8",
    )
    artifact_path.write_text(
        json.dumps(
            artifact,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    write_summary(
        summary_path,
        metrics,
        artifact,
    )

    print("Probability calibration fit complete.")
    print(f"Artifact: {artifact_path}")
    print(f"Metrics: {metrics_path}")
    print()
    display_cols = [
        "market",
        "period",
        "observations",
        "raw_log_loss",
        "calibrated_log_loss",
        "raw_ece",
        "calibrated_ece",
    ]
    print(
        metrics[
            metrics["period"].isin(
                ["crossfit_combined", "final_test_reference"]
            )
        ][display_cols].to_string(index=False)
    )
    print()
    print("Production calibration status:")
    for market in ("moneyline", "run_line", "total"):
        cal = artifact["markets"][market]
        print(
            f"  {market}: "
            f"{'ENABLED' if cal['enabled'] else 'DISABLED (identity/raw probability)'}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
