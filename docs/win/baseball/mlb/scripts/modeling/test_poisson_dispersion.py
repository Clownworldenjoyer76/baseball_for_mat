#!/usr/bin/env python3
"""Test MLB production run-model residuals for Poisson dispersion."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn

BASE_DIR = Path("docs/win/baseball/mlb")
DEFAULT_TRAINING_DATA = BASE_DIR / "modeling/data/mlb_run_training_set.csv"
DEFAULT_MODEL_DIR = BASE_DIR / "models/run_projection"
DEFAULT_REPORT_DIR = BASE_DIR / "modeling/reports"
EVALUATOR_PATH = BASE_DIR / "scripts/modeling/evaluate_run_model.py"

HOME_MODEL_NAME = "home_runs_model.joblib"
AWAY_MODEL_NAME = "away_runs_model.joblib"
HOME_METADATA_NAME = "home_runs_model_metadata.json"
AWAY_METADATA_NAME = "away_runs_model_metadata.json"

DEFAULT_SIMULATIONS = 50_000
DEFAULT_ALPHA = 0.05
DEFAULT_SEED = 20260913
SIMULATION_BATCH_SIZE = 2_000
EPSILON = 1e-12


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / "docs/win/baseball/mlb").is_dir():
                return candidate
    raise RuntimeError("Could not locate repository root")


ROOT = find_repo_root()
os.chdir(ROOT)


def load_module(name: str, path: Path):
    full_path = path if path.is_absolute() else ROOT / path
    if not full_path.exists():
        raise RuntimeError(f"Required module not found: {full_path}")
    spec = importlib.util.spec_from_file_location(name, full_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not import module: {full_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def poisson_unit_deviance(y: np.ndarray, mu: np.ndarray) -> np.ndarray:
    y = np.asarray(y, dtype=float)
    mu = np.maximum(np.asarray(mu, dtype=float), EPSILON)
    with np.errstate(divide="ignore", invalid="ignore"):
        log_term = np.where(y > 0.0, y * np.log(y / mu), 0.0)
    return 2.0 * (log_term - (y - mu))


def observed_statistics(y: np.ndarray, mu: np.ndarray) -> dict[str, float]:
    y = np.asarray(y, dtype=float)
    mu = np.maximum(np.asarray(mu, dtype=float), EPSILON)
    residual = y - mu
    return {
        "pearson_dispersion": float(np.mean((residual**2) / mu)),
        "mean_poisson_deviance": float(np.mean(poisson_unit_deviance(y, mu))),
        "residual_variance_ratio": float(np.var(residual, ddof=1) / np.mean(mu)),
    }


def simulate_null_statistics(
    mu: np.ndarray,
    simulations: int,
    seed: int,
) -> dict[str, np.ndarray]:
    mu = np.maximum(np.asarray(mu, dtype=float), EPSILON)
    rng = np.random.default_rng(seed)

    pearson = np.empty(simulations, dtype=float)
    deviance = np.empty(simulations, dtype=float)
    residual_variance = np.empty(simulations, dtype=float)

    completed = 0
    mean_mu = float(np.mean(mu))

    while completed < simulations:
        batch = min(SIMULATION_BATCH_SIZE, simulations - completed)
        simulated = rng.poisson(lam=mu, size=(batch, len(mu))).astype(float)
        residual = simulated - mu[None, :]

        pearson_batch = np.mean((residual**2) / mu[None, :], axis=1)

        with np.errstate(divide="ignore", invalid="ignore"):
            log_term = np.where(
                simulated > 0.0,
                simulated * np.log(simulated / mu[None, :]),
                0.0,
            )

        deviance_batch = np.mean(
            2.0 * (log_term - (simulated - mu[None, :])),
            axis=1,
        )

        residual_variance_batch = (
            np.var(residual, axis=1, ddof=1) / mean_mu
        )

        end = completed + batch
        pearson[completed:end] = pearson_batch
        deviance[completed:end] = deviance_batch
        residual_variance[completed:end] = residual_variance_batch
        completed = end

    return {
        "pearson_dispersion": pearson,
        "mean_poisson_deviance": deviance,
        "residual_variance_ratio": residual_variance,
    }


def empirical_test(
    observed: float,
    simulated: np.ndarray,
    alpha: float,
) -> dict[str, Any]:
    simulated = np.asarray(simulated, dtype=float)
    n = len(simulated)

    p_over = float((1 + np.count_nonzero(simulated >= observed)) / (n + 1))
    p_under = float((1 + np.count_nonzero(simulated <= observed)) / (n + 1))
    p_two_sided = float(min(1.0, 2.0 * min(p_over, p_under)))

    lower, median, upper = np.quantile(
        simulated,
        [alpha / 2.0, 0.5, 1.0 - alpha / 2.0],
    )

    if p_two_sided < alpha:
        direction = "overdispersion" if observed > median else "underdispersion"
        reject = True
    else:
        direction = "consistent_with_poisson"
        reject = False

    return {
        "observed": float(observed),
        "null_median": float(median),
        "null_interval_lower": float(lower),
        "null_interval_upper": float(upper),
        "p_over": p_over,
        "p_under": p_under,
        "p_two_sided": p_two_sided,
        "alpha": float(alpha),
        "reject_poisson_dispersion": reject,
        "direction": direction,
    }


def side_diagnostic(
    side: str,
    scored: pd.DataFrame,
    simulations: int,
    alpha: float,
    seed: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    target_col = f"target_{side}_runs"
    prediction_col = f"model_{side}_runs"

    y = pd.to_numeric(scored[target_col], errors="coerce").to_numpy(dtype=float)
    mu = pd.to_numeric(scored[prediction_col], errors="coerce").to_numpy(dtype=float)

    if np.any(~np.isfinite(y)):
        raise RuntimeError(f"{side}: observed runs contain non-finite values")
    if np.any(~np.isfinite(mu)) or np.any(mu <= 0.0):
        raise RuntimeError(f"{side}: predicted means must be finite and positive")

    observed = observed_statistics(y, mu)
    null = simulate_null_statistics(mu, simulations, seed)
    tests = {
        name: empirical_test(observed[name], null[name], alpha)
        for name in observed
    }

    primary = tests["pearson_dispersion"]
    residual = y - mu
    pearson_residual = residual / np.sqrt(mu)

    result = {
        "side": side,
        "rows": int(len(y)),
        "observed_mean_runs": float(np.mean(y)),
        "predicted_mean_runs": float(np.mean(mu)),
        "mean_residual": float(np.mean(residual)),
        "observed_run_variance": float(np.var(y, ddof=1)),
        "mean_predicted_poisson_variance": float(np.mean(mu)),
        "primary_test": "pearson_dispersion",
        "poisson_variance_assumption": (
            "rejected" if primary["reject_poisson_dispersion"] else "not_rejected"
        ),
        "dispersion_direction": primary["direction"],
        "tests": tests,
    }

    residual_frame = pd.DataFrame(
        {
            "game_id": scored["game_id"].astype("string"),
            "game_date": scored["game_date"].astype("string"),
            "side": side,
            "observed_runs": y,
            "predicted_mean_runs": mu,
            "raw_residual": residual,
            "pearson_residual": pearson_residual,
            "squared_pearson_residual": pearson_residual**2,
        }
    )

    return result, residual_frame


def model_class_name(path: Path) -> str:
    model = joblib.load(path)
    return model.__class__.__name__


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--training-data", type=Path, default=DEFAULT_TRAINING_DATA)
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help="Production model directory to diagnose.",
    )
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--simulations", type=int, default=DEFAULT_SIMULATIONS)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.simulations < 1_000:
        raise RuntimeError("--simulations must be at least 1000")
    if not (0.0 < args.alpha < 1.0):
        raise RuntimeError("--alpha must be between 0 and 1")

    evaluator = load_module(
        "mlb_poisson_dispersion_evaluator",
        EVALUATOR_PATH,
    )

    home_metadata = evaluator.load_json(
        args.model_dir / HOME_METADATA_NAME,
        "home production metadata",
    )
    away_metadata = evaluator.load_json(
        args.model_dir / AWAY_METADATA_NAME,
        "away production metadata",
    )

    (
        home_features,
        away_features,
        all_features,
        test_start,
        test_end,
        expected_test_rows,
    ) = evaluator.validate_metadata(home_metadata, away_metadata)

    test = evaluator.load_test_period(
        args.training_data,
        all_features,
        test_start,
        test_end,
        expected_test_rows,
    )

    scored = evaluator.score_models(
        test,
        home_features,
        away_features,
        args.model_dir,
    )

    home_result, home_residuals = side_diagnostic(
        "home",
        scored,
        args.simulations,
        args.alpha,
        args.seed,
    )
    away_result, away_residuals = side_diagnostic(
        "away",
        scored,
        args.simulations,
        args.alpha,
        args.seed + 1,
    )

    payload = {
        "created_at": now_iso(),
        "method": {
            "description": "Fixed-mean out-of-sample parametric Poisson dispersion test",
            "primary_statistic": "pearson_dispersion",
            "simulations": int(args.simulations),
            "alpha": float(args.alpha),
            "seed_home": int(args.seed),
            "seed_away": int(args.seed + 1),
            "decision_rule": (
                "Reject Poisson variance when the two-sided empirical "
                "Pearson-dispersion p-value is below alpha."
            ),
        },
        "runtime": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "scikit_learn": sklearn.__version__,
            "joblib": joblib.__version__,
        },
        "model_dir": str(args.model_dir),
        "model_classes": {
            "home": model_class_name(args.model_dir / HOME_MODEL_NAME),
            "away": model_class_name(args.model_dir / AWAY_MODEL_NAME),
        },
        "test_start_date": test_start,
        "test_end_date": test_end,
        "test_rows": int(len(scored)),
        "home": home_result,
        "away": away_result,
    }

    summary_rows: list[dict[str, Any]] = []
    for side_result in [home_result, away_result]:
        for statistic_name, test_result in side_result["tests"].items():
            summary_rows.append(
                {
                    "side": side_result["side"],
                    "statistic": statistic_name,
                    "observed": test_result["observed"],
                    "null_median": test_result["null_median"],
                    "null_interval_lower": test_result["null_interval_lower"],
                    "null_interval_upper": test_result["null_interval_upper"],
                    "p_under": test_result["p_under"],
                    "p_over": test_result["p_over"],
                    "p_two_sided": test_result["p_two_sided"],
                    "reject_poisson_dispersion": test_result[
                        "reject_poisson_dispersion"
                    ],
                    "direction": test_result["direction"],
                }
            )

    summary = pd.DataFrame(summary_rows)
    residuals = pd.concat([home_residuals, away_residuals], ignore_index=True)

    args.report_dir.mkdir(parents=True, exist_ok=True)

    summary_path = args.report_dir / "poisson_dispersion_summary.csv"
    json_path = args.report_dir / "poisson_dispersion_test.json"
    residual_path = args.report_dir / "poisson_dispersion_residuals.csv"

    summary.to_csv(summary_path, index=False)
    residuals.to_csv(residual_path, index=False)
    json_path.write_text(
        json.dumps(payload, indent=2, default=str) + "\n",
        encoding="utf-8",
    )

    print("MLB Poisson dispersion diagnostic complete")
    print(
        f"Test period: {test_start} through {test_end} "
        f"({len(scored)} games)"
    )
    print(
        f"Home: {home_result['dispersion_direction']} | "
        f"Pearson={home_result['tests']['pearson_dispersion']['observed']:.6f} | "
        f"p={home_result['tests']['pearson_dispersion']['p_two_sided']:.6f}"
    )
    print(
        f"Away: {away_result['dispersion_direction']} | "
        f"Pearson={away_result['tests']['pearson_dispersion']['observed']:.6f} | "
        f"p={away_result['tests']['pearson_dispersion']['p_two_sided']:.6f}"
    )
    print(f"Summary: {summary_path}")
    print(f"JSON: {json_path}")
    print(f"Residuals: {residual_path}")


if __name__ == "__main__":
    main()
