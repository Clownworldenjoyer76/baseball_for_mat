#!/usr/bin/env python3
"""Backtest alternative coupled MLB score distributions for checklist item #6.

This is a research script. It does not modify production models or production
pipeline outputs.

It keeps the selected home/away run-mean model pair fixed and compares:
1. independent Poisson / Skellam
2. independent negative binomial with separately fitted home/away dispersion
3. bivariate Poisson with a fitted shared-score component
4. empirical paired-residual simulation

Distribution parameters are fitted only from rows earlier than each evaluation
period. The final test uses distribution parameters estimated from development
OOF residuals and run-mean models refit on all development rows.

Outputs:
    docs/win/baseball/mlb/modeling/count_distribution_backtest/
        distribution_metrics.csv
        distribution_parameters.csv
        game_probabilities.csv
        distribution_backtest_summary.md
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from scipy.special import gammaln, logsumexp, xlogy
from scipy.stats import nbinom, poisson, skellam
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


BASE_DIR = Path("docs/win/baseball/mlb")
TRAINING_FILE = BASE_DIR / "modeling/data/mlb_run_training_set.csv"
TRAINER_FILE = BASE_DIR / "scripts/modeling/train_run_model.py"
CANDIDATE_DIR = BASE_DIR / "models/run_projection/candidates"
HOME_METADATA_FILE = CANDIDATE_DIR / "home_runs_model_metadata.json"
AWAY_METADATA_FILE = CANDIDATE_DIR / "away_runs_model_metadata.json"
DEFAULT_OUTPUT_DIR = BASE_DIR / "modeling/count_distribution_backtest"

EXPECTED_HOME_ID = "671c8f6c42c7bf84430c"
EXPECTED_AWAY_ID = "15fad4fc350fd375711d"

COMMON_TOTAL_LINES = np.array(
    [6.5, 7.5, 8.5, 9.5, 10.5, 11.5],
    dtype=float,
)
EPS = 1e-12
RANDOM_STATE = 42
PROBABILITY_BINS = np.linspace(0.0, 1.0, 11)
FINAL_REPRO_TOLERANCE = 1e-7


@dataclass
class DistributionFit:
    home_alpha: float | None = None
    away_alpha: float | None = None
    bivariate_theta: float | None = None
    residual_home: np.ndarray | None = None
    residual_away: np.ndarray | None = None


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
    full = ROOT / path
    spec = importlib.util.spec_from_file_location(name, full)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not import {full}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


TRAIN = load_module("count_distribution_train_run_model", TRAINER_FILE)


def load_json(path: Path) -> dict:
    full = ROOT / path
    if not full.exists():
        raise FileNotFoundError(full)
    payload = json.loads(full.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object: {full}")
    return payload


def validate_metadata(home: dict, away: dict) -> None:
    if home.get("candidate_id") != EXPECTED_HOME_ID:
        raise RuntimeError(
            "Unexpected home candidate_id: "
            f"{home.get('candidate_id')!r}; expected {EXPECTED_HOME_ID}"
        )
    if away.get("candidate_id") != EXPECTED_AWAY_ID:
        raise RuntimeError(
            "Unexpected away candidate_id: "
            f"{away.get('candidate_id')!r}; expected {EXPECTED_AWAY_ID}"
        )

    required = [
        "family",
        "params",
        "feature_columns",
        "cv_folds",
        "test_start_date",
        "test_end_date",
        "untouched_test_candidate_metrics",
    ]
    for label, metadata in (("home", home), ("away", away)):
        missing = [key for key in required if key not in metadata]
        if missing:
            raise RuntimeError(f"{label} metadata missing keys: {missing}")

    if home["cv_folds"] != away["cv_folds"]:
        raise RuntimeError("Home/away metadata CV folds differ")
    if home["test_start_date"] != away["test_start_date"]:
        raise RuntimeError("Home/away test_start_date differs")
    if home["test_end_date"] != away["test_end_date"]:
        raise RuntimeError("Home/away test_end_date differs")


def make_model(family: str, params: dict, workers: int):
    if family == "random_forest":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestRegressor(
                        criterion=params["criterion"],
                        n_estimators=params["n_estimators"],
                        max_depth=params["max_depth"],
                        min_samples_leaf=params["min_samples_leaf"],
                        max_features=params["max_features"],
                        max_samples=params.get("max_samples"),
                        bootstrap=True,
                        random_state=RANDOM_STATE,
                        n_jobs=workers,
                    ),
                ),
            ]
        )

    if family == "poisson_glm":
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                (
                    "model",
                    PoissonRegressor(
                        alpha=params["alpha"],
                        solver=params["solver"],
                        max_iter=3000,
                        tol=1e-8,
                    ),
                ),
            ]
        )

    raise RuntimeError(f"Unsupported selected model family: {family}")


def load_training(home_meta: dict, away_meta: dict) -> pd.DataFrame:
    df = TRAIN.load_training_set(ROOT / TRAINING_FILE)
    all_features = list(
        dict.fromkeys(
            list(home_meta["feature_columns"])
            + list(away_meta["feature_columns"])
        )
    )
    df = TRAIN.coerce_and_validate_training_data(df, all_features)
    df = df.sort_values(["_game_date_dt", "game_id"]).reset_index(drop=True)
    return df


def date_slice(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    out = df[
        (df["_game_date_dt"] >= start_ts)
        & (df["_game_date_dt"] <= end_ts)
    ].copy()
    if out.empty:
        raise RuntimeError(f"Empty date slice {start}..{end}")
    return out


def fit_mean_pair(
    train: pd.DataFrame,
    home_meta: dict,
    away_meta: dict,
    workers: int,
):
    hm = make_model(home_meta["family"], home_meta["params"], workers)
    am = make_model(away_meta["family"], away_meta["params"], workers)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        hm.fit(
            train[home_meta["feature_columns"]],
            train["target_home_runs"],
        )
        am.fit(
            train[away_meta["feature_columns"]],
            train["target_away_runs"],
        )
    return hm, am


def predict_pair(
    hm,
    am,
    frame: pd.DataFrame,
    home_meta: dict,
    away_meta: dict,
) -> tuple[np.ndarray, np.ndarray]:
    hp = np.asarray(
        hm.predict(frame[home_meta["feature_columns"]]),
        dtype=float,
    )
    ap = np.asarray(
        am.predict(frame[away_meta["feature_columns"]]),
        dtype=float,
    )
    if (
        np.any(~np.isfinite(hp))
        or np.any(~np.isfinite(ap))
        or np.any(hp <= 0)
        or np.any(ap <= 0)
    ):
        raise RuntimeError("Selected run models emitted invalid run means")
    return hp, ap


def binary_log_loss(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
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
            np.sum(mask)
            / total
            * abs(float(np.mean(p[mask])) - float(np.mean(y[mask])))
        )
    return float(value)


def fit_nb_alpha(y: np.ndarray, mu: np.ndarray) -> float:
    y = np.asarray(y, dtype=int)
    mu = np.maximum(np.asarray(mu, dtype=float), EPS)

    def objective(log_alpha: float) -> float:
        alpha = math.exp(log_alpha)
        size = 1.0 / alpha
        prob = size / (size + mu)
        ll = nbinom.logpmf(y, size, prob)
        if np.any(~np.isfinite(ll)):
            return float("inf")
        return float(-np.sum(ll))

    result = minimize_scalar(
        objective,
        bounds=(math.log(1e-8), math.log(20.0)),
        method="bounded",
        options={"xatol": 1e-10},
    )
    candidates = [
        (1e-8, objective(math.log(1e-8))),
        (20.0, objective(math.log(20.0))),
    ]
    if result.success and np.isfinite(result.fun):
        candidates.append((math.exp(float(result.x)), float(result.fun)))
    alpha, _ = min(candidates, key=lambda item: item[1])
    return float(alpha)


def bivariate_logpmf_one(
    home_runs: int,
    away_runs: int,
    mean_home: float,
    mean_away: float,
    theta: float,
) -> float:
    shared = theta * min(mean_home, mean_away)
    lam_home = max(mean_home - shared, 0.0)
    lam_away = max(mean_away - shared, 0.0)
    lam_shared = max(shared, 0.0)

    k = np.arange(min(home_runs, away_runs) + 1, dtype=int)
    log_terms = (
        xlogy(home_runs - k, lam_home)
        - gammaln(home_runs - k + 1)
        + xlogy(away_runs - k, lam_away)
        - gammaln(away_runs - k + 1)
        + xlogy(k, lam_shared)
        - gammaln(k + 1)
    )
    return float(
        -(lam_home + lam_away + lam_shared)
        + logsumexp(log_terms)
    )


def fit_bivariate_theta(
    home_y: np.ndarray,
    away_y: np.ndarray,
    home_mu: np.ndarray,
    away_mu: np.ndarray,
) -> float:
    home_y = np.asarray(home_y, dtype=int)
    away_y = np.asarray(away_y, dtype=int)
    home_mu = np.asarray(home_mu, dtype=float)
    away_mu = np.asarray(away_mu, dtype=float)

    def objective(theta: float) -> float:
        vals = [
            bivariate_logpmf_one(
                int(h),
                int(a),
                float(mh),
                float(ma),
                float(theta),
            )
            for h, a, mh, ma in zip(
                home_y,
                away_y,
                home_mu,
                away_mu,
                strict=True,
            )
        ]
        vals = np.asarray(vals, dtype=float)
        if np.any(~np.isfinite(vals)):
            return float("inf")
        return float(-np.sum(vals))

    result = minimize_scalar(
        objective,
        bounds=(0.0, 0.95),
        method="bounded",
        options={"xatol": 1e-8},
    )
    candidates = [(0.0, objective(0.0)), (0.95, objective(0.95))]
    if result.success and np.isfinite(result.fun):
        candidates.append((float(result.x), float(result.fun)))
    theta, _ = min(candidates, key=lambda item: item[1])
    return float(theta)


def fit_distribution_parameters(
    home_y: np.ndarray,
    away_y: np.ndarray,
    home_mu: np.ndarray,
    away_mu: np.ndarray,
) -> DistributionFit:
    home_alpha = fit_nb_alpha(home_y, home_mu)
    away_alpha = fit_nb_alpha(away_y, away_mu)
    theta = fit_bivariate_theta(home_y, away_y, home_mu, away_mu)

    home_residual = (
        np.asarray(home_y, dtype=float) - home_mu
    ) / np.sqrt(np.maximum(home_mu, EPS))
    away_residual = (
        np.asarray(away_y, dtype=float) - away_mu
    ) / np.sqrt(np.maximum(away_mu, EPS))

    # Center before resampling so the empirical simulation preserves the target
    # means as closely as integer rounding/truncation allow.
    home_residual = home_residual - float(np.mean(home_residual))
    away_residual = away_residual - float(np.mean(away_residual))

    return DistributionFit(
        home_alpha=home_alpha,
        away_alpha=away_alpha,
        bivariate_theta=theta,
        residual_home=home_residual,
        residual_away=away_residual,
    )


def poisson_probabilities(
    mean_home: float,
    mean_away: float,
    actual_home: int,
    actual_away: int,
) -> dict:
    diff = actual_home - actual_away
    total = actual_home + actual_away

    p_home_raw = 1.0 - skellam.cdf(0, mean_home, mean_away)
    p_away_raw = skellam.cdf(-1, mean_home, mean_away)
    resolved = p_home_raw + p_away_raw
    p_ml = 0.5 if resolved <= EPS else p_home_raw / resolved

    result = {
        "moneyline_home": float(p_ml),
        "runline_home_minus_1_5": float(
            1.0 - skellam.cdf(1, mean_home, mean_away)
        ),
        "runline_home_plus_1_5": float(
            1.0 - skellam.cdf(-2, mean_home, mean_away)
        ),
        "margin_prob": float(
            np.clip(
                skellam.pmf(diff, mean_home, mean_away),
                EPS,
                1.0,
            )
        ),
        "total_prob": float(
            np.clip(
                poisson.pmf(total, mean_home + mean_away),
                EPS,
                1.0,
            )
        ),
    }
    for line in COMMON_TOTAL_LINES:
        threshold = int(math.floor(line))
        result[f"over_{line:.1f}"] = float(
            1.0 - poisson.cdf(
                threshold,
                mean_home + mean_away,
            )
        )
    return result


def support_limit(
    mean_home: float,
    mean_away: float,
    var_home: float,
    var_away: float,
    actual_home: int,
    actual_away: int,
) -> int:
    upper = max(
        25.0,
        actual_home + 5.0,
        actual_away + 5.0,
        mean_home + 10.0 * math.sqrt(max(var_home, EPS)),
        mean_away + 10.0 * math.sqrt(max(var_away, EPS)),
    )
    return int(min(80, math.ceil(upper)))


def probabilities_from_joint(
    joint: np.ndarray,
    actual_home: int,
    actual_away: int,
) -> dict:
    joint = np.asarray(joint, dtype=float)
    mass = float(np.sum(joint))
    if not np.isfinite(mass) or mass <= 0:
        raise RuntimeError("Invalid joint probability mass")
    joint = joint / mass

    h = np.arange(joint.shape[0])[:, None]
    a = np.arange(joint.shape[1])[None, :]
    diff = h - a
    total = h + a

    home_raw = float(np.sum(joint[diff > 0]))
    away_raw = float(np.sum(joint[diff < 0]))
    resolved = home_raw + away_raw
    moneyline = 0.5 if resolved <= EPS else home_raw / resolved

    observed_diff = actual_home - actual_away
    observed_total = actual_home + actual_away

    result = {
        "moneyline_home": float(moneyline),
        "runline_home_minus_1_5": float(np.sum(joint[diff >= 2])),
        "runline_home_plus_1_5": float(np.sum(joint[diff >= -1])),
        "margin_prob": float(
            max(np.sum(joint[diff == observed_diff]), EPS)
        ),
        "total_prob": float(
            max(np.sum(joint[total == observed_total]), EPS)
        ),
    }
    for line in COMMON_TOTAL_LINES:
        result[f"over_{line:.1f}"] = float(np.sum(joint[total > line]))
    return result


def negative_binomial_probabilities(
    mean_home: float,
    mean_away: float,
    actual_home: int,
    actual_away: int,
    home_alpha: float,
    away_alpha: float,
) -> dict:
    var_home = mean_home + home_alpha * mean_home * mean_home
    var_away = mean_away + away_alpha * mean_away * mean_away
    max_runs = support_limit(
        mean_home,
        mean_away,
        var_home,
        var_away,
        actual_home,
        actual_away,
    )
    support = np.arange(max_runs + 1)

    home_size = 1.0 / home_alpha
    away_size = 1.0 / away_alpha
    home_prob = home_size / (home_size + mean_home)
    away_prob = away_size / (away_size + mean_away)

    hp = nbinom.pmf(support, home_size, home_prob)
    ap = nbinom.pmf(support, away_size, away_prob)
    joint = np.outer(hp, ap)
    return probabilities_from_joint(
        joint,
        actual_home,
        actual_away,
    )


def bivariate_total_pmf(
    total: int,
    lam_home: float,
    lam_away: float,
    lam_shared: float,
) -> float:
    base = lam_home + lam_away
    values = []
    for shared_count in range(total // 2 + 1):
        remainder = total - 2 * shared_count
        values.append(
            poisson.pmf(shared_count, lam_shared)
            * poisson.pmf(remainder, base)
        )
    return float(np.sum(values))


def bivariate_poisson_probabilities(
    mean_home: float,
    mean_away: float,
    actual_home: int,
    actual_away: int,
    theta: float,
) -> dict:
    shared = theta * min(mean_home, mean_away)
    lam_home = max(mean_home - shared, EPS)
    lam_away = max(mean_away - shared, EPS)
    lam_shared = max(shared, 0.0)

    diff = actual_home - actual_away
    total = actual_home + actual_away

    p_home_raw = 1.0 - skellam.cdf(0, lam_home, lam_away)
    p_away_raw = skellam.cdf(-1, lam_home, lam_away)
    resolved = p_home_raw + p_away_raw
    p_ml = 0.5 if resolved <= EPS else p_home_raw / resolved

    result = {
        "moneyline_home": float(p_ml),
        "runline_home_minus_1_5": float(
            1.0 - skellam.cdf(1, lam_home, lam_away)
        ),
        "runline_home_plus_1_5": float(
            1.0 - skellam.cdf(-2, lam_home, lam_away)
        ),
        "margin_prob": float(
            max(skellam.pmf(diff, lam_home, lam_away), EPS)
        ),
        "total_prob": float(
            max(
                bivariate_total_pmf(
                    total,
                    lam_home,
                    lam_away,
                    lam_shared,
                ),
                EPS,
            )
        ),
    }

    for line in COMMON_TOTAL_LINES:
        threshold = int(math.floor(line))
        cdf = sum(
            bivariate_total_pmf(
                t,
                lam_home,
                lam_away,
                lam_shared,
            )
            for t in range(threshold + 1)
        )
        result[f"over_{line:.1f}"] = float(
            np.clip(1.0 - cdf, 0.0, 1.0)
        )
    return result


def empirical_probabilities(
    mean_home: float,
    mean_away: float,
    actual_home: int,
    actual_away: int,
    residual_home: np.ndarray,
    residual_away: np.ndarray,
    simulations: int,
    rng: np.random.Generator,
) -> dict:
    if len(residual_home) != len(residual_away) or len(residual_home) == 0:
        raise RuntimeError("Invalid empirical residual pool")

    idx = rng.integers(
        0,
        len(residual_home),
        size=simulations,
    )
    home = np.rint(
        mean_home
        + residual_home[idx] * math.sqrt(max(mean_home, EPS))
    ).astype(int)
    away = np.rint(
        mean_away
        + residual_away[idx] * math.sqrt(max(mean_away, EPS))
    ).astype(int)
    home = np.maximum(home, 0)
    away = np.maximum(away, 0)

    diff = home - away
    total = home + away

    home_raw = float(np.mean(diff > 0))
    away_raw = float(np.mean(diff < 0))
    resolved = home_raw + away_raw
    p_ml = 0.5 if resolved <= EPS else home_raw / resolved

    observed_diff = actual_home - actual_away
    observed_total = actual_home + actual_away
    one_draw_floor = 1.0 / simulations

    result = {
        "moneyline_home": float(p_ml),
        "runline_home_minus_1_5": float(np.mean(diff >= 2)),
        "runline_home_plus_1_5": float(np.mean(diff >= -1)),
        "margin_prob": float(
            max(np.mean(diff == observed_diff), one_draw_floor)
        ),
        "total_prob": float(
            max(np.mean(total == observed_total), one_draw_floor)
        ),
    }
    for line in COMMON_TOTAL_LINES:
        result[f"over_{line:.1f}"] = float(np.mean(total > line))
    return result


def build_record(
    period: str,
    distribution: str,
    row: pd.Series,
    mean_home: float,
    mean_away: float,
    probs: dict,
) -> dict:
    actual_home = int(row["target_home_runs"])
    actual_away = int(row["target_away_runs"])
    diff = actual_home - actual_away
    total = actual_home + actual_away

    record = {
        "period": period,
        "distribution": distribution,
        "game_date": pd.Timestamp(row["_game_date_dt"]).strftime("%Y-%m-%d"),
        "game_id": str(row["game_id"]),
        "actual_home_runs": actual_home,
        "actual_away_runs": actual_away,
        "mean_home_runs": float(mean_home),
        "mean_away_runs": float(mean_away),
        "moneyline_home_prob": probs["moneyline_home"],
        "moneyline_home_win": float(diff > 0) if diff != 0 else np.nan,
        "runline_home_minus_1_5_prob": probs["runline_home_minus_1_5"],
        "runline_home_minus_1_5_win": float(diff >= 2),
        "runline_home_plus_1_5_prob": probs["runline_home_plus_1_5"],
        "runline_home_plus_1_5_win": float(diff >= -1),
        "margin_prob_observed": probs["margin_prob"],
        "total_prob_observed": probs["total_prob"],
    }
    for line in COMMON_TOTAL_LINES:
        key = f"{line:.1f}".replace(".", "_")
        record[f"over_{key}_prob"] = probs[f"over_{line:.1f}"]
        record[f"over_{key}_win"] = float(total > line)
    return record


def score_frame(frame: pd.DataFrame) -> dict:
    ml = frame.dropna(
        subset=["moneyline_home_win", "moneyline_home_prob"]
    )
    rl_y = np.concatenate(
        [
            frame["runline_home_minus_1_5_win"].to_numpy(dtype=float),
            frame["runline_home_plus_1_5_win"].to_numpy(dtype=float),
        ]
    )
    rl_p = np.concatenate(
        [
            frame["runline_home_minus_1_5_prob"].to_numpy(dtype=float),
            frame["runline_home_plus_1_5_prob"].to_numpy(dtype=float),
        ]
    )

    total_y_parts = []
    total_p_parts = []
    for line in COMMON_TOTAL_LINES:
        key = f"{line:.1f}".replace(".", "_")
        total_y_parts.append(frame[f"over_{key}_win"].to_numpy(dtype=float))
        total_p_parts.append(frame[f"over_{key}_prob"].to_numpy(dtype=float))
    total_y = np.concatenate(total_y_parts)
    total_p = np.concatenate(total_p_parts)

    return {
        "games": int(len(frame)),
        "moneyline_log_loss": binary_log_loss(
            ml["moneyline_home_win"],
            ml["moneyline_home_prob"],
        ),
        "moneyline_ece": ece(
            ml["moneyline_home_win"],
            ml["moneyline_home_prob"],
        ),
        "runline_log_loss": binary_log_loss(rl_y, rl_p),
        "runline_ece": ece(rl_y, rl_p),
        "totals_log_loss": binary_log_loss(total_y, total_p),
        "totals_ece": ece(total_y, total_p),
        "margin_nll": float(
            -np.log(
                np.clip(
                    frame["margin_prob_observed"].to_numpy(dtype=float),
                    EPS,
                    1.0,
                )
            ).mean()
        ),
        "total_nll": float(
            -np.log(
                np.clip(
                    frame["total_prob_observed"].to_numpy(dtype=float),
                    EPS,
                    1.0,
                )
            ).mean()
        ),
    }


def evaluate_period(
    period: str,
    eval_frame: pd.DataFrame,
    home_mu: np.ndarray,
    away_mu: np.ndarray,
    fit: DistributionFit,
    simulations: int,
    seed: int,
) -> list[dict]:
    rows: list[dict] = []
    rng = np.random.default_rng(seed)

    for i, (_, row) in enumerate(eval_frame.iterrows()):
        mh = float(home_mu[i])
        ma = float(away_mu[i])
        ah = int(row["target_home_runs"])
        aa = int(row["target_away_runs"])

        methods = {
            "poisson_skellam": poisson_probabilities(
                mh,
                ma,
                ah,
                aa,
            ),
            "negative_binomial": negative_binomial_probabilities(
                mh,
                ma,
                ah,
                aa,
                float(fit.home_alpha),
                float(fit.away_alpha),
            ),
            "bivariate_poisson": bivariate_poisson_probabilities(
                mh,
                ma,
                ah,
                aa,
                float(fit.bivariate_theta),
            ),
            "empirical_simulation": empirical_probabilities(
                mh,
                ma,
                ah,
                aa,
                fit.residual_home,
                fit.residual_away,
                simulations,
                rng,
            ),
        }

        for name, probabilities in methods.items():
            rows.append(
                build_record(
                    period,
                    name,
                    row,
                    mh,
                    ma,
                    probabilities,
                )
            )
    return rows


def parameter_row(
    period: str,
    fit: DistributionFit,
    fit_rows: int,
    simulations: int,
) -> dict:
    return {
        "period": period,
        "fit_rows": int(fit_rows),
        "negative_binomial_home_alpha": float(fit.home_alpha),
        "negative_binomial_away_alpha": float(fit.away_alpha),
        "bivariate_poisson_theta": float(fit.bivariate_theta),
        "empirical_residual_pairs": int(len(fit.residual_home)),
        "empirical_simulations_per_game": int(simulations),
    }


def verify_final_poisson(
    metrics: pd.DataFrame,
    home_meta: dict,
) -> None:
    row = metrics[
        (metrics["period"] == "final_test")
        & (metrics["distribution"] == "poisson_skellam")
    ]
    if len(row) != 1:
        raise RuntimeError("Missing final-test Poisson reproduction row")
    row = row.iloc[0]

    expected = home_meta["untouched_test_candidate_metrics"]
    checks = {
        "moneyline_log_loss": float(expected["moneyline_log_loss"]),
        "runline_minus_1_5_log_loss": float(
            expected["runline_minus_1_5_log_loss"]
        ),
        "runline_plus_1_5_log_loss": float(
            expected["runline_plus_1_5_log_loss"]
        ),
        "totals_log_loss": float(expected["totals_halfline_log_loss"]),
        "margin_nll": float(expected["margin_nll"]),
        "total_nll": float(expected["total_nll"]),
    }

    final_games = metrics.attrs["final_game_probabilities"]
    poisson_games = final_games[
        final_games["distribution"] == "poisson_skellam"
    ]
    minus_ll = binary_log_loss(
        poisson_games["runline_home_minus_1_5_win"],
        poisson_games["runline_home_minus_1_5_prob"],
    )
    plus_ll = binary_log_loss(
        poisson_games["runline_home_plus_1_5_win"],
        poisson_games["runline_home_plus_1_5_prob"],
    )
    actual = {
        "moneyline_log_loss": float(row["moneyline_log_loss"]),
        "runline_minus_1_5_log_loss": minus_ll,
        "runline_plus_1_5_log_loss": plus_ll,
        "totals_log_loss": float(row["totals_log_loss"]),
        "margin_nll": float(row["margin_nll"]),
        "total_nll": float(row["total_nll"]),
    }

    failures = []
    for key, expected_value in checks.items():
        delta = abs(actual[key] - expected_value)
        if delta > FINAL_REPRO_TOLERANCE:
            failures.append(
                f"{key}: actual={actual[key]:.12f} "
                f"expected={expected_value:.12f} delta={delta:.3g}"
            )
    if failures:
        raise RuntimeError(
            "Final-test Poisson reproduction failed; "
            + " | ".join(failures)
        )


def write_summary(
    path: Path,
    metrics: pd.DataFrame,
    params: pd.DataFrame,
) -> None:
    final = metrics[metrics["period"] == "final_test"].copy()
    final = final.sort_values(
        ["moneyline_log_loss", "runline_log_loss", "totals_log_loss"]
    )

    validation = metrics[
        metrics["period"] == "validation_combined"
    ].copy()
    validation = validation.sort_values(
        ["moneyline_log_loss", "runline_log_loss", "totals_log_loss"]
    )

    columns = [
        "distribution",
        "games",
        "moneyline_log_loss",
        "moneyline_ece",
        "runline_log_loss",
        "runline_ece",
        "totals_log_loss",
        "totals_ece",
        "margin_nll",
        "total_nll",
    ]

    text = [
        "# MLB Count-Distribution Backtest",
        "",
        "Research only. Production was not modified.",
        "",
        "## Validation combined",
        "",
        validation[columns].to_markdown(index=False, floatfmt=".8f"),
        "",
        "## Final test",
        "",
        final[columns].to_markdown(index=False, floatfmt=".8f"),
        "",
        "## Fitted distribution parameters",
        "",
        params.to_markdown(index=False, floatfmt=".8f"),
        "",
    ]
    path.write_text("\n".join(text), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=-1,
    )
    parser.add_argument(
        "--simulations",
        type=int,
        default=50000,
    )
    args = parser.parse_args()

    if args.simulations < 1000:
        raise RuntimeError("--simulations must be >= 1000")

    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    home_meta = load_json(HOME_METADATA_FILE)
    away_meta = load_json(AWAY_METADATA_FILE)
    validate_metadata(home_meta, away_meta)
    training = load_training(home_meta, away_meta)

    folds = home_meta["cv_folds"]
    all_game_records: list[dict] = []
    parameter_rows: list[dict] = []

    # Store leakage-safe OOF means/residuals for the final-test distribution fit.
    oof_home_y = []
    oof_away_y = []
    oof_home_mu = []
    oof_away_mu = []

    for fold_number, fold in enumerate(folds, 1):
        period = f"cv_fold_{fold_number}"
        train = date_slice(
            training,
            fold["train_start"],
            fold["train_end"],
        )
        validation = date_slice(
            training,
            fold["validation_start"],
            fold["validation_end"],
        )

        hm, am = fit_mean_pair(
            train,
            home_meta,
            away_meta,
            args.workers,
        )
        train_home_mu, train_away_mu = predict_pair(
            hm,
            am,
            train,
            home_meta,
            away_meta,
        )
        val_home_mu, val_away_mu = predict_pair(
            hm,
            am,
            validation,
            home_meta,
            away_meta,
        )

        fit = fit_distribution_parameters(
            train["target_home_runs"].to_numpy(dtype=int),
            train["target_away_runs"].to_numpy(dtype=int),
            train_home_mu,
            train_away_mu,
        )
        parameter_rows.append(
            parameter_row(
                period,
                fit,
                len(train),
                args.simulations,
            )
        )
        all_game_records.extend(
            evaluate_period(
                period,
                validation,
                val_home_mu,
                val_away_mu,
                fit,
                args.simulations,
                RANDOM_STATE + fold_number * 100000,
            )
        )

        oof_home_y.append(
            validation["target_home_runs"].to_numpy(dtype=int)
        )
        oof_away_y.append(
            validation["target_away_runs"].to_numpy(dtype=int)
        )
        oof_home_mu.append(val_home_mu)
        oof_away_mu.append(val_away_mu)

        print(
            f"{period}: train={len(train)} validation={len(validation)} "
            f"home_alpha={fit.home_alpha:.6f} "
            f"away_alpha={fit.away_alpha:.6f} "
            f"biv_theta={fit.bivariate_theta:.6f}",
            flush=True,
        )

    # Final test: selected run means are refit on every development row before
    # the test. Distribution parameters are fitted from pooled leakage-safe OOF
    # development residuals, not from final-test outcomes.
    test_start = home_meta["test_start_date"]
    test_end = home_meta["test_end_date"]
    development_start = folds[0]["train_start"]
    development_end = folds[-1]["validation_end"]

    development = date_slice(
        training,
        development_start,
        development_end,
    )
    final_test = date_slice(
        training,
        test_start,
        test_end,
    )

    final_hm, final_am = fit_mean_pair(
        development,
        home_meta,
        away_meta,
        args.workers,
    )
    final_home_mu, final_away_mu = predict_pair(
        final_hm,
        final_am,
        final_test,
        home_meta,
        away_meta,
    )

    final_fit = fit_distribution_parameters(
        np.concatenate(oof_home_y),
        np.concatenate(oof_away_y),
        np.concatenate(oof_home_mu),
        np.concatenate(oof_away_mu),
    )
    parameter_rows.append(
        parameter_row(
            "final_test",
            final_fit,
            len(np.concatenate(oof_home_y)),
            args.simulations,
        )
    )
    final_records = evaluate_period(
        "final_test",
        final_test,
        final_home_mu,
        final_away_mu,
        final_fit,
        args.simulations,
        RANDOM_STATE + 999999,
    )
    all_game_records.extend(final_records)

    games = pd.DataFrame(all_game_records)
    params = pd.DataFrame(parameter_rows)

    metric_rows = []
    for (period, distribution), group in games.groupby(
        ["period", "distribution"],
        sort=True,
    ):
        metric_rows.append(
            {
                "period": period,
                "distribution": distribution,
                **score_frame(group),
            }
        )

    validation_games = games[
        games["period"].str.startswith("cv_fold_")
    ]
    for distribution, group in validation_games.groupby(
        "distribution",
        sort=True,
    ):
        metric_rows.append(
            {
                "period": "validation_combined",
                "distribution": distribution,
                **score_frame(group),
            }
        )

    metrics = pd.DataFrame(metric_rows)
    metrics.attrs["final_game_probabilities"] = pd.DataFrame(final_records)
    verify_final_poisson(metrics, home_meta)

    games.to_csv(
        output_dir / "game_probabilities.csv",
        index=False,
        encoding="utf-8",
    )
    params.to_csv(
        output_dir / "distribution_parameters.csv",
        index=False,
        encoding="utf-8",
    )
    metrics.to_csv(
        output_dir / "distribution_metrics.csv",
        index=False,
        encoding="utf-8",
    )
    write_summary(
        output_dir / "distribution_backtest_summary.md",
        metrics,
        params,
    )

    print(
        "Backtest complete. "
        f"Output: {output_dir}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
