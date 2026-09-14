#!/usr/bin/env python3
"""
Item 13a: targeted refit/re-evaluation of MLB totals calibration.

Goal
----
Correct the chronological OOS totals overstatement found in item #12 for the
current production-calibrated probability ranges:
    0.30-0.40
    0.40-0.50
    0.50-0.60
    0.60-0.70
    0.70-0.80

Hard acceptance gate
--------------------
A replacement totals calibrator is accepted only if, on the SAME chronological
cross-fit rows used by item #8/#12:

1. Absolute calibration error is strictly lower than the current production
   totals calibrator in EACH of the five identified baseline cohorts.
2. In EACH of those five cohorts, the candidate no longer has
   bootstrap-95%-CI-confirmed overstatement. Formally, the lower bound of the
   candidate CI for (predicted - observed) must be <= 0.
3. Overall totals log loss does not worsen.
4. Overall totals ECE does not worsen.

The five cohorts are defined by CURRENT production-calibrated probabilities,
not by candidate probabilities. This prevents a candidate from "passing" by
simply moving observations into different buckets.

Chronology / leakage control
----------------------------
Candidate cross-fit predictions are generated strictly chronologically:
- cv_fold_2 candidate is fit only on cv_fold_1 OOS raw probabilities.
- cv_fold_3 candidate is fit only on cv_fold_1 + cv_fold_2 OOS raw probabilities.
- cv_fold_4 candidate is fit only on cv_fold_1 + cv_fold_2 + cv_fold_3 OOS raw probabilities.

cv_fold_1 raw totals records are reconstructed from item #6's Poisson OOS
run means, using the exact total contracts found in item #8's prediction file.

The final_test period is NEVER used for fitting, candidate selection, tuning,
or gate decisions. It is evaluated only after selection as a reference.

Candidate family
----------------
All candidates remain in the production-compatible monotone beta-logistic
family:
    q = sigmoid(c + a*log(p) - b*log(1-p)), with a>0 and b>0.

Candidate variants use different strengths of an explicit calibration penalty
on the five target ranges. No runtime model-family change is required.

If a candidate passes every gate, this script:
- fits that selected variant on all four CV OOS folds,
- writes a full candidate calibration artifact,
- backs up the production calibration artifact,
- replaces ONLY the total beta-logistic parameters in production.

Moneyline and run-line calibration entries are left unchanged.

Inputs
------
docs/win/baseball/mlb/modeling/probability_calibration/calibration_predictions.csv
docs/win/baseball/mlb/modeling/count_distribution_backtest/game_probabilities.csv
docs/win/baseball/mlb/models/probability_calibration/market_calibrators.json

Outputs
-------
docs/win/baseball/mlb/modeling/totals_calibration_13a/
    candidate_overall_metrics.csv
    target_bucket_metrics.csv
    candidate_gate_results.csv
    selected_candidate.json
    final_test_reference_metrics.csv
    item13a_summary.md
    market_calibrators_13a_candidate.json

Production artifact is changed ONLY if the hard gate passes and the existing
artifact exposes one unambiguous a/b/c beta-logistic parameter block for total.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import re
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit
from scipy.stats import poisson

BASE_DIR = Path("docs/win/baseball/mlb")

DEFAULT_PREDICTIONS = (
    BASE_DIR
    / "modeling/probability_calibration/calibration_predictions.csv"
)
DEFAULT_DISTRIBUTION_PROBS = (
    BASE_DIR
    / "modeling/count_distribution_backtest/game_probabilities.csv"
)
DEFAULT_ARTIFACT = (
    BASE_DIR
    / "models/probability_calibration/market_calibrators.json"
)
DEFAULT_OUTPUT_DIR = (
    BASE_DIR
    / "modeling/totals_calibration_13a"
)

TARGET_BUCKETS = [
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.60),
    (0.60, 0.70),
    (0.70, 0.80),
]

# Lambda=0 is the ordinary beta-logistic fit. Larger values increasingly
# penalize calibration gaps in the five target ranges.
PENALTY_LAMBDAS = [
    0.0,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
    64.0,
    128.0,
]

MIN_P = 1e-8
ECE_EDGES = np.linspace(0.0, 1.0, 11)
BOOTSTRAP_DRAWS = 20000
RANDOM_SEED = 130
GATE_TOL = 1e-12
MIN_TARGET_ROWS = 30


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / BASE_DIR).is_dir():
                return candidate
    raise RuntimeError("Could not locate repository root")


def resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def clip_prob(values) -> np.ndarray:
    return np.clip(
        np.asarray(values, dtype=float),
        MIN_P,
        1.0 - MIN_P,
    )


def log_loss(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = clip_prob(p)
    return float(
        np.mean(
            -(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
        )
    )


def ece(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)

    ids = np.digitize(
        np.clip(p, 0.0, 1.0),
        ECE_EDGES[1:-1],
        right=False,
    )

    n = len(y)
    if n == 0:
        return float("nan")

    value = 0.0
    for idx in range(10):
        mask = ids == idx
        if not np.any(mask):
            continue
        value += (
            float(np.sum(mask))
            / float(n)
            * abs(
                float(np.mean(p[mask]))
                - float(np.mean(y[mask]))
            )
        )
    return float(value)


def parse_contract_line(contract: str) -> float:
    matches = re.findall(
        r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)",
        str(contract),
    )
    if not matches:
        raise RuntimeError(
            f"Could not parse total line from contract={contract!r}"
        )
    return float(matches[-1])


def contract_side(contract: str) -> str:
    text = str(contract).lower()
    if "under" in text:
        return "under"
    if "over" in text:
        return "over"
    # Item #8 currently calibrates totals as over probabilities.
    # If the label contains no side, treat it as over.
    return "over"


def load_item8_predictions(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)

    required = {
        "market",
        "period",
        "game_date",
        "game_id",
        "contract",
        "y",
        "raw_p",
        "calibrated_p",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"{path} missing required columns: {missing}"
        )

    frame = frame[
        frame["market"].eq("total")
    ].copy()
    if frame.empty:
        raise RuntimeError(
            "No total rows found in calibration_predictions.csv"
        )

    frame["game_date"] = pd.to_datetime(
        frame["game_date"],
        errors="raise",
    )

    for col in ["y", "raw_p", "calibrated_p"]:
        frame[col] = pd.to_numeric(
            frame[col],
            errors="coerce",
        )
        if (
            frame[col].isna().any()
            or (~np.isfinite(frame[col])).any()
        ):
            raise RuntimeError(
                f"Invalid total calibration values in {col}"
            )

    frame["raw_p"] = clip_prob(frame["raw_p"])
    frame["calibrated_p"] = clip_prob(
        frame["calibrated_p"]
    )

    bad_y = ~frame["y"].isin([0.0, 1.0])
    if bad_y.any():
        raise RuntimeError(
            "Total calibration outcomes are not binary"
        )

    return frame.reset_index(drop=True)


def load_poisson_oos(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)

    required = {
        "period",
        "distribution",
        "game_date",
        "game_id",
        "actual_home_runs",
        "actual_away_runs",
        "mean_home_runs",
        "mean_away_runs",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"{path} missing required columns: {missing}"
        )

    frame = frame[
        frame["distribution"].eq("poisson_skellam")
    ].copy()

    if frame.empty:
        raise RuntimeError(
            "No poisson_skellam rows found in item #6 output"
        )

    frame["game_date"] = pd.to_datetime(
        frame["game_date"],
        errors="raise",
    )

    for col in [
        "actual_home_runs",
        "actual_away_runs",
        "mean_home_runs",
        "mean_away_runs",
    ]:
        frame[col] = pd.to_numeric(
            frame[col],
            errors="coerce",
        )
        if (
            frame[col].isna().any()
            or (~np.isfinite(frame[col])).any()
        ):
            raise RuntimeError(
                f"Invalid item #6 values in {col}"
            )

    return frame.reset_index(drop=True)


def reconstruct_fold1(
    item8: pd.DataFrame,
    item6: pd.DataFrame,
) -> pd.DataFrame:
    fold1_games = item6[
        item6["period"].eq("cv_fold_1")
    ].copy()
    if fold1_games.empty:
        raise RuntimeError(
            "item #6 output has no cv_fold_1 Poisson rows"
        )

    contracts = sorted(
        item8["contract"].astype(str).unique().tolist()
    )
    if not contracts:
        raise RuntimeError(
            "No total contracts found in item #8 predictions"
        )

    rows: list[dict] = []

    for game in fold1_games.itertuples(index=False):
        total_mean = (
            float(game.mean_home_runs)
            + float(game.mean_away_runs)
        )
        actual_total = (
            float(game.actual_home_runs)
            + float(game.actual_away_runs)
        )

        for contract in contracts:
            line = parse_contract_line(contract)
            side = contract_side(contract)

            is_integer = abs(line - round(line)) < 1e-9

            if is_integer:
                k = int(round(line))
                p_under = float(
                    poisson.cdf(k - 1, total_mean)
                )
                p_push = float(
                    poisson.pmf(k, total_mean)
                )
                p_over = float(
                    1.0 - poisson.cdf(k, total_mean)
                )
            else:
                threshold = int(math.floor(line))
                p_under = float(
                    poisson.cdf(threshold, total_mean)
                )
                p_push = 0.0
                p_over = float(1.0 - p_under)

            resolved = p_over + p_under
            if not np.isfinite(resolved) or resolved <= 0.0:
                raise RuntimeError(
                    "Invalid reconstructed total resolved mass"
                )

            # Pushes are excluded from calibration fitting/evaluation.
            if abs(actual_total - line) < 1e-9:
                continue

            if side == "over":
                raw_p = p_over / resolved
                y = float(actual_total > line)
            else:
                raw_p = p_under / resolved
                y = float(actual_total < line)

            rows.append(
                {
                    "market": "total",
                    "period": "cv_fold_1",
                    "game_date": game.game_date,
                    "game_id": game.game_id,
                    "contract": contract,
                    "y": y,
                    "raw_p": float(
                        np.clip(
                            raw_p,
                            MIN_P,
                            1.0 - MIN_P,
                        )
                    ),
                    "calibrated_p": np.nan,
                    "_source": "reconstructed_item6",
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError(
            "Reconstructed cv_fold_1 produced no total records"
        )

    print(
        "Reconstructed cv_fold_1 total records: "
        f"{len(out)} from {len(fold1_games)} games "
        f"across {len(contracts)} contracts"
    )

    return out


def sigmoid_beta(
    p,
    a: float,
    b: float,
    c: float,
) -> np.ndarray:
    p = clip_prob(p)
    z = (
        c
        + a * np.log(p)
        - b * np.log1p(-p)
    )
    return expit(z)


def unpack(theta: np.ndarray) -> tuple[float, float, float]:
    return (
        float(np.exp(theta[0])),
        float(np.exp(theta[1])),
        float(theta[2]),
    )


def standard_beta_fit(
    p,
    y,
) -> tuple[float, float, float]:
    p = clip_prob(p)
    y = np.asarray(y, dtype=float)

    def objective(theta):
        a, b, c = unpack(theta)
        q = sigmoid_beta(p, a, b, c)
        return log_loss(y, q)

    result = minimize(
        objective,
        x0=np.array([0.0, 0.0, 0.0]),
        method="L-BFGS-B",
        bounds=[
            (-5.0, 5.0),
            (-5.0, 5.0),
            (-8.0, 8.0),
        ],
        options={
            "maxiter": 3000,
            "ftol": 1e-12,
        },
    )
    if not result.success:
        raise RuntimeError(
            "Standard beta-logistic fit failed: "
            f"{result.message}"
        )
    return unpack(result.x)


def target_masks(
    reference_probability: np.ndarray,
) -> list[np.ndarray]:
    p = np.asarray(
        reference_probability,
        dtype=float,
    )
    masks = []
    for low, high in TARGET_BUCKETS:
        mask = (
            (p >= low)
            & (
                (p < high)
                if high < 1.0
                else (p <= high)
            )
        )
        masks.append(mask)
    return masks


def penalized_beta_fit(
    p,
    y,
    penalty_lambda: float,
) -> tuple[float, float, float]:
    p = clip_prob(p)
    y = np.asarray(y, dtype=float)

    base_a, base_b, base_c = standard_beta_fit(
        p,
        y,
    )
    reference_q = sigmoid_beta(
        p,
        base_a,
        base_b,
        base_c,
    )
    masks = target_masks(reference_q)

    theta0 = np.array(
        [
            math.log(base_a),
            math.log(base_b),
            base_c,
        ],
        dtype=float,
    )

    def objective(theta):
        a, b, c = unpack(theta)
        q = sigmoid_beta(p, a, b, c)
        value = log_loss(y, q)

        penalties = []
        for mask in masks:
            if int(np.sum(mask)) < MIN_TARGET_ROWS:
                continue
            gap = float(
                np.mean(q[mask])
                - np.mean(y[mask])
            )
            penalties.append(gap * gap)

        if penalties and penalty_lambda > 0.0:
            value += (
                float(penalty_lambda)
                * float(np.mean(penalties))
            )

        return float(value)

    result = minimize(
        objective,
        x0=theta0,
        method="L-BFGS-B",
        bounds=[
            (-5.0, 5.0),
            (-5.0, 5.0),
            (-8.0, 8.0),
        ],
        options={
            "maxiter": 4000,
            "ftol": 1e-12,
        },
    )

    if not result.success:
        raise RuntimeError(
            f"Penalized beta fit failed lambda={penalty_lambda}: "
            f"{result.message}"
        )

    return unpack(result.x)


def build_cv_training_records(
    item8: pd.DataFrame,
    fold1: pd.DataFrame,
) -> pd.DataFrame:
    # item8 has chronological cross-fit rows for folds 2-4.
    cv = item8[
        item8["period"].astype(str).str.startswith(
            "cv_fold_"
        )
    ].copy()

    raw_cols = [
        "market",
        "period",
        "game_date",
        "game_id",
        "contract",
        "y",
        "raw_p",
        "calibrated_p",
    ]

    combined = pd.concat(
        [
            fold1[raw_cols],
            cv[raw_cols],
        ],
        ignore_index=True,
        sort=False,
    )

    return combined


def fold_number(period: str) -> int:
    match = re.fullmatch(
        r"cv_fold_(\d+)",
        str(period),
    )
    if match is None:
        raise RuntimeError(
            f"Invalid CV period: {period}"
        )
    return int(match.group(1))


def chronological_candidate_predictions(
    cv_records: pd.DataFrame,
    eval_rows: pd.DataFrame,
    penalty_lambda: float,
) -> pd.DataFrame:
    outputs = []

    periods = sorted(
        eval_rows["period"]
        .astype(str)
        .unique()
        .tolist(),
        key=fold_number,
    )

    for period in periods:
        fold = fold_number(period)
        if fold <= 1:
            continue

        train = cv_records[
            cv_records["period"].map(
                fold_number
            ) < fold
        ].copy()

        validation = eval_rows[
            eval_rows["period"].eq(period)
        ].copy()

        if train.empty or validation.empty:
            raise RuntimeError(
                f"Missing train/validation rows for {period}"
            )

        a, b, c = penalized_beta_fit(
            train["raw_p"].to_numpy(dtype=float),
            train["y"].to_numpy(dtype=float),
            penalty_lambda,
        )

        validation["candidate_p"] = sigmoid_beta(
            validation["raw_p"].to_numpy(dtype=float),
            a,
            b,
            c,
        )
        validation["candidate_lambda"] = float(
            penalty_lambda
        )
        validation["fit_a"] = a
        validation["fit_b"] = b
        validation["fit_c"] = c
        validation["fit_rows"] = int(len(train))
        outputs.append(validation)

        print(
            f"lambda={penalty_lambda:g} {period}: "
            f"fit_rows={len(train)} "
            f"a={a:.6f} b={b:.6f} c={c:.6f}"
        )

    if not outputs:
        raise RuntimeError(
            "No chronological candidate predictions produced"
        )

    return pd.concat(
        outputs,
        ignore_index=True,
    )


def bootstrap_gap_ci(
    p,
    y,
    *,
    seed: int,
) -> tuple[float, float]:
    p = np.asarray(p, dtype=float)
    y = np.asarray(y, dtype=float)
    gap = p - y

    n = len(gap)
    if n == 0:
        return float("nan"), float("nan")

    rng = np.random.default_rng(seed)
    samples = np.empty(
        BOOTSTRAP_DRAWS,
        dtype=float,
    )

    chunk = 1000
    done = 0
    while done < BOOTSTRAP_DRAWS:
        size = min(
            chunk,
            BOOTSTRAP_DRAWS - done,
        )
        idx = rng.integers(
            0,
            n,
            size=(size, n),
        )
        samples[
            done : done + size
        ] = gap[idx].mean(axis=1)
        done += size

    low, high = np.quantile(
        samples,
        [0.025, 0.975],
    )
    return float(low), float(high)


def cohort_bucket_metrics(
    frame: pd.DataFrame,
    candidate_name: str,
) -> pd.DataFrame:
    rows = []

    baseline_p = frame[
        "calibrated_p"
    ].to_numpy(dtype=float)

    for index, (low, high) in enumerate(
        TARGET_BUCKETS
    ):
        mask = (
            (baseline_p >= low)
            & (baseline_p < high)
        )
        cohort = frame.loc[mask].copy()

        if len(cohort) < MIN_TARGET_ROWS:
            raise RuntimeError(
                f"Target bucket {low:.1f}-{high:.1f} has only "
                f"{len(cohort)} rows; expected at least "
                f"{MIN_TARGET_ROWS}"
            )

        y = cohort["y"].to_numpy(dtype=float)
        base = cohort[
            "calibrated_p"
        ].to_numpy(dtype=float)
        cand = cohort[
            "candidate_p"
        ].to_numpy(dtype=float)

        base_gap = float(
            np.mean(base) - np.mean(y)
        )
        cand_gap = float(
            np.mean(cand) - np.mean(y)
        )

        base_low, base_high = bootstrap_gap_ci(
            base,
            y,
            seed=(
                RANDOM_SEED
                + index * 100
                + 1
            ),
        )
        cand_low, cand_high = bootstrap_gap_ci(
            cand,
            y,
            seed=(
                RANDOM_SEED
                + index * 100
                + 2
                + sum(
                    ord(ch)
                    for ch in candidate_name
                )
            ),
        )

        rows.append(
            {
                "candidate": candidate_name,
                "bucket": f"{low:.1f}-{high:.1f}",
                "rows": int(len(cohort)),
                "baseline_mean_pred": float(
                    np.mean(base)
                ),
                "candidate_mean_pred": float(
                    np.mean(cand)
                ),
                "observed_rate": float(
                    np.mean(y)
                ),
                "baseline_gap_pred_minus_obs": base_gap,
                "candidate_gap_pred_minus_obs": cand_gap,
                "baseline_abs_error": abs(base_gap),
                "candidate_abs_error": abs(cand_gap),
                "abs_error_improved": bool(
                    abs(cand_gap)
                    < abs(base_gap) - GATE_TOL
                ),
                "baseline_gap_ci95_low": base_low,
                "baseline_gap_ci95_high": base_high,
                "candidate_gap_ci95_low": cand_low,
                "candidate_gap_ci95_high": cand_high,
                "candidate_confirmed_overstatement_removed": bool(
                    cand_low <= 0.0 + GATE_TOL
                ),
            }
        )

    return pd.DataFrame(rows)


def evaluate_candidate(
    frame: pd.DataFrame,
    candidate_name: str,
) -> tuple[dict, pd.DataFrame]:
    y = frame["y"].to_numpy(dtype=float)
    baseline = frame[
        "calibrated_p"
    ].to_numpy(dtype=float)
    candidate = frame[
        "candidate_p"
    ].to_numpy(dtype=float)

    baseline_ll = log_loss(y, baseline)
    candidate_ll = log_loss(y, candidate)
    baseline_ece = ece(y, baseline)
    candidate_ece = ece(y, candidate)

    buckets = cohort_bucket_metrics(
        frame,
        candidate_name,
    )

    all_bucket_error_improved = bool(
        buckets["abs_error_improved"].all()
    )
    all_overstatement_removed = bool(
        buckets[
            "candidate_confirmed_overstatement_removed"
        ].all()
    )
    ll_not_worse = bool(
        candidate_ll <= baseline_ll + GATE_TOL
    )
    ece_not_worse = bool(
        candidate_ece <= baseline_ece + GATE_TOL
    )

    passed = bool(
        all_bucket_error_improved
        and all_overstatement_removed
        and ll_not_worse
        and ece_not_worse
    )

    overall = {
        "candidate": candidate_name,
        "rows": int(len(frame)),
        "baseline_log_loss": baseline_ll,
        "candidate_log_loss": candidate_ll,
        "log_loss_delta_candidate_minus_baseline": (
            candidate_ll - baseline_ll
        ),
        "baseline_ece": baseline_ece,
        "candidate_ece": candidate_ece,
        "ece_delta_candidate_minus_baseline": (
            candidate_ece - baseline_ece
        ),
        "all_five_bucket_abs_errors_improved": (
            all_bucket_error_improved
        ),
        "all_five_confirmed_overstatements_removed": (
            all_overstatement_removed
        ),
        "overall_log_loss_not_worse": ll_not_worse,
        "overall_ece_not_worse": ece_not_worse,
        "gate_passed": passed,
    }

    return overall, buckets


def locate_abc_param_dict(
    total_record: dict,
) -> tuple[dict, str]:
    found: list[tuple[dict, str]] = []

    def walk(node, path: str) -> None:
        if isinstance(node, dict):
            keys = set(node.keys())
            if {"a", "b", "c"}.issubset(keys):
                values = [
                    node.get("a"),
                    node.get("b"),
                    node.get("c"),
                ]
                if all(
                    isinstance(v, (int, float))
                    for v in values
                ):
                    found.append((node, path))
            for key, value in node.items():
                walk(
                    value,
                    f"{path}.{key}",
                )
        elif isinstance(node, list):
            for idx, value in enumerate(node):
                walk(
                    value,
                    f"{path}[{idx}]",
                )

    walk(total_record, "markets.total")

    if len(found) != 1:
        raise RuntimeError(
            "Could not identify exactly one total beta-logistic "
            f"a/b/c parameter block; found={len(found)}"
        )

    return found[0]


def fit_selected_on_all_cv(
    cv_records: pd.DataFrame,
    penalty_lambda: float,
) -> tuple[float, float, float]:
    return penalized_beta_fit(
        cv_records["raw_p"].to_numpy(dtype=float),
        cv_records["y"].to_numpy(dtype=float),
        penalty_lambda,
    )


def evaluate_final_reference(
    item8: pd.DataFrame,
    params: tuple[float, float, float] | None,
) -> pd.DataFrame:
    final_rows = item8[
        item8["period"].eq("final_test")
    ].copy()

    if final_rows.empty:
        return pd.DataFrame(
            columns=[
                "system",
                "rows",
                "log_loss",
                "ece",
            ]
        )

    y = final_rows["y"].to_numpy(dtype=float)
    baseline = final_rows[
        "calibrated_p"
    ].to_numpy(dtype=float)

    rows = [
        {
            "system": "current_production_total_calibrator",
            "rows": int(len(final_rows)),
            "log_loss": log_loss(y, baseline),
            "ece": ece(y, baseline),
        }
    ]

    if params is not None:
        a, b, c = params
        candidate = sigmoid_beta(
            final_rows["raw_p"].to_numpy(dtype=float),
            a,
            b,
            c,
        )
        rows.append(
            {
                "system": "selected_13a_candidate",
                "rows": int(len(final_rows)),
                "log_loss": log_loss(y, candidate),
                "ece": ece(y, candidate),
            }
        )

    return pd.DataFrame(rows)


def write_summary(
    path: Path,
    overall: pd.DataFrame,
    buckets: pd.DataFrame,
    selected: dict,
    final_reference: pd.DataFrame,
) -> None:
    lines = [
        "# Item 13a — Targeted Totals Calibration Refit",
        "",
        "## Hard gate",
        "",
        (
            "A replacement is accepted only if all five current-production "
            "0.30-0.80 cohorts have strictly lower absolute calibration error, "
            "none retains bootstrap-95%-CI-confirmed overstatement, and combined "
            "chronological OOS totals log loss and ECE do not worsen."
        ),
        "",
        "The final-test period is reference-only and is not used for fitting, tuning, selection, or gating.",
        "",
        "## Candidate overall results",
        "",
        overall.to_markdown(index=False),
        "",
        "## Target-bucket results",
        "",
        buckets.to_markdown(index=False),
        "",
        "## Selection",
        "",
        "```json",
        json.dumps(selected, indent=2),
        "```",
        "",
        "## Final-test reference",
        "",
        (
            final_reference.to_markdown(index=False)
            if not final_reference.empty
            else "No final-test reference rows were available."
        ),
        "",
    ]

    path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Item 13a targeted totals calibration refit and hard gate"
        )
    )
    parser.add_argument(
        "--predictions",
        type=Path,
        default=DEFAULT_PREDICTIONS,
    )
    parser.add_argument(
        "--distribution-probabilities",
        type=Path,
        default=DEFAULT_DISTRIBUTION_PROBS,
    )
    parser.add_argument(
        "--artifact",
        type=Path,
        default=DEFAULT_ARTIFACT,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    parser.add_argument(
        "--no-promote",
        action="store_true",
        help=(
            "Evaluate only. Even if the hard gate passes, do not replace "
            "the production total calibrator."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = find_repo_root()

    predictions_path = resolve(
        root,
        args.predictions,
    )
    distribution_path = resolve(
        root,
        args.distribution_probabilities,
    )
    artifact_path = resolve(
        root,
        args.artifact,
    )
    output_dir = resolve(
        root,
        args.output_dir,
    )
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    item8 = load_item8_predictions(
        predictions_path
    )
    item6 = load_poisson_oos(
        distribution_path
    )

    fold1 = reconstruct_fold1(
        item8,
        item6,
    )
    cv_records = build_cv_training_records(
        item8,
        fold1,
    )

    eval_rows = item8[
        item8["period"].isin(
            [
                "cv_fold_2",
                "cv_fold_3",
                "cv_fold_4",
            ]
        )
    ].copy()

    if eval_rows.empty:
        raise RuntimeError(
            "No item #8 chronological cross-fit total rows found"
        )

    # Reproduce the known item #12 target-bucket overstatement before searching.
    baseline_probe = eval_rows.copy()
    baseline_probe["candidate_p"] = baseline_probe[
        "calibrated_p"
    ]
    baseline_buckets = cohort_bucket_metrics(
        baseline_probe,
        "baseline_reproduction",
    )
    expected_overstated = bool(
        (
            baseline_buckets[
                "baseline_gap_ci95_low"
            ] > 0.0
        ).all()
    )
    if not expected_overstated:
        raise RuntimeError(
            "Item #12 target-bucket overstatement did not reproduce "
            "across all five target buckets. Refusing to tune against "
            "a changed baseline."
        )

    overall_rows = []
    bucket_frames = []
    candidate_predictions: dict[str, pd.DataFrame] = {}

    for penalty_lambda in PENALTY_LAMBDAS:
        candidate_name = (
            "beta_logistic_standard"
            if penalty_lambda == 0.0
            else f"beta_bucket_penalty_{penalty_lambda:g}"
        )

        predictions = chronological_candidate_predictions(
            cv_records,
            eval_rows,
            penalty_lambda,
        )
        candidate_predictions[
            candidate_name
        ] = predictions

        overall, bucket_metrics = evaluate_candidate(
            predictions,
            candidate_name,
        )
        overall["penalty_lambda"] = float(
            penalty_lambda
        )
        overall_rows.append(overall)

        bucket_metrics[
            "penalty_lambda"
        ] = float(penalty_lambda)
        bucket_frames.append(
            bucket_metrics
        )

    overall_df = pd.DataFrame(
        overall_rows
    ).sort_values(
        [
            "gate_passed",
            "candidate_log_loss",
            "candidate_ece",
        ],
        ascending=[
            False,
            True,
            True,
        ],
    ).reset_index(drop=True)

    bucket_df = pd.concat(
        bucket_frames,
        ignore_index=True,
    )

    passing = overall_df[
        overall_df["gate_passed"]
    ].copy()

    selected_params = None
    production_changed = False
    backup_path = None
    candidate_artifact_path = (
        output_dir
        / "market_calibrators_13a_candidate.json"
    )

    if passing.empty:
        selected = {
            "status": "no_candidate_passed",
            "production_changed": False,
            "reason": (
                "No beta-logistic candidate satisfied every 13a gate."
            ),
        }
        selected_lambda = None
        selected_name = None
    else:
        winner = passing.sort_values(
            [
                "candidate_log_loss",
                "candidate_ece",
            ],
            ascending=[
                True,
                True,
            ],
        ).iloc[0]

        selected_name = str(
            winner["candidate"]
        )
        selected_lambda = float(
            winner["penalty_lambda"]
        )

        selected_params = fit_selected_on_all_cv(
            cv_records,
            selected_lambda,
        )
        a, b, c = selected_params

        artifact = json.loads(
            artifact_path.read_text(
                encoding="utf-8"
            )
        )

        if (
            "markets" not in artifact
            or "total" not in artifact["markets"]
        ):
            raise RuntimeError(
                "Production calibration artifact has no markets.total entry"
            )

        candidate_artifact = copy.deepcopy(
            artifact
        )
        param_dict, param_path = locate_abc_param_dict(
            candidate_artifact[
                "markets"
            ]["total"]
        )

        old_params = {
            "a": float(param_dict["a"]),
            "b": float(param_dict["b"]),
            "c": float(param_dict["c"]),
        }

        param_dict["a"] = float(a)
        param_dict["b"] = float(b)
        param_dict["c"] = float(c)

        total_record = candidate_artifact[
            "markets"
        ]["total"]
        total_record["item13a"] = {
            "selected_candidate": selected_name,
            "penalty_lambda": selected_lambda,
            "fit_rows": int(len(cv_records)),
            "selection_data": (
                "chronological OOS cv folds only; final_test excluded"
            ),
            "gate": (
                "all five target cohort absolute calibration errors improved; "
                "all confirmed overstatements removed; overall LL/ECE not worse"
            ),
        }

        candidate_artifact_path.write_text(
            json.dumps(
                candidate_artifact,
                indent=2,
                sort_keys=False,
            )
            + "\n",
            encoding="utf-8",
        )

        selected = {
            "status": (
                "candidate_passed_no_promote"
                if args.no_promote
                else "candidate_promoted"
            ),
            "candidate": selected_name,
            "penalty_lambda": selected_lambda,
            "production_fit": {
                "a": float(a),
                "b": float(b),
                "c": float(c),
                "fit_rows": int(len(cv_records)),
            },
            "replaced_parameter_block": param_path,
            "old_parameters": old_params,
            "candidate_artifact": str(
                candidate_artifact_path
            ),
            "production_changed": False,
        }

        if not args.no_promote:
            timestamp = datetime.now().strftime(
                "%Y%m%d_%H%M%S"
            )
            backup_path = artifact_path.with_name(
                artifact_path.name
                + f".item13a_{timestamp}.bak"
            )
            shutil.copy2(
                artifact_path,
                backup_path,
            )
            shutil.copy2(
                candidate_artifact_path,
                artifact_path,
            )
            production_changed = True
            selected[
                "production_changed"
            ] = True
            selected[
                "production_backup"
            ] = str(backup_path)

    final_reference = evaluate_final_reference(
        item8,
        selected_params,
    )

    gate_path = (
        output_dir
        / "candidate_gate_results.csv"
    )
    bucket_path = (
        output_dir
        / "target_bucket_metrics.csv"
    )
    overall_path = (
        output_dir
        / "candidate_overall_metrics.csv"
    )
    selected_path = (
        output_dir
        / "selected_candidate.json"
    )
    final_path = (
        output_dir
        / "final_test_reference_metrics.csv"
    )
    summary_path = (
        output_dir
        / "item13a_summary.md"
    )

    overall_df.to_csv(
        overall_path,
        index=False,
    )
    bucket_df.to_csv(
        bucket_path,
        index=False,
    )

    gate_cols = [
        "candidate",
        "penalty_lambda",
        "all_five_bucket_abs_errors_improved",
        "all_five_confirmed_overstatements_removed",
        "overall_log_loss_not_worse",
        "overall_ece_not_worse",
        "gate_passed",
    ]
    overall_df[
        gate_cols
    ].to_csv(
        gate_path,
        index=False,
    )

    selected_path.write_text(
        json.dumps(
            selected,
            indent=2,
            sort_keys=False,
        )
        + "\n",
        encoding="utf-8",
    )
    final_reference.to_csv(
        final_path,
        index=False,
    )

    write_summary(
        summary_path,
        overall_df,
        bucket_df,
        selected,
        final_reference,
    )

    print()
    print("Item 13a evaluation complete.")
    print()
    print(
        overall_df[
            [
                "candidate",
                "penalty_lambda",
                "baseline_log_loss",
                "candidate_log_loss",
                "baseline_ece",
                "candidate_ece",
                "all_five_bucket_abs_errors_improved",
                "all_five_confirmed_overstatements_removed",
                "overall_log_loss_not_worse",
                "overall_ece_not_worse",
                "gate_passed",
            ]
        ].to_string(index=False)
    )
    print()
    print("Selection:")
    print(
        json.dumps(
            selected,
            indent=2,
        )
    )
    print()
    print("Final-test reference only:")
    print(
        final_reference.to_string(
            index=False
        )
        if not final_reference.empty
        else "No final-test rows"
    )
    print()
    print(f"Overall metrics: {overall_path}")
    print(f"Bucket metrics: {bucket_path}")
    print(f"Gate results: {gate_path}")
    print(f"Selection: {selected_path}")
    print(f"Final reference: {final_path}")
    print(f"Summary: {summary_path}")

    if production_changed:
        print(
            "PRODUCTION TOTAL CALIBRATOR UPDATED "
            f"(backup: {backup_path})"
        )
    else:
        print(
            "PRODUCTION TOTAL CALIBRATOR UNCHANGED"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
