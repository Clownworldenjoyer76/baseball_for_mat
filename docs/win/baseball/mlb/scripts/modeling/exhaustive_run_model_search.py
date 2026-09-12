#!/usr/bin/env python3
"""
Staged intensive coupled MLB run-model search.

This is a research script. It NEVER modifies production models.

Design:
- Reuses the repository's existing training-set loader, feature contract, validation,
  and chronological 70/15/15 split from train_run_model.py.
- The final 15% chronological test partition is never used to select a model.
- The first 85% is the development set.
- Model selection uses expanding-window chronological CV inside the development set.
- Home and away candidates are searched separately.
- A diverse top shortlist from each side is then exhaustively paired.
- Pair selection uses coupled probability quality:
    * home Poisson deviance
    * away Poisson deviance
    * moneyline log loss
    * home -1.5 log loss
    * home +1.5 log loss
    * totals log loss over common half-run totals
    * exact run-margin negative log likelihood
    * exact game-total negative log likelihood
- Every pair metric is normalized to the DRatings baseline on exactly the same
  validation observations. Lower is better.
- Only after a winning validation pair is fixed is it refit on all development rows
  and evaluated once on the untouched final test period.
- Search output is checkpointed and resumable.

The search uses a broad finite stage-1 grid, then creates a dense local stage-2
neighborhood only around candidates that actually perform well out of sample.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import itertools
import json
import math
import os
import platform
import shutil
import time
import traceback
import warnings
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
import scipy
from scipy.stats import poisson, skellam
import sklearn
from sklearn.ensemble import (
    ExtraTreesRegressor,
    HistGradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import PoissonRegressor, TweedieRegressor
from sklearn.metrics import mean_absolute_error, mean_poisson_deviance
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    import lightgbm
    from lightgbm import LGBMRegressor
except Exception as exc:
    lightgbm = None
    LGBMRegressor = None
    LIGHTGBM_IMPORT_ERROR = repr(exc)
else:
    LIGHTGBM_IMPORT_ERROR = None

try:
    import xgboost
    from xgboost import XGBRegressor
except Exception as exc:
    xgboost = None
    XGBRegressor = None
    XGBOOST_IMPORT_ERROR = repr(exc)
else:
    XGBOOST_IMPORT_ERROR = None


BASE_DIR = Path("docs/win/baseball/mlb")
TRAINER_PATH = BASE_DIR / "scripts/modeling/train_run_model.py"
DEFAULT_INPUT = BASE_DIR / "modeling/data/mlb_run_training_set.csv"
DEFAULT_OUTPUT = BASE_DIR / "modeling/exhaustive_run_model_search"

EPS = 1e-12
RANDOM_STATE = 42
COMMON_TOTAL_LINES = np.array([6.5, 7.5, 8.5, 9.5, 10.5, 11.5], dtype=float)

RESULT_COLUMNS = [
    "candidate_id", "side", "family", "feature_set", "params_json", "status",
    "validation_rows", "validation_mae", "validation_poisson",
    "baseline_mae", "baseline_poisson", "mae_ratio", "poisson_ratio",
    "elapsed_seconds", "error",
]

PAIR_COLUMNS = [
    "home_candidate_id", "away_candidate_id",
    "home_family", "away_family",
    "home_feature_set", "away_feature_set",
    "home_validation_poisson", "away_validation_poisson",
    "home_poisson_ratio", "away_poisson_ratio",
    "moneyline_log_loss", "moneyline_ratio",
    "runline_minus_1_5_log_loss", "runline_minus_1_5_ratio",
    "runline_plus_1_5_log_loss", "runline_plus_1_5_ratio",
    "totals_halfline_log_loss", "totals_halfline_ratio",
    "margin_nll", "margin_nll_ratio",
    "total_nll", "total_nll_ratio",
    "composite_ratio", "worst_component_ratio",
]


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
    path = (ROOT / path).resolve() if not path.is_absolute() else path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TRAIN = load_module("exhaustive_train_run_model", TRAINER_PATH)


def log(message: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    line = f"{now_iso()} | {message}"
    print(line, flush=True)
    with (output_dir / "exhaustive_search.log").open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def make_candidate_id(side: str, family: str, feature_set: str, params: dict) -> str:
    raw = f"{side}|{family}|{feature_set}|{stable_json(params)}".encode("utf-8")
    return hashlib.blake2b(raw, digest_size=10).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def append_csv(path: Path, rows: list[dict], columns: list[str]) -> None:
    if not rows:
        return
    frame = pd.DataFrame(rows)
    for col in columns:
        if col not in frame.columns:
            frame[col] = np.nan
    frame = frame[columns]
    frame.to_csv(
        path,
        mode="a",
        header=not path.exists(),
        index=False,
        encoding="utf-8",
    )


def safe_poisson(y_true, y_pred) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    if np.any(~np.isfinite(y_pred)) or np.any(y_pred <= 0):
        raise ValueError("Poisson scoring requires finite positive predictions")
    return float(mean_poisson_deviance(y_true, np.maximum(y_pred, EPS)))


def binary_log_loss(y, p, axis=None):
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    return np.mean(-(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)), axis=axis)


def date_range(frame: pd.DataFrame) -> tuple[str, str]:
    dates = pd.to_datetime(frame["_game_date_dt"])
    return dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")


@dataclass(frozen=True)
class Fold:
    fold_id: int
    train_index: np.ndarray
    validation_index: np.ndarray
    train_start: str
    train_end: str
    validation_start: str
    validation_end: str


def build_walk_forward_folds(
    development: pd.DataFrame,
    fold_count: int,
    initial_train_fraction: float,
) -> list[Fold]:
    dates = np.array(sorted(development["_game_date_dt"].drop_duplicates().tolist()))
    initial = int(math.floor(len(dates) * initial_train_fraction))
    initial = max(3, min(initial, len(dates) - fold_count))
    future_dates = dates[initial:]
    chunks = [c for c in np.array_split(future_dates, fold_count) if len(c)]

    folds: list[Fold] = []
    for number, val_dates in enumerate(chunks, 1):
        first_val = val_dates[0]
        train_dates = set(dates[dates < first_val])
        val_dates_set = set(val_dates)
        train_idx = np.flatnonzero(development["_game_date_dt"].isin(train_dates).to_numpy())
        val_idx = np.flatnonzero(development["_game_date_dt"].isin(val_dates_set).to_numpy())

        if len(train_idx) == 0 or len(val_idx) == 0:
            raise RuntimeError(f"Empty chronological CV fold {number}")

        train_frame = development.iloc[train_idx]
        val_frame = development.iloc[val_idx]
        tr_start, tr_end = date_range(train_frame)
        va_start, va_end = date_range(val_frame)

        folds.append(
            Fold(
                fold_id=number,
                train_index=train_idx,
                validation_index=val_idx,
                train_start=tr_start,
                train_end=tr_end,
                validation_start=va_start,
                validation_end=va_end,
            )
        )
    return folds


def combined_oof_rows(folds: list[Fold]) -> np.ndarray:
    return np.concatenate([f.validation_index for f in folds])


def build_feature_sets(features: list[str]) -> dict[str, list[str]]:
    weather_set = set(TRAIN.OPTIONAL_NUMERIC_FEATURE_COLUMNS)
    dratings = [c for c in features if c.startswith("dratings_")]
    pitcher = [c for c in features if "_sp_" in c]
    bullpen = [c for c in features if "_bp_" in c]
    weather = [c for c in features if c in weather_set]

    raw = {
        "all": list(features),
        "no_weather": [c for c in features if c not in weather_set],
        "dratings_only": dratings,
        "no_dratings": [c for c in features if not c.startswith("dratings_")],
        "no_dratings_prob": [
            c for c in features
            if c not in {"dratings_home_prob", "dratings_away_prob"}
        ],
        "dratings_plus_pitcher": list(dict.fromkeys([*dratings, *pitcher, *weather])),
        "dratings_plus_bullpen": list(dict.fromkeys([*dratings, *bullpen, *weather])),
        "pitcher_plus_bullpen": list(dict.fromkeys([*pitcher, *bullpen, *weather])),
        "pitcher_only": list(dict.fromkeys([*pitcher, *weather])),
        "bullpen_only": list(dict.fromkeys([*bullpen, *weather])),
    }

    output: dict[str, list[str]] = {}
    seen: set[tuple[str, ...]] = set()
    for name, cols in raw.items():
        cols = [c for c in cols if c in features]
        if not cols:
            continue
        key = tuple(cols)
        if key in seen:
            continue
        seen.add(key)
        output[name] = cols
    return output


def hist_grid(smoke: bool) -> Iterable[dict]:
    """Broad stage-1 HistGradientBoosting grid."""
    if smoke:
        values = itertools.product(
            ["poisson"], [0.05], [15], [20], [1.0], [200]
        )
    else:
        values = itertools.product(
            ["poisson", "squared_error"],
            [0.025, 0.05, 0.10],
            [15, 31],
            [10, 30],
            [0.0, 5.0],
            [300],
        )
    for loss, lr, leaves, min_leaf, l2, iterations in values:
        yield {
            "loss": loss,
            "learning_rate": lr,
            "max_leaf_nodes": leaves,
            "min_samples_leaf": min_leaf,
            "l2_regularization": l2,
            "max_iter": iterations,
        }


def lightgbm_grid(smoke: bool) -> Iterable[dict]:
    """Broad stage-1 LightGBM grid."""
    objectives = [("poisson", None)] if smoke else [
        ("poisson", None),
        ("tweedie", 1.5),
        ("regression", None),
    ]
    lrs = [0.05] if smoke else [0.025, 0.05, 0.10]
    leaves = [15] if smoke else [15, 31]
    mins = [20] if smoke else [10, 30]

    for (objective, tweedie_power), lr, nleaf, min_child in itertools.product(
        objectives, lrs, leaves, mins
    ):
        yield {
            "objective": objective,
            "tweedie_variance_power": tweedie_power,
            "learning_rate": lr,
            "num_leaves": nleaf,
            "min_child_samples": min_child,
            "reg_lambda": 1.0,
            "n_estimators": 500 if not smoke else 250,
            "subsample": 0.90 if not smoke else 1.0,
            "colsample_bytree": 0.90 if not smoke else 1.0,
        }


def xgboost_grid(smoke: bool) -> Iterable[dict]:
    """Broad stage-1 XGBoost grid."""
    objectives = [("count:poisson", None)] if smoke else [
        ("count:poisson", None),
        ("reg:tweedie", 1.5),
        ("reg:squarederror", None),
    ]
    lrs = [0.05] if smoke else [0.025, 0.05, 0.10]
    depths = [3] if smoke else [3, 6]
    mins = [1.0] if smoke else [1.0, 5.0]

    for (objective, tweedie_power), lr, depth, min_child in itertools.product(
        objectives, lrs, depths, mins
    ):
        yield {
            "objective": objective,
            "tweedie_variance_power": tweedie_power,
            "learning_rate": lr,
            "max_depth": depth,
            "min_child_weight": min_child,
            "reg_lambda": 1.0,
            "n_estimators": 500 if not smoke else 250,
            "subsample": 0.90 if not smoke else 1.0,
            "colsample_bytree": 0.90 if not smoke else 1.0,
        }


def forest_grid(smoke: bool) -> Iterable[dict]:
    """Broad stage-1 RandomForest / ExtraTrees grid."""
    criteria = ["poisson"] if smoke else ["poisson", "squared_error"]
    estimators = [300] if smoke else [400]
    depths = [None] if smoke else [None, 14]
    mins = [3] if smoke else [2, 8]
    features = [0.8] if smoke else [0.60, 1.0]
    samples = [None]

    for vals in itertools.product(
        criteria, estimators, depths, mins, features, samples
    ):
        criterion, ne, depth, min_leaf, max_feat, max_samples = vals
        yield {
            "criterion": criterion,
            "n_estimators": ne,
            "max_depth": depth,
            "min_samples_leaf": min_leaf,
            "max_features": max_feat,
            "max_samples": max_samples,
        }


def poisson_glm_grid(smoke: bool) -> Iterable[dict]:
    alphas = [1.0] if smoke else [0.0, 0.01, 0.10, 1.0, 10.0]
    for alpha in alphas:
        yield {"alpha": alpha, "solver": "lbfgs"}


def tweedie_glm_grid(smoke: bool) -> Iterable[dict]:
    powers = [1.5] if smoke else [1.20, 1.50, 1.80]
    alphas = [1.0] if smoke else [0.01, 0.10, 1.0]
    for power, alpha in itertools.product(powers, alphas):
        yield {"power": power, "alpha": alpha, "solver": "lbfgs"}


def family_grids(smoke: bool) -> dict[str, list[dict]]:
    """Stage-1 broad grids. Stage 2 is generated around actual winners."""
    grids = {
        "hist_gradient_boosting": list(hist_grid(smoke)),
        "random_forest": list(forest_grid(smoke)),
        "extra_trees": list(forest_grid(smoke)),
        "poisson_glm": list(poisson_glm_grid(smoke)),
        "tweedie_glm": list(tweedie_glm_grid(smoke)),
    }
    if LGBMRegressor is not None:
        grids["lightgbm"] = list(lightgbm_grid(smoke))
    if XGBRegressor is not None:
        grids["xgboost"] = list(xgboost_grid(smoke))
    return grids


def _unique_dicts(items: Iterable[dict]) -> list[dict]:
    seen: set[str] = set()
    output: list[dict] = []
    for item in items:
        key = stable_json(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _positive_lr_values(value: float) -> list[float]:
    vals = [
        max(0.005, min(0.20, value * factor))
        for factor in (0.65, 0.80, 1.0, 1.20, 1.50)
    ]
    return sorted({round(v, 6) for v in vals})


def _nearest_values(value: float, universe: list[float], count: int = 3) -> list:
    ordered = sorted(universe, key=lambda x: (abs(float(x) - float(value)), float(x)))
    chosen = ordered[:count]
    if value not in chosen:
        chosen = [value, *chosen[: max(0, count - 1)]]
    return list(dict.fromkeys(chosen))


def refinement_neighbors(
    family: str,
    base: dict,
    intensive: bool,
) -> list[dict]:
    """
    Generate a local stage-2 neighborhood around one strong stage-1 result.

    Most seeds receive one-factor-at-a-time perturbations. The very best seeds
    also receive a small local Cartesian search over the most influential tree
    complexity parameters. This captures interactions without restoring the
    original multi-week global Cartesian explosion.
    """
    candidates: list[dict] = [dict(base)]

    def one_factor(key: str, values: Iterable[Any]) -> None:
        for value in values:
            p = dict(base)
            p[key] = value
            candidates.append(p)

    if family == "hist_gradient_boosting":
        one_factor("learning_rate", _positive_lr_values(float(base["learning_rate"])))
        one_factor(
            "max_leaf_nodes",
            _nearest_values(float(base["max_leaf_nodes"]), [7, 15, 31, 63], 4),
        )
        one_factor(
            "min_samples_leaf",
            _nearest_values(float(base["min_samples_leaf"]), [5, 10, 20, 30, 40, 60], 4),
        )
        one_factor(
            "l2_regularization",
            _nearest_values(float(base["l2_regularization"]), [0.0, 0.5, 1.0, 5.0, 10.0, 20.0], 4),
        )
        one_factor(
            "max_iter",
            _nearest_values(float(base["max_iter"]), [200, 300, 500, 800], 3),
        )
        one_factor(
            "loss",
            ["poisson", "squared_error"],
        )

        if intensive:
            lrs = _nearest_values(float(base["learning_rate"]), _positive_lr_values(float(base["learning_rate"])), 3)
            leaves = _nearest_values(float(base["max_leaf_nodes"]), [7, 15, 31, 63], 3)
            mins = _nearest_values(float(base["min_samples_leaf"]), [5, 10, 20, 30, 40, 60], 3)
            for lr, leaves_n, min_leaf in itertools.product(lrs, leaves, mins):
                p = dict(base)
                p["learning_rate"] = lr
                p["max_leaf_nodes"] = int(leaves_n)
                p["min_samples_leaf"] = int(min_leaf)
                candidates.append(p)

    elif family == "lightgbm":
        one_factor("learning_rate", _positive_lr_values(float(base["learning_rate"])))
        one_factor("num_leaves", _nearest_values(float(base["num_leaves"]), [7, 15, 31, 63], 4))
        one_factor("min_child_samples", _nearest_values(float(base["min_child_samples"]), [5, 10, 20, 30, 50], 4))
        one_factor("reg_lambda", _nearest_values(float(base["reg_lambda"]), [0.0, 0.5, 1.0, 5.0, 15.0], 4))
        one_factor("n_estimators", _nearest_values(float(base["n_estimators"]), [250, 500, 800, 1200], 3))
        one_factor("subsample", [0.75, 0.90, 1.0])
        one_factor("colsample_bytree", [0.70, 0.85, 1.0])

        objective_options = [
            ("poisson", None),
            ("tweedie", 1.20),
            ("tweedie", 1.50),
            ("tweedie", 1.80),
            ("regression", None),
            ("regression_l1", None),
        ]
        for objective, power in objective_options:
            p = dict(base)
            p["objective"] = objective
            p["tweedie_variance_power"] = power
            candidates.append(p)

        if intensive:
            lrs = _nearest_values(float(base["learning_rate"]), _positive_lr_values(float(base["learning_rate"])), 3)
            leaves = _nearest_values(float(base["num_leaves"]), [7, 15, 31, 63], 3)
            mins = _nearest_values(float(base["min_child_samples"]), [5, 10, 20, 30, 50], 3)
            for lr, leaves_n, min_child in itertools.product(lrs, leaves, mins):
                p = dict(base)
                p["learning_rate"] = lr
                p["num_leaves"] = int(leaves_n)
                p["min_child_samples"] = int(min_child)
                candidates.append(p)

    elif family == "xgboost":
        one_factor("learning_rate", _positive_lr_values(float(base["learning_rate"])))
        one_factor("max_depth", _nearest_values(float(base["max_depth"]), [2, 3, 4, 6, 8], 4))
        one_factor("min_child_weight", _nearest_values(float(base["min_child_weight"]), [0.5, 1.0, 3.0, 5.0, 10.0], 4))
        one_factor("reg_lambda", _nearest_values(float(base["reg_lambda"]), [0.0, 0.5, 1.0, 5.0, 15.0], 4))
        one_factor("n_estimators", _nearest_values(float(base["n_estimators"]), [250, 500, 800, 1200], 3))
        one_factor("subsample", [0.75, 0.90, 1.0])
        one_factor("colsample_bytree", [0.70, 0.85, 1.0])

        objective_options = [
            ("count:poisson", None),
            ("reg:tweedie", 1.20),
            ("reg:tweedie", 1.50),
            ("reg:tweedie", 1.80),
            ("reg:squarederror", None),
            ("reg:absoluteerror", None),
        ]
        for objective, power in objective_options:
            p = dict(base)
            p["objective"] = objective
            p["tweedie_variance_power"] = power
            candidates.append(p)

        if intensive:
            lrs = _nearest_values(float(base["learning_rate"]), _positive_lr_values(float(base["learning_rate"])), 3)
            depths = _nearest_values(float(base["max_depth"]), [2, 3, 4, 6, 8], 3)
            mins = _nearest_values(float(base["min_child_weight"]), [0.5, 1.0, 3.0, 5.0, 10.0], 3)
            for lr, depth, min_child in itertools.product(lrs, depths, mins):
                p = dict(base)
                p["learning_rate"] = lr
                p["max_depth"] = int(depth)
                p["min_child_weight"] = float(min_child)
                candidates.append(p)

    elif family in {"random_forest", "extra_trees"}:
        one_factor("criterion", ["poisson", "squared_error", "absolute_error"])
        one_factor("n_estimators", [250, 500, 800])
        depth_values = [None, 8, 14, 22, 32]
        for depth in depth_values:
            p = dict(base)
            p["max_depth"] = depth
            candidates.append(p)
        one_factor("min_samples_leaf", [1, 2, 4, 8, 15])
        one_factor("max_features", [0.40, 0.60, 0.80, 1.0])
        if family == "random_forest":
            one_factor("max_samples", [0.70, 0.85, None])

        if intensive:
            depth_local = [None, 10, 18] if base["max_depth"] is None else _nearest_values(float(base["max_depth"]), [8, 14, 22, 32], 3)
            min_local = _nearest_values(float(base["min_samples_leaf"]), [1, 2, 4, 8, 15], 3)
            feat_local = _nearest_values(float(base["max_features"]), [0.40, 0.60, 0.80, 1.0], 3)
            for depth, min_leaf, max_feat in itertools.product(depth_local, min_local, feat_local):
                p = dict(base)
                p["max_depth"] = depth
                p["min_samples_leaf"] = int(min_leaf)
                p["max_features"] = float(max_feat)
                candidates.append(p)

    elif family == "poisson_glm":
        alpha = float(base["alpha"])
        one_factor(
            "alpha",
            sorted({
                0.0,
                max(1e-6, alpha / 10.0),
                max(1e-6, alpha / 3.0),
                alpha,
                max(1e-6, alpha * 3.0),
                max(1e-6, alpha * 10.0),
            }),
        )
        one_factor("solver", ["lbfgs", "newton-cholesky"])

    elif family == "tweedie_glm":
        alpha = float(base["alpha"])
        power = float(base["power"])
        one_factor(
            "alpha",
            sorted({
                max(1e-6, alpha / 10.0),
                max(1e-6, alpha / 3.0),
                alpha,
                max(1e-6, alpha * 3.0),
                max(1e-6, alpha * 10.0),
            }),
        )
        power_values = sorted({
            round(max(1.01, min(1.95, power + delta)), 3)
            for delta in (-0.25, -0.10, 0.0, 0.10, 0.25)
        })
        one_factor("power", power_values)
        one_factor("solver", ["lbfgs", "newton-cholesky"])

        if intensive:
            alpha_values = sorted({
                max(1e-6, alpha / 3.0),
                alpha,
                max(1e-6, alpha * 3.0),
            })
            power_local = sorted({
                round(max(1.01, min(1.95, power + delta)), 3)
                for delta in (-0.10, 0.0, 0.10)
            })
            for new_power, new_alpha in itertools.product(power_local, alpha_values):
                p = dict(base)
                p["power"] = new_power
                p["alpha"] = new_alpha
                candidates.append(p)

    else:
        raise ValueError(f"Unknown refinement family: {family}")

    return _unique_dicts(candidates)


def select_refinement_seeds(
    results: pd.DataFrame,
    top_overall: int = 10,
) -> pd.DataFrame:
    """
    Keep strong global candidates while preserving family and feature diversity.
    Maximum seed count is approximately top_overall + families + feature sets.
    """
    ok = results[results["status"] == "ok"].copy()
    ok["validation_poisson"] = pd.to_numeric(ok["validation_poisson"], errors="coerce")
    ok["validation_mae"] = pd.to_numeric(ok["validation_mae"], errors="coerce")
    ok = ok[np.isfinite(ok["validation_poisson"])]
    if ok.empty:
        raise RuntimeError("No successful candidates available for refinement")

    parts = [
        ok.nsmallest(min(top_overall, len(ok)), "validation_poisson")
    ]

    for _, group in ok.groupby("family", sort=True):
        parts.append(group.nsmallest(1, "validation_poisson"))

    for _, group in ok.groupby("feature_set", sort=True):
        parts.append(group.nsmallest(1, "validation_poisson"))

    seeds = pd.concat(parts, ignore_index=True).drop_duplicates("candidate_id")
    seeds = seeds.sort_values(
        ["validation_poisson", "validation_mae", "candidate_id"],
        kind="stable",
    ).reset_index(drop=True)
    return seeds


def build_refinement_configs(
    side: str,
    seeds: pd.DataFrame,
    intensive_seed_count: int = 5,
) -> list[dict]:
    intensive_ids = set(
        seeds.head(min(intensive_seed_count, len(seeds)))["candidate_id"].astype(str)
    )

    configs: list[dict] = []
    seen: set[str] = set()

    for row in seeds.itertuples(index=False):
        base = json.loads(row.params_json)
        intensive = str(row.candidate_id) in intensive_ids

        for params in refinement_neighbors(row.family, base, intensive):
            cid = make_candidate_id(side, row.family, row.feature_set, params)
            if cid in seen:
                continue
            seen.add(cid)
            configs.append({
                "candidate_id": cid,
                "side": side,
                "family": row.family,
                "feature_set": row.feature_set,
                "params": params,
            })

    return configs


def make_model(family: str, params: dict, workers: int):
    if family == "hist_gradient_boosting":
        return HistGradientBoostingRegressor(
            loss=params["loss"],
            learning_rate=params["learning_rate"],
            max_leaf_nodes=params["max_leaf_nodes"],
            min_samples_leaf=params["min_samples_leaf"],
            l2_regularization=params["l2_regularization"],
            max_iter=params["max_iter"],
            early_stopping=False,
            random_state=RANDOM_STATE,
        )

    if family == "lightgbm":
        kwargs = dict(
            objective=params["objective"],
            learning_rate=params["learning_rate"],
            num_leaves=params["num_leaves"],
            min_child_samples=params["min_child_samples"],
            reg_lambda=params["reg_lambda"],
            n_estimators=params["n_estimators"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            random_state=RANDOM_STATE,
            n_jobs=workers,
            verbosity=-1,
        )
        if params["objective"] == "tweedie":
            kwargs["tweedie_variance_power"] = params["tweedie_variance_power"]
        return LGBMRegressor(**kwargs)

    if family == "xgboost":
        kwargs = dict(
            objective=params["objective"],
            learning_rate=params["learning_rate"],
            max_depth=params["max_depth"],
            min_child_weight=params["min_child_weight"],
            reg_lambda=params["reg_lambda"],
            n_estimators=params["n_estimators"],
            subsample=params["subsample"],
            colsample_bytree=params["colsample_bytree"],
            random_state=RANDOM_STATE,
            n_jobs=workers,
            tree_method="hist",
            verbosity=0,
        )
        if params["objective"] == "reg:tweedie":
            kwargs["tweedie_variance_power"] = params["tweedie_variance_power"]
        return XGBRegressor(**kwargs)

    if family in {"random_forest", "extra_trees"}:
        klass = RandomForestRegressor if family == "random_forest" else ExtraTreesRegressor
        bootstrap = family == "random_forest" or params["max_samples"] is not None
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", klass(
                criterion=params["criterion"],
                n_estimators=params["n_estimators"],
                max_depth=params["max_depth"],
                min_samples_leaf=params["min_samples_leaf"],
                max_features=params["max_features"],
                max_samples=params["max_samples"] if bootstrap else None,
                bootstrap=bootstrap,
                random_state=RANDOM_STATE,
                n_jobs=workers,
            )),
        ])

    if family == "poisson_glm":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            ("model", PoissonRegressor(
                alpha=params["alpha"],
                solver=params["solver"],
                max_iter=3000,
                tol=1e-8,
            )),
        ])

    if family == "tweedie_glm":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            ("model", TweedieRegressor(
                power=params["power"],
                alpha=params["alpha"],
                link="log",
                solver=params["solver"],
                max_iter=3000,
                tol=1e-8,
            )),
        ])

    raise ValueError(f"Unknown family: {family}")


def side_columns(side: str) -> tuple[str, str]:
    if side == "home":
        return "target_home_runs", "dratings_home_projected_runs"
    if side == "away":
        return "target_away_runs", "dratings_away_projected_runs"
    raise ValueError(side)


def baseline_oof(
    development: pd.DataFrame,
    folds: list[Fold],
    target_col: str,
    baseline_col: str,
) -> tuple[np.ndarray, np.ndarray]:
    actual_parts = []
    pred_parts = []
    for fold in folds:
        val = development.iloc[fold.validation_index]
        actual_parts.append(val[target_col].to_numpy(dtype=float))
        pred_parts.append(val[baseline_col].to_numpy(dtype=float))
    actual = np.concatenate(actual_parts)
    pred = np.concatenate(pred_parts)
    if np.any(~np.isfinite(pred)) or np.any(pred <= 0):
        raise RuntimeError(f"Invalid DRatings baseline values in {baseline_col}")
    return actual, pred


def cv_predict_candidate(
    development: pd.DataFrame,
    folds: list[Fold],
    features: list[str],
    target_col: str,
    family: str,
    params: dict,
    workers: int,
) -> tuple[np.ndarray, np.ndarray]:
    actual_parts = []
    pred_parts = []

    for fold in folds:
        train = development.iloc[fold.train_index]
        val = development.iloc[fold.validation_index]
        model = make_model(family, params, workers)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(train[features], train[target_col])
            pred = np.asarray(model.predict(val[features]), dtype=float)

        if np.any(~np.isfinite(pred)) or np.any(pred <= 0):
            raise RuntimeError(
                f"{family} emitted invalid predictions: "
                f"nonfinite={int(np.sum(~np.isfinite(pred)))} "
                f"nonpositive={int(np.sum(pred <= 0))}"
            )

        actual_parts.append(val[target_col].to_numpy(dtype=float))
        pred_parts.append(pred)

    return np.concatenate(actual_parts), np.concatenate(pred_parts)


def enumerate_configs(
    side: str,
    feature_sets: dict[str, list[str]],
    grids: dict[str, list[dict]],
):
    for feature_name in feature_sets:
        for family, params_list in grids.items():
            for params in params_list:
                yield {
                    "candidate_id": make_candidate_id(side, family, feature_name, params),
                    "side": side,
                    "family": family,
                    "feature_set": feature_name,
                    "params": params,
                }


def search_side(
    side: str,
    development: pd.DataFrame,
    folds: list[Fold],
    feature_sets: dict[str, list[str]],
    grids: dict[str, list[dict]],
    output_dir: Path,
    workers: int,
    checkpoint_every: int,
    max_candidates: int | None,
    result_label: str = "coarse",
) -> pd.DataFrame:
    result_path = output_dir / f"{side}_{result_label}_results.csv"
    if result_path.exists():
        existing = pd.read_csv(result_path, dtype={"candidate_id": str})
        done = set(existing["candidate_id"].astype(str))
    else:
        done = set()

    target_col, baseline_col = side_columns(side)
    base_actual, base_pred = baseline_oof(
        development, folds, target_col, baseline_col
    )
    baseline_mae = float(mean_absolute_error(base_actual, base_pred))
    baseline_poisson = safe_poisson(base_actual, base_pred)

    total_grid = len(feature_sets) * sum(len(v) for v in grids.values())
    log(
        f"{side.upper()} search | total={total_grid} already_done={len(done)} "
        f"baseline_poisson={baseline_poisson:.8f}",
        output_dir,
    )

    pending: list[dict] = []
    completed_this_run = 0

    for config in enumerate_configs(side, feature_sets, grids):
        cid = config["candidate_id"]
        if cid in done:
            continue

        if max_candidates is not None and completed_this_run >= max_candidates:
            break

        start = time.perf_counter()
        status = "ok"
        error = ""
        val_mae = np.nan
        val_poisson = np.nan

        try:
            actual, pred = cv_predict_candidate(
                development,
                folds,
                feature_sets[config["feature_set"]],
                target_col,
                config["family"],
                config["params"],
                workers,
            )
            val_mae = float(mean_absolute_error(actual, pred))
            val_poisson = safe_poisson(actual, pred)
        except Exception as exc:
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"

        elapsed = time.perf_counter() - start
        pending.append({
            "candidate_id": cid,
            "side": side,
            "family": config["family"],
            "feature_set": config["feature_set"],
            "params_json": stable_json(config["params"]),
            "status": status,
            "validation_rows": len(base_actual),
            "validation_mae": val_mae,
            "validation_poisson": val_poisson,
            "baseline_mae": baseline_mae,
            "baseline_poisson": baseline_poisson,
            "mae_ratio": val_mae / baseline_mae if np.isfinite(val_mae) else np.nan,
            "poisson_ratio": val_poisson / baseline_poisson if np.isfinite(val_poisson) else np.nan,
            "elapsed_seconds": elapsed,
            "error": error,
        })
        completed_this_run += 1

        if len(pending) >= checkpoint_every:
            append_csv(result_path, pending, RESULT_COLUMNS)
            ok_batch = [x for x in pending if x["status"] == "ok"]
            msg = (
                f"{side.upper()} checkpoint | this_run={completed_this_run} "
                f"saved={len(pending)}"
            )
            if ok_batch:
                best = min(ok_batch, key=lambda x: x["validation_poisson"])
                msg += f" batch_best={best['validation_poisson']:.8f}"
            log(msg, output_dir)
            pending.clear()

    append_csv(result_path, pending, RESULT_COLUMNS)

    result = pd.read_csv(result_path, dtype={"candidate_id": str})
    log(
        f"{side.upper()} candidate search stored | rows={len(result)} "
        f"ok={int((result['status'] == 'ok').sum())} "
        f"failed={int((result['status'] == 'failed').sum())}",
        output_dir,
    )
    return result



def search_explicit_configs(
    side: str,
    configs: list[dict],
    development: pd.DataFrame,
    folds: list[Fold],
    feature_sets: dict[str, list[str]],
    output_dir: Path,
    workers: int,
    checkpoint_every: int,
    result_label: str,
    max_candidates: int | None = None,
) -> pd.DataFrame:
    """Evaluate an explicit stage-2 configuration list with checkpoint/resume."""
    result_path = output_dir / f"{side}_{result_label}_results.csv"

    if result_path.exists():
        existing = pd.read_csv(result_path, dtype={"candidate_id": str})
        done = set(existing["candidate_id"].astype(str))
    else:
        done = set()

    target_col, baseline_col = side_columns(side)
    base_actual, base_pred = baseline_oof(
        development, folds, target_col, baseline_col
    )
    baseline_mae = float(mean_absolute_error(base_actual, base_pred))
    baseline_poisson = safe_poisson(base_actual, base_pred)

    log(
        f"{side.upper()} {result_label} search | total={len(configs)} "
        f"already_done={len(done)} baseline_poisson={baseline_poisson:.8f}",
        output_dir,
    )

    pending: list[dict] = []
    completed_this_run = 0

    for config in configs:
        cid = config["candidate_id"]
        if cid in done:
            continue

        if max_candidates is not None and completed_this_run >= max_candidates:
            break

        start = time.perf_counter()
        status = "ok"
        error = ""
        val_mae = np.nan
        val_poisson = np.nan

        try:
            actual, pred = cv_predict_candidate(
                development,
                folds,
                feature_sets[config["feature_set"]],
                target_col,
                config["family"],
                config["params"],
                workers,
            )
            val_mae = float(mean_absolute_error(actual, pred))
            val_poisson = safe_poisson(actual, pred)
        except Exception as exc:
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"

        elapsed = time.perf_counter() - start
        pending.append({
            "candidate_id": cid,
            "side": side,
            "family": config["family"],
            "feature_set": config["feature_set"],
            "params_json": stable_json(config["params"]),
            "status": status,
            "validation_rows": len(base_actual),
            "validation_mae": val_mae,
            "validation_poisson": val_poisson,
            "baseline_mae": baseline_mae,
            "baseline_poisson": baseline_poisson,
            "mae_ratio": val_mae / baseline_mae if np.isfinite(val_mae) else np.nan,
            "poisson_ratio": val_poisson / baseline_poisson if np.isfinite(val_poisson) else np.nan,
            "elapsed_seconds": elapsed,
            "error": error,
        })
        completed_this_run += 1

        if len(pending) >= checkpoint_every:
            append_csv(result_path, pending, RESULT_COLUMNS)
            ok_batch = [x for x in pending if x["status"] == "ok"]
            msg = (
                f"{side.upper()} {result_label} checkpoint | "
                f"this_run={completed_this_run} saved={len(pending)}"
            )
            if ok_batch:
                best = min(ok_batch, key=lambda x: x["validation_poisson"])
                msg += f" batch_best={best['validation_poisson']:.8f}"
            log(msg, output_dir)
            pending.clear()

    append_csv(result_path, pending, RESULT_COLUMNS)

    if not result_path.exists():
        return pd.DataFrame(columns=RESULT_COLUMNS)

    result = pd.read_csv(result_path, dtype={"candidate_id": str})
    log(
        f"{side.upper()} {result_label} stored | rows={len(result)} "
        f"ok={int((result['status'] == 'ok').sum())} "
        f"failed={int((result['status'] == 'failed').sum())}",
        output_dir,
    )
    return result


def combine_candidate_results(
    coarse: pd.DataFrame,
    refined: pd.DataFrame,
) -> pd.DataFrame:
    frames = [frame for frame in (coarse, refined) if frame is not None and not frame.empty]
    if not frames:
        raise RuntimeError("No candidate results to combine")

    combined = pd.concat(frames, ignore_index=True)
    combined["validation_poisson"] = pd.to_numeric(
        combined["validation_poisson"], errors="coerce"
    )
    combined["validation_mae"] = pd.to_numeric(
        combined["validation_mae"], errors="coerce"
    )
    combined = combined.sort_values(
        ["validation_poisson", "validation_mae"],
        kind="stable",
        na_position="last",
    )
    combined = combined.drop_duplicates("candidate_id", keep="first").reset_index(drop=True)
    return combined


def shortlist(
    results: pd.DataFrame,
    top_k: int,
    diversity_per_group: int,
) -> pd.DataFrame:
    ok = results[results["status"] == "ok"].copy()
    ok["validation_poisson"] = pd.to_numeric(ok["validation_poisson"], errors="coerce")
    ok["validation_mae"] = pd.to_numeric(ok["validation_mae"], errors="coerce")
    ok = ok[np.isfinite(ok["validation_poisson"])]
    if ok.empty:
        raise RuntimeError("No successful candidates")

    parts = [ok.nsmallest(min(top_k, len(ok)), "validation_poisson")]
    if diversity_per_group:
        for _, group in ok.groupby(["family", "feature_set"], sort=True):
            parts.append(
                group.nsmallest(
                    min(diversity_per_group, len(group)),
                    "validation_poisson",
                )
            )

    short = pd.concat(parts, ignore_index=True).drop_duplicates("candidate_id")
    short = short.sort_values(
        ["validation_poisson", "validation_mae", "candidate_id"],
        kind="stable",
    ).reset_index(drop=True)
    return short


def shortlist_oof_matrix(
    side: str,
    short: pd.DataFrame,
    development: pd.DataFrame,
    folds: list[Fold],
    feature_sets: dict[str, list[str]],
    workers: int,
    output_dir: Path,
) -> tuple[np.ndarray, np.ndarray]:
    target_col, _ = side_columns(side)
    actual_ref = None
    preds = []

    for n, row in enumerate(short.itertuples(index=False), 1):
        actual, pred = cv_predict_candidate(
            development,
            folds,
            feature_sets[row.feature_set],
            target_col,
            row.family,
            json.loads(row.params_json),
            workers,
        )
        if actual_ref is None:
            actual_ref = actual
        elif not np.array_equal(actual_ref, actual):
            raise RuntimeError("OOF row-order mismatch")

        preds.append(pred)

        if n % 25 == 0 or n == len(short):
            log(f"{side.upper()} OOF shortlist predictions {n}/{len(short)}", output_dir)

    return np.vstack(preds), actual_ref


def pair_probability_metrics(
    hp: np.ndarray,
    ap: np.ndarray,
    actual_home: np.ndarray,
    actual_away: np.ndarray,
) -> dict[str, float]:
    diff = actual_home - actual_away
    total = actual_home + actual_away

    ml_mask = diff != 0
    p_home_raw = 1.0 - skellam.cdf(0, hp, ap)
    p_away_raw = skellam.cdf(-1, hp, ap)
    resolved = p_home_raw + p_away_raw
    p_ml = np.divide(
        p_home_raw,
        resolved,
        out=np.full_like(p_home_raw, 0.5),
        where=resolved > EPS,
    )
    y_ml = (diff > 0).astype(float)

    p_minus = 1.0 - skellam.cdf(1, hp, ap)
    y_minus = (diff >= 2).astype(float)

    p_plus = 1.0 - skellam.cdf(-2, hp, ap)
    y_plus = (diff >= -1).astype(float)

    lam_total = hp + ap
    total_losses = []
    for line in COMMON_TOTAL_LINES:
        threshold = int(math.floor(line))
        p_over = 1.0 - poisson.cdf(threshold, lam_total)
        y_over = (total > line).astype(float)
        total_losses.append(float(binary_log_loss(y_over, p_over)))

    margin_prob = np.clip(skellam.pmf(diff, hp, ap), EPS, 1.0)
    total_prob = np.clip(poisson.pmf(total, lam_total), EPS, 1.0)

    return {
        "moneyline_log_loss": float(binary_log_loss(y_ml[ml_mask], p_ml[ml_mask])),
        "runline_minus_1_5_log_loss": float(binary_log_loss(y_minus, p_minus)),
        "runline_plus_1_5_log_loss": float(binary_log_loss(y_plus, p_plus)),
        "totals_halfline_log_loss": float(np.mean(total_losses)),
        "margin_nll": float(-np.log(margin_prob).mean()),
        "total_nll": float(-np.log(total_prob).mean()),
    }


def validation_baseline(
    development: pd.DataFrame,
    folds: list[Fold],
) -> tuple[dict[str, float], np.ndarray, np.ndarray]:
    oof = development.iloc[combined_oof_rows(folds)]
    actual_home = oof["target_home_runs"].to_numpy(dtype=float)
    actual_away = oof["target_away_runs"].to_numpy(dtype=float)
    base_home = oof["dratings_home_projected_runs"].to_numpy(dtype=float)
    base_away = oof["dratings_away_projected_runs"].to_numpy(dtype=float)

    metrics = pair_probability_metrics(
        base_home, base_away, actual_home, actual_away
    )
    metrics["home_poisson"] = safe_poisson(actual_home, base_home)
    metrics["away_poisson"] = safe_poisson(actual_away, base_away)
    return metrics, actual_home, actual_away


def search_pairs(
    home_short: pd.DataFrame,
    away_short: pd.DataFrame,
    home_matrix: np.ndarray,
    away_matrix: np.ndarray,
    actual_home: np.ndarray,
    actual_away: np.ndarray,
    baseline: dict[str, float],
    output_dir: Path,
    batch_size: int,
) -> pd.DataFrame:
    pair_path = output_dir / "pair_results.csv"

    done = set()
    if pair_path.exists():
        old = pd.read_csv(
            pair_path,
            usecols=["home_candidate_id", "away_candidate_id"],
            dtype=str,
        )
        done = set(zip(old["home_candidate_id"], old["away_candidate_id"]))

    home_ids = home_short["candidate_id"].astype(str).tolist()
    away_ids = away_short["candidate_id"].astype(str).tolist()
    h_lookup = home_short.set_index("candidate_id")
    a_lookup = away_short.set_index("candidate_id")

    diff = actual_home - actual_away
    total = actual_home + actual_away
    y_ml = (diff > 0).astype(float)
    ml_mask = diff != 0
    y_minus = (diff >= 2).astype(float)
    y_plus = (diff >= -1).astype(float)

    pairs_iter = (
        (hi, ai)
        for hi in range(len(home_ids))
        for ai in range(len(away_ids))
        if (home_ids[hi], away_ids[ai]) not in done
    )

    total_pairs = len(home_ids) * len(away_ids)
    processed = len(done)

    log(
        f"PAIR search | home={len(home_ids)} away={len(away_ids)} "
        f"total={total_pairs} already_done={len(done)}",
        output_dir,
    )

    while True:
        batch = list(itertools.islice(pairs_iter, batch_size))
        if not batch:
            break

        hi = np.array([x[0] for x in batch], dtype=int)
        ai = np.array([x[1] for x in batch], dtype=int)
        hp = home_matrix[hi, :]
        ap = away_matrix[ai, :]

        p_home_raw = 1.0 - skellam.cdf(0, hp, ap)
        p_away_raw = skellam.cdf(-1, hp, ap)
        resolved = p_home_raw + p_away_raw
        p_ml = np.divide(
            p_home_raw,
            resolved,
            out=np.full_like(p_home_raw, 0.5),
            where=resolved > EPS,
        )
        ml_ll = binary_log_loss(y_ml[None, ml_mask], p_ml[:, ml_mask], axis=1)

        p_minus = 1.0 - skellam.cdf(1, hp, ap)
        rl_minus = binary_log_loss(y_minus[None, :], p_minus, axis=1)

        p_plus = 1.0 - skellam.cdf(-2, hp, ap)
        rl_plus = binary_log_loss(y_plus[None, :], p_plus, axis=1)

        lam_total = hp + ap
        total_parts = []
        for line in COMMON_TOTAL_LINES:
            threshold = int(math.floor(line))
            p_over = 1.0 - poisson.cdf(threshold, lam_total)
            y_over = (total > line).astype(float)
            total_parts.append(binary_log_loss(y_over[None, :], p_over, axis=1))
        total_ll = np.mean(np.vstack(total_parts), axis=0)

        margin_prob = np.clip(
            skellam.pmf(diff[None, :], hp, ap),
            EPS,
            1.0,
        )
        margin_nll = -np.log(margin_prob).mean(axis=1)

        total_prob = np.clip(
            poisson.pmf(total[None, :], lam_total),
            EPS,
            1.0,
        )
        total_nll = -np.log(total_prob).mean(axis=1)

        rows = []
        for j, (h_idx, a_idx) in enumerate(batch):
            hid = home_ids[h_idx]
            aid = away_ids[a_idx]
            hr = h_lookup.loc[hid]
            ar = a_lookup.loc[aid]

            ratios = {
                "home_poisson_ratio": float(hr["validation_poisson"]) / baseline["home_poisson"],
                "away_poisson_ratio": float(ar["validation_poisson"]) / baseline["away_poisson"],
                "moneyline_ratio": float(ml_ll[j]) / baseline["moneyline_log_loss"],
                "runline_minus_1_5_ratio": float(rl_minus[j]) / baseline["runline_minus_1_5_log_loss"],
                "runline_plus_1_5_ratio": float(rl_plus[j]) / baseline["runline_plus_1_5_log_loss"],
                "totals_halfline_ratio": float(total_ll[j]) / baseline["totals_halfline_log_loss"],
                "margin_nll_ratio": float(margin_nll[j]) / baseline["margin_nll"],
                "total_nll_ratio": float(total_nll[j]) / baseline["total_nll"],
            }
            vals = np.array(list(ratios.values()), dtype=float)
            composite = float(np.exp(np.log(vals).mean()))
            worst = float(vals.max())

            rows.append({
                "home_candidate_id": hid,
                "away_candidate_id": aid,
                "home_family": hr["family"],
                "away_family": ar["family"],
                "home_feature_set": hr["feature_set"],
                "away_feature_set": ar["feature_set"],
                "home_validation_poisson": float(hr["validation_poisson"]),
                "away_validation_poisson": float(ar["validation_poisson"]),
                "home_poisson_ratio": ratios["home_poisson_ratio"],
                "away_poisson_ratio": ratios["away_poisson_ratio"],
                "moneyline_log_loss": float(ml_ll[j]),
                "moneyline_ratio": ratios["moneyline_ratio"],
                "runline_minus_1_5_log_loss": float(rl_minus[j]),
                "runline_minus_1_5_ratio": ratios["runline_minus_1_5_ratio"],
                "runline_plus_1_5_log_loss": float(rl_plus[j]),
                "runline_plus_1_5_ratio": ratios["runline_plus_1_5_ratio"],
                "totals_halfline_log_loss": float(total_ll[j]),
                "totals_halfline_ratio": ratios["totals_halfline_ratio"],
                "margin_nll": float(margin_nll[j]),
                "margin_nll_ratio": ratios["margin_nll_ratio"],
                "total_nll": float(total_nll[j]),
                "total_nll_ratio": ratios["total_nll_ratio"],
                "composite_ratio": composite,
                "worst_component_ratio": worst,
            })

        append_csv(pair_path, rows, PAIR_COLUMNS)
        processed += len(batch)

        if processed % max(batch_size * 10, 1) < batch_size:
            best = min(rows, key=lambda r: r["composite_ratio"])
            log(
                f"PAIR checkpoint | processed={processed}/{total_pairs} "
                f"batch_best={best['composite_ratio']:.8f}",
                output_dir,
            )

    pairs = pd.read_csv(pair_path, dtype={
        "home_candidate_id": str,
        "away_candidate_id": str,
    })
    pairs["composite_ratio"] = pd.to_numeric(pairs["composite_ratio"], errors="coerce")
    pairs["worst_component_ratio"] = pd.to_numeric(
        pairs["worst_component_ratio"], errors="coerce"
    )
    pairs = pairs.sort_values(
        ["composite_ratio", "worst_component_ratio"],
        kind="stable",
    ).reset_index(drop=True)
    pairs.to_csv(pair_path, index=False)
    return pairs


def fit_development_winner(
    row: pd.Series,
    development: pd.DataFrame,
    feature_sets: dict[str, list[str]],
    side: str,
    workers: int,
):
    target_col, _ = side_columns(side)
    params = json.loads(row["params_json"])
    features = feature_sets[row["feature_set"]]
    model = make_model(row["family"], params, workers)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model.fit(development[features], development[target_col])
    return model, features, params


def evaluate_test(
    home_model,
    away_model,
    home_features: list[str],
    away_features: list[str],
    test: pd.DataFrame,
) -> tuple[dict, dict, dict, np.ndarray, np.ndarray]:
    home_pred = np.asarray(home_model.predict(test[home_features]), dtype=float)
    away_pred = np.asarray(away_model.predict(test[away_features]), dtype=float)

    if np.any(~np.isfinite(home_pred)) or np.any(home_pred <= 0):
        raise RuntimeError("Winning home model produced invalid untouched-test predictions")
    if np.any(~np.isfinite(away_pred)) or np.any(away_pred <= 0):
        raise RuntimeError("Winning away model produced invalid untouched-test predictions")

    y_home = test["target_home_runs"].to_numpy(dtype=float)
    y_away = test["target_away_runs"].to_numpy(dtype=float)
    base_home = test["dratings_home_projected_runs"].to_numpy(dtype=float)
    base_away = test["dratings_away_projected_runs"].to_numpy(dtype=float)

    candidate = pair_probability_metrics(home_pred, away_pred, y_home, y_away)
    candidate["home_poisson"] = safe_poisson(y_home, home_pred)
    candidate["away_poisson"] = safe_poisson(y_away, away_pred)
    candidate["home_mae"] = float(mean_absolute_error(y_home, home_pred))
    candidate["away_mae"] = float(mean_absolute_error(y_away, away_pred))

    baseline = pair_probability_metrics(base_home, base_away, y_home, y_away)
    baseline["home_poisson"] = safe_poisson(y_home, base_home)
    baseline["away_poisson"] = safe_poisson(y_away, base_away)
    baseline["home_mae"] = float(mean_absolute_error(y_home, base_home))
    baseline["away_mae"] = float(mean_absolute_error(y_away, base_away))

    ratio_keys = [
        "home_poisson",
        "away_poisson",
        "moneyline_log_loss",
        "runline_minus_1_5_log_loss",
        "runline_plus_1_5_log_loss",
        "totals_halfline_log_loss",
        "margin_nll",
        "total_nll",
    ]
    ratios = {k: candidate[k] / baseline[k] for k in ratio_keys}
    vals = np.array(list(ratios.values()), dtype=float)
    ratios["composite_ratio"] = float(np.exp(np.log(vals).mean()))
    ratios["worst_component_ratio"] = float(vals.max())

    return candidate, baseline, ratios, home_pred, away_pred


def versions() -> dict:
    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "scipy": scipy.__version__,
        "scikit_learn": sklearn.__version__,
        "lightgbm": getattr(lightgbm, "__version__", None),
        "xgboost": getattr(xgboost, "__version__", None),
        "lightgbm_import_error": LIGHTGBM_IMPORT_ERROR,
        "xgboost_import_error": XGBOOST_IMPORT_ERROR,
    }


def save_winner(
    output_dir: Path,
    home_model,
    away_model,
    home_row: pd.Series,
    away_row: pd.Series,
    home_features: list[str],
    away_features: list[str],
    home_params: dict,
    away_params: dict,
    original_train: pd.DataFrame,
    original_validation: pd.DataFrame,
    test: pd.DataFrame,
    folds: list[Fold],
    candidate_test: dict,
    baseline_test: dict,
    ratios: dict,
    home_test_pred: np.ndarray,
    away_test_pred: np.ndarray,
) -> None:
    winner = output_dir / "winner"
    winner.mkdir(parents=True, exist_ok=True)

    joblib.dump(home_model, winner / "home_runs_model.joblib")
    joblib.dump(away_model, winner / "away_runs_model.joblib")

    tr_start, tr_end = date_range(original_train)
    va_start, va_end = date_range(original_validation)
    te_start, te_end = date_range(test)

    shared = {
        "artifact_stage": "exhaustive_search_winner_unpromoted",
        "production_modified": False,
        "selection_method": "development_only_expanding_walk_forward_cv_and_coupled_pair_search",
        "training_start_date": tr_start,
        "training_end_date": tr_end,
        "validation_start_date": va_start,
        "validation_end_date": va_end,
        "test_start_date": te_start,
        "test_end_date": te_end,
        "package_versions": versions(),
        "created_at": now_iso(),
        "cv_folds": [
            {
                "fold_id": f.fold_id,
                "train_start": f.train_start,
                "train_end": f.train_end,
                "validation_start": f.validation_start,
                "validation_end": f.validation_end,
            }
            for f in folds
        ],
        "untouched_test_candidate_metrics": candidate_test,
        "untouched_test_dratings_metrics": baseline_test,
        "untouched_test_ratios": ratios,
        "integration_warning": (
            "Research artifact only. build_run_projection.py currently contains "
            "HistGradientBoosting-specific walk-forward fitting logic. If this winner "
            "uses another model family or different training behavior, production "
            "projection code must be updated and validated before promotion."
        ),
    }

    home_meta = {
        **shared,
        "side": "home",
        "candidate_id": str(home_row.name),
        "family": str(home_row["family"]),
        "feature_set": str(home_row["feature_set"]),
        "feature_columns": home_features,
        "params": home_params,
        "target_column": "target_home_runs",
    }
    away_meta = {
        **shared,
        "side": "away",
        "candidate_id": str(away_row.name),
        "family": str(away_row["family"]),
        "feature_set": str(away_row["feature_set"]),
        "feature_columns": away_features,
        "params": away_params,
        "target_column": "target_away_runs",
    }

    write_json(winner / "home_runs_model_metadata.json", home_meta)
    write_json(winner / "away_runs_model_metadata.json", away_meta)

    pred = test[[
        "game_date", "game_id", "gamePk", "home_team", "away_team",
        "target_home_runs", "target_away_runs",
        "dratings_home_projected_runs", "dratings_away_projected_runs",
    ]].copy()
    pred["winner_home_projected_runs"] = home_test_pred
    pred["winner_away_projected_runs"] = away_test_pred
    pred.to_csv(winner / "untouched_test_predictions.csv", index=False)


def write_report(
    output_dir: Path,
    home_results: pd.DataFrame,
    away_results: pd.DataFrame,
    home_short: pd.DataFrame,
    away_short: pd.DataFrame,
    pairs: pd.DataFrame,
    home_winner: pd.Series,
    away_winner: pd.Series,
    candidate_test: dict,
    baseline_test: dict,
    ratios: dict,
    feature_sets: dict[str, list[str]],
    grids: dict[str, list[dict]],
    folds: list[Fold],
) -> None:
    winner_pair = pairs.iloc[0]
    lines = [
        "# MLB Exhaustive Coupled Run-Model Search",
        "",
        f"- Generated: `{now_iso()}`",
        "- Production artifacts modified: `NO`",
        "- Final untouched test used for model selection: `NO`",
        f"- Feature sets: `{len(feature_sets)}`",
        f"- Model families: `{len(grids)}`",
        f"- Successful home candidates: `{int((home_results['status'] == 'ok').sum())}`",
        f"- Successful away candidates: `{int((away_results['status'] == 'ok').sum())}`",
        f"- Home shortlist: `{len(home_short)}`",
        f"- Away shortlist: `{len(away_short)}`",
        f"- Coupled pairs evaluated: `{len(pairs)}`",
        "",
        "## Model-family grid sizes per feature set",
        "",
    ]
    for family, params in grids.items():
        lines.append(f"- `{family}`: `{len(params)}` configurations")

    lines += ["", "## Walk-forward CV folds", ""]
    for fold in folds:
        lines.append(
            f"- Fold {fold.fold_id}: train `{fold.train_start}` through `{fold.train_end}`; "
            f"validate `{fold.validation_start}` through `{fold.validation_end}`"
        )

    lines += [
        "",
        "## Validation winner",
        "",
        f"- Home: `{home_winner['family']}` / `{home_winner['feature_set']}` / `{home_winner.name}`",
        f"- Away: `{away_winner['family']}` / `{away_winner['feature_set']}` / `{away_winner.name}`",
        f"- Composite ratio vs DRatings: `{float(winner_pair['composite_ratio']):.8f}`",
        f"- Worst component ratio vs DRatings: `{float(winner_pair['worst_component_ratio']):.8f}`",
        "",
        "## Untouched final test",
        "",
        f"- Composite ratio vs DRatings: `{ratios['composite_ratio']:.8f}`",
        f"- Worst component ratio vs DRatings: `{ratios['worst_component_ratio']:.8f}`",
        "",
        "| Metric | Winner | DRatings | Ratio |",
        "| --- | ---: | ---: | ---: |",
    ]

    keys = [
        "home_poisson",
        "away_poisson",
        "moneyline_log_loss",
        "runline_minus_1_5_log_loss",
        "runline_plus_1_5_log_loss",
        "totals_halfline_log_loss",
        "margin_nll",
        "total_nll",
    ]
    for key in keys:
        lines.append(
            f"| {key} | {candidate_test[key]:.8f} | "
            f"{baseline_test[key]:.8f} | {ratios[key]:.8f} |"
        )

    lines += [
        "",
        "## Interpretation",
        "",
        "- Ratio `< 1.0`: winner beat DRatings on that metric.",
        "- Ratio `> 1.0`: DRatings was better on that metric.",
        "- The composite is the geometric mean of the eight ratios.",
        "- Winner files remain research artifacts until production integration is explicitly approved.",
        "",
    ]

    (output_dir / "exhaustive_search_report.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cv-folds", type=int, default=4)
    parser.add_argument("--initial-train-fraction", type=float, default=0.50)
    parser.add_argument("--pair-top-k", type=int, default=300)
    parser.add_argument("--pair-diversity-per-group", type=int, default=4)
    parser.add_argument("--pair-batch-size", type=int, default=128)
    parser.add_argument("--checkpoint-every", type=int, default=20)
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(6, (os.cpu_count() or 2) - 1)),
    )
    parser.add_argument("--smoke-test", action="store_true")
    parser.add_argument("--show-plan", action="store_true")
    parser.add_argument("--fresh", action="store_true")
    parser.add_argument("--max-candidates-per-side", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.cv_folds < 2:
        raise ValueError("--cv-folds must be >= 2")
    if not (0.30 <= args.initial_train_fraction <= 0.80):
        raise ValueError("--initial-train-fraction must be between 0.30 and 0.80")

    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = (ROOT / output_dir).resolve()

    input_path = args.input
    if not input_path.is_absolute():
        input_path = (ROOT / input_path).resolve()

    if args.fresh and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log(f"START staged run-model search | root={ROOT}", output_dir)
    log(f"versions={stable_json(versions())}", output_dir)

    training = TRAIN.load_training_set(input_path)
    all_features = TRAIN.determine_feature_columns(training)
    training = TRAIN.coerce_and_validate_training_data(training, all_features)

    split = TRAIN.chronological_date_split(training)
    original_train = split["train"].copy()
    original_validation = split["validation"].copy()
    test = split["test"].copy()

    development = pd.concat(
        [original_train, original_validation],
        ignore_index=True,
        sort=False,
    ).sort_values(
        ["_game_date_dt", "game_id"],
        kind="stable",
    ).reset_index(drop=True)

    feature_sets = build_feature_sets(all_features)
    coarse_grids = family_grids(args.smoke_test)
    folds = build_walk_forward_folds(
        development,
        args.cv_folds,
        args.initial_train_fraction,
    )

    coarse_per_feature = sum(len(v) for v in coarse_grids.values())
    coarse_per_side = coarse_per_feature * len(feature_sets)
    coarse_fits_both_sides = coarse_per_side * len(folds) * 2

    maximum_seed_count = (
        10
        + len(coarse_grids)
        + len(feature_sets)
    )

    plan = {
        "search_strategy": "two_stage_coarse_then_local_refinement",
        "versions": versions(),
        "feature_sets": {k: len(v) for k, v in feature_sets.items()},
        "coarse_family_grid_sizes": {
            k: len(v) for k, v in coarse_grids.items()
        },
        "coarse_candidate_configs_per_side": coarse_per_side,
        "cv_folds": len(folds),
        "coarse_model_fits_both_sides": coarse_fits_both_sides,
        "maximum_refinement_seed_count_per_side": maximum_seed_count,
        "refinement_rule": (
            "top 10 overall + best per family + best per feature set; "
            "local one-factor perturbations for every seed plus small Cartesian "
            "interaction search for the top 5 seeds"
        ),
        "pair_top_k": args.pair_top_k,
        "untouched_test_rows": int(len(test)),
        "untouched_test_used_for_selection": False,
    }
    write_json(output_dir / "search_plan.json", plan)

    print()
    print("STAGED SEARCH PLAN")
    print(json.dumps(plan, indent=2))
    print()

    if args.show_plan:
        return

    # --------------------------------------------------------------
    # Stage 1: broad search across every family and feature-set group.
    # --------------------------------------------------------------
    home_coarse = search_side(
        "home",
        development,
        folds,
        feature_sets,
        coarse_grids,
        output_dir,
        args.workers,
        args.checkpoint_every,
        args.max_candidates_per_side,
        result_label="coarse",
    )
    away_coarse = search_side(
        "away",
        development,
        folds,
        feature_sets,
        coarse_grids,
        output_dir,
        args.workers,
        args.checkpoint_every,
        args.max_candidates_per_side,
        result_label="coarse",
    )

    # --------------------------------------------------------------
    # Stage 2: refine only around data-driven winners.
    # Smoke mode intentionally skips this so validation remains fast.
    # --------------------------------------------------------------
    if args.smoke_test:
        home_seeds = pd.DataFrame()
        away_seeds = pd.DataFrame()
        home_refined = pd.DataFrame(columns=RESULT_COLUMNS)
        away_refined = pd.DataFrame(columns=RESULT_COLUMNS)
        home_refinement_configs: list[dict] = []
        away_refinement_configs: list[dict] = []
    else:
        home_seeds = select_refinement_seeds(home_coarse, top_overall=10)
        away_seeds = select_refinement_seeds(away_coarse, top_overall=10)

        home_seeds.to_csv(
            output_dir / "home_refinement_seeds.csv",
            index=False,
        )
        away_seeds.to_csv(
            output_dir / "away_refinement_seeds.csv",
            index=False,
        )

        home_refinement_configs = build_refinement_configs(
            "home",
            home_seeds,
            intensive_seed_count=5,
        )
        away_refinement_configs = build_refinement_configs(
            "away",
            away_seeds,
            intensive_seed_count=5,
        )

        write_json(
            output_dir / "refinement_plan.json",
            {
                "home_seed_count": len(home_seeds),
                "away_seed_count": len(away_seeds),
                "home_refinement_configs": len(home_refinement_configs),
                "away_refinement_configs": len(away_refinement_configs),
                "home_refinement_cv_fits": len(home_refinement_configs) * len(folds),
                "away_refinement_cv_fits": len(away_refinement_configs) * len(folds),
            },
        )

        log(
            f"REFINEMENT PLAN | home_seeds={len(home_seeds)} "
            f"home_configs={len(home_refinement_configs)} "
            f"away_seeds={len(away_seeds)} "
            f"away_configs={len(away_refinement_configs)}",
            output_dir,
        )

        home_refined = search_explicit_configs(
            "home",
            home_refinement_configs,
            development,
            folds,
            feature_sets,
            output_dir,
            args.workers,
            args.checkpoint_every,
            "refined",
            args.max_candidates_per_side,
        )
        away_refined = search_explicit_configs(
            "away",
            away_refinement_configs,
            development,
            folds,
            feature_sets,
            output_dir,
            args.workers,
            args.checkpoint_every,
            "refined",
            args.max_candidates_per_side,
        )

    home_results = combine_candidate_results(
        home_coarse,
        home_refined,
    )
    away_results = combine_candidate_results(
        away_coarse,
        away_refined,
    )

    home_results.to_csv(
        output_dir / "home_candidate_results.csv",
        index=False,
    )
    away_results.to_csv(
        output_dir / "away_candidate_results.csv",
        index=False,
    )

    log(
        f"CANDIDATE TOTALS | home={len(home_results)} "
        f"away={len(away_results)}",
        output_dir,
    )

    # --------------------------------------------------------------
    # Coupled pair phase: shortlist after BOTH stages, then score every
    # home/away combination in the shortlist on development OOF rows.
    # --------------------------------------------------------------
    home_short = shortlist(
        home_results,
        args.pair_top_k,
        args.pair_diversity_per_group,
    )
    away_short = shortlist(
        away_results,
        args.pair_top_k,
        args.pair_diversity_per_group,
    )

    home_short.to_csv(output_dir / "home_pair_shortlist.csv", index=False)
    away_short.to_csv(output_dir / "away_pair_shortlist.csv", index=False)

    baseline, actual_home, actual_away = validation_baseline(
        development,
        folds,
    )
    write_json(output_dir / "validation_dratings_baseline.json", baseline)

    home_matrix, home_actual_check = shortlist_oof_matrix(
        "home",
        home_short,
        development,
        folds,
        feature_sets,
        args.workers,
        output_dir,
    )
    away_matrix, away_actual_check = shortlist_oof_matrix(
        "away",
        away_short,
        development,
        folds,
        feature_sets,
        args.workers,
        output_dir,
    )

    if not np.array_equal(home_actual_check, actual_home):
        raise RuntimeError("Home OOF ordering mismatch")
    if not np.array_equal(away_actual_check, actual_away):
        raise RuntimeError("Away OOF ordering mismatch")

    pairs = search_pairs(
        home_short,
        away_short,
        home_matrix,
        away_matrix,
        actual_home,
        actual_away,
        baseline,
        output_dir,
        args.pair_batch_size,
    )

    if pairs.empty:
        raise RuntimeError("No pairs evaluated")

    winner_pair = pairs.iloc[0]
    home_row = home_short.set_index("candidate_id").loc[
        str(winner_pair["home_candidate_id"])
    ]
    away_row = away_short.set_index("candidate_id").loc[
        str(winner_pair["away_candidate_id"])
    ]

    log(
        f"VALIDATION WINNER | home={winner_pair['home_candidate_id']} "
        f"away={winner_pair['away_candidate_id']} "
        f"composite={float(winner_pair['composite_ratio']):.8f}",
        output_dir,
    )

    home_model, home_features, home_params = fit_development_winner(
        home_row,
        development,
        feature_sets,
        "home",
        args.workers,
    )
    away_model, away_features, away_params = fit_development_winner(
        away_row,
        development,
        feature_sets,
        "away",
        args.workers,
    )

    # Final 15% chronological period is touched only here.
    candidate_test, baseline_test, ratios, home_test_pred, away_test_pred = evaluate_test(
        home_model,
        away_model,
        home_features,
        away_features,
        test,
    )

    write_json(
        output_dir / "untouched_test_comparison.json",
        {
            "winner": candidate_test,
            "dratings": baseline_test,
            "ratios": ratios,
            "test_used_for_selection": False,
        },
    )

    save_winner(
        output_dir,
        home_model,
        away_model,
        home_row,
        away_row,
        home_features,
        away_features,
        home_params,
        away_params,
        original_train,
        original_validation,
        test,
        folds,
        candidate_test,
        baseline_test,
        ratios,
        home_test_pred,
        away_test_pred,
    )

    write_report(
        output_dir,
        home_results,
        away_results,
        home_short,
        away_short,
        pairs,
        home_row,
        away_row,
        candidate_test,
        baseline_test,
        ratios,
        feature_sets,
        coarse_grids,
        folds,
    )

    write_json(
        output_dir / "staged_search_summary.json",
        {
            "home_coarse_candidates": len(home_coarse),
            "away_coarse_candidates": len(away_coarse),
            "home_refinement_seeds": len(home_seeds),
            "away_refinement_seeds": len(away_seeds),
            "home_refinement_configs": len(home_refinement_configs),
            "away_refinement_configs": len(away_refinement_configs),
            "home_total_unique_candidates": len(home_results),
            "away_total_unique_candidates": len(away_results),
            "home_pair_shortlist": len(home_short),
            "away_pair_shortlist": len(away_short),
            "pairs_evaluated": len(pairs),
            "validation_composite_ratio": float(winner_pair["composite_ratio"]),
            "untouched_test_composite_ratio": ratios["composite_ratio"],
        },
    )

    log(
        f"COMPLETE | validation_composite={float(winner_pair['composite_ratio']):.8f} "
        f"test_composite={ratios['composite_ratio']:.8f}",
        output_dir,
    )

    print()
    print("STAGED SEARCH COMPLETE")
    print(f"Unique home candidates:      {len(home_results)}")
    print(f"Unique away candidates:      {len(away_results)}")
    print(f"Coupled pairs evaluated:     {len(pairs)}")
    print(f"Validation composite ratio:  {float(winner_pair['composite_ratio']):.8f}")
    print(f"Untouched test ratio:        {ratios['composite_ratio']:.8f}")
    print(f"Report: {output_dir / 'exhaustive_search_report.md'}")
    print(f"Winner: {output_dir / 'winner'}")
    print("Production models were NOT modified.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(
            "\nInterrupted. Candidate and pair checkpoints are preserved. "
            "Rerun the same command to resume."
        )
        raise
    except Exception:
        traceback.print_exc()
        raise
