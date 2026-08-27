#!/usr/bin/env python3
"""
Repeated chronological holdout test for MLB totals.

Compares:

1. baseline
   Current direct Over/Under classifier.

2. park_recent
   Current direct Over/Under classifier plus:
   - leakage-safe historical park scoring features
   - recent-game training weights

Five expanding chronological tests are used.

Each fold:
    earlier dates -> training
    next dates    -> validation
    following     -> untouched test

The five untouched test blocks are non-overlapping and cover the final
50% of dates with usable sportsbook totals.

Static repository park-factor files are NOT used because the current files
are labeled 2024-2026 and therefore are not safe for historical 2026 testing.

Sportsbook total line is a model feature.
Sportsbook odds/prices/juice are never model features.

Outputs:
    mlb_total_repeated_holdout_summary.csv
    mlb_total_repeated_holdout_folds.csv
    mlb_total_repeated_holdout_buckets.csv
    mlb_total_repeated_holdout_predictions.csv
    mlb_total_repeated_holdout_report.html
"""

from __future__ import annotations

import importlib.util
import json
import warnings
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    roc_auc_score,
)


warnings.filterwarnings(
    "ignore",
    message=r".*sklearn\.utils\.parallel\.delayed.*",
    category=UserWarning,
)


EPS = 1e-12

PARK_SHRINK_GAMES = 10.0
MIN_PRIOR_LEAGUE_GAMES = 30

RECENCY_HALF_LIVES = (
    14,
    30,
    60,
    90,
)

FOLD_TRAIN_ENDS = (
    0.40,
    0.50,
    0.60,
    0.70,
    0.80,
)

FOLD_WIDTH = 0.10


def repo_root() -> Path:
    for start in (
        Path.cwd().resolve(),
        Path(__file__).resolve().parent,
    ):
        for candidate in (
            start,
            *start.parents,
        ):
            if (
                candidate
                / "docs/win/baseball/mlb"
            ).exists():
                return candidate

    raise RuntimeError(
        "Could not locate repository root"
    )


ROOT = repo_root()

BASE = (
    ROOT
    / "docs/win/baseball/mlb"
)

CLASSIFIER_SCRIPT = (
    BASE
    / "scripts/modeling/test_total_classifier.py"
)

SUMMARY_FILE = (
    ROOT
    / "mlb_total_repeated_holdout_summary.csv"
)

FOLD_FILE = (
    ROOT
    / "mlb_total_repeated_holdout_folds.csv"
)

BUCKET_FILE = (
    ROOT
    / "mlb_total_repeated_holdout_buckets.csv"
)

PREDICTIONS_FILE = (
    ROOT
    / "mlb_total_repeated_holdout_predictions.csv"
)

REPORT_FILE = (
    ROOT
    / "mlb_total_repeated_holdout_report.html"
)


def load_module(
    name: str,
    path: Path,
):
    if not path.exists():
        raise FileNotFoundError(
            path
        )

    spec = (
        importlib.util
        .spec_from_file_location(
            name,
            path,
        )
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            f"Could not load module: {path}"
        )

    module = (
        importlib.util
        .module_from_spec(
            spec
        )
    )

    spec.loader.exec_module(
        module
    )

    return module


CLASSIFIER = load_module(
    "total_classifier_existing",
    CLASSIFIER_SCRIPT,
)

TM = CLASSIFIER.TM
TRAIN = CLASSIFIER.TRAIN

TRAIN._log = (
    lambda *args, **kwargs: None
)


PARK_FEATURE_COLUMNS = [
    "park_run_factor_asof",
    "park_prior_avg_total",
    "park_prior_games_log",
]


def numeric(
    frame: pd.DataFrame,
    column: str,
) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(
            np.nan,
            index=frame.index,
            dtype=float,
        )

    return pd.to_numeric(
        frame[column],
        errors="coerce",
    )


def safe_corr(
    x,
    y,
) -> float:
    x = np.asarray(
        x,
        dtype=float,
    )

    y = np.asarray(
        y,
        dtype=float,
    )

    mask = (
        np.isfinite(x)
        & np.isfinite(y)
    )

    if mask.sum() < 2:
        return np.nan

    x = x[mask]
    y = y[mask]

    if (
        np.std(x) <= EPS
        or np.std(y) <= EPS
    ):
        return np.nan

    return float(
        np.corrcoef(
            x,
            y,
        )[0, 1]
    )


def build_leakage_safe_park_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    required = [
        "_game_date_dt",
        "game_id",
        "venue_id",
        "target_home_runs",
        "target_away_runs",
    ]

    missing = [
        col
        for col in required
        if col not in frame.columns
    ]

    if missing:
        raise RuntimeError(
            "Cannot build park features; "
            f"missing columns: {missing}"
        )

    out = frame.copy()

    out[
        "_actual_total_for_park"
    ] = (
        numeric(
            out,
            "target_home_runs",
        )
        + numeric(
            out,
            "target_away_runs",
        )
    )

    out[
        "_venue_for_park"
    ] = pd.to_numeric(
        out["venue_id"],
        errors="coerce",
    )

    out[
        "park_prior_games"
    ] = np.nan

    out[
        "park_prior_avg_total"
    ] = np.nan

    out[
        "league_prior_avg_total"
    ] = np.nan

    out[
        "park_run_factor_asof"
    ] = np.nan

    out[
        "park_prior_games_log"
    ] = np.nan

    out = (
        out
        .sort_values(
            [
                "_game_date_dt",
                "game_id",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    venue_sum: dict[int, float] = {}
    venue_count: dict[int, int] = {}

    league_sum = 0.0
    league_count = 0

    for (
        game_date,
        group,
    ) in out.groupby(
        "_game_date_dt",
        sort=True,
    ):
        indices = (
            group.index.tolist()
        )

        league_prior_avg = (
            league_sum
            / league_count
            if league_count > 0
            else np.nan
        )

        # Calculate all games on this date before adding
        # any results from this same date.
        for idx in indices:
            venue_value = (
                out.at[
                    idx,
                    "_venue_for_park",
                ]
            )

            if not np.isfinite(
                venue_value
            ):
                continue

            venue_id = int(
                round(
                    float(
                        venue_value
                    )
                )
            )

            prior_count = (
                venue_count.get(
                    venue_id,
                    0,
                )
            )

            prior_sum = (
                venue_sum.get(
                    venue_id,
                    0.0,
                )
            )

            out.at[
                idx,
                "park_prior_games",
            ] = float(
                prior_count
            )

            out.at[
                idx,
                "park_prior_games_log",
            ] = float(
                np.log1p(
                    prior_count
                )
            )

            if prior_count > 0:
                out.at[
                    idx,
                    "park_prior_avg_total",
                ] = (
                    prior_sum
                    / prior_count
                )

            if (
                league_count
                < MIN_PRIOR_LEAGUE_GAMES
                or not np.isfinite(
                    league_prior_avg
                )
                or league_prior_avg <= 0
            ):
                continue

            out.at[
                idx,
                "league_prior_avg_total",
            ] = (
                league_prior_avg
            )

            shrunk_venue_avg = (
                prior_sum
                + (
                    league_prior_avg
                    * PARK_SHRINK_GAMES
                )
            ) / (
                prior_count
                + PARK_SHRINK_GAMES
            )

            park_factor = (
                100.0
                * shrunk_venue_avg
                / league_prior_avg
            )

            if not np.isfinite(
                park_factor
            ):
                raise RuntimeError(
                    "Non-finite park factor: "
                    f"game_id={out.at[idx, 'game_id']}"
                )

            out.at[
                idx,
                "park_run_factor_asof",
            ] = float(
                park_factor
            )

        # Add this date to history only after every feature
        # for this date has been calculated.
        for idx in indices:
            actual_total = (
                out.at[
                    idx,
                    "_actual_total_for_park",
                ]
            )

            if (
                not np.isfinite(
                    actual_total
                )
                or actual_total < 0
            ):
                raise RuntimeError(
                    "Invalid historical actual total: "
                    f"game_id={out.at[idx, 'game_id']}"
                )

            league_sum += float(
                actual_total
            )

            league_count += 1

            venue_value = (
                out.at[
                    idx,
                    "_venue_for_park",
                ]
            )

            if not np.isfinite(
                venue_value
            ):
                continue

            venue_id = int(
                round(
                    float(
                        venue_value
                    )
                )
            )

            venue_sum[
                venue_id
            ] = (
                venue_sum.get(
                    venue_id,
                    0.0,
                )
                + float(
                    actual_total
                )
            )

            venue_count[
                venue_id
            ] = (
                venue_count.get(
                    venue_id,
                    0,
                )
                + 1
            )

    return out.drop(
        columns=[
            "_actual_total_for_park",
            "_venue_for_park",
        ]
    )


def attach_resolved_totals(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    out = (
        CLASSIFIER
        .attach_lines(
            frame
        )
    )

    out = out[
        ~out[
            "_is_push"
        ]
    ].copy()

    if out.empty:
        raise RuntimeError(
            "No resolved sportsbook totals"
        )

    out = (
        out
        .sort_values(
            [
                "_game_date_dt",
                "game_id",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    if (
        out["game_id"]
        .astype(str)
        .duplicated()
        .any()
    ):
        raise RuntimeError(
            "Resolved totals contain duplicate game_id"
        )

    return out


def make_folds(
    frame: pd.DataFrame,
) -> list[dict]:
    dates = np.array(
        sorted(
            frame[
                "_game_date_dt"
            ]
            .drop_duplicates()
            .tolist()
        )
    )

    n_dates = len(
        dates
    )

    if n_dates < 20:
        raise RuntimeError(
            "Not enough dates for repeated holdout test: "
            f"{n_dates}"
        )

    folds = []
    previous_test_dates: set = set()

    for (
        fold_number,
        train_end_fraction,
    ) in enumerate(
        FOLD_TRAIN_ENDS,
        start=1,
    ):
        train_end = int(
            np.floor(
                n_dates
                * train_end_fraction
            )
        )

        validation_end = int(
            np.floor(
                n_dates
                * (
                    train_end_fraction
                    + FOLD_WIDTH
                )
            )
        )

        if fold_number == len(
            FOLD_TRAIN_ENDS
        ):
            test_end = (
                n_dates
            )
        else:
            test_end = int(
                np.floor(
                    n_dates
                    * (
                        train_end_fraction
                        + (2 * FOLD_WIDTH)
                    )
                )
            )

        if (
            train_end <= 0
            or validation_end <= train_end
            or test_end <= validation_end
        ):
            raise RuntimeError(
                f"Invalid fold boundaries for fold {fold_number}"
            )

        train_dates = set(
            dates[
                :train_end
            ]
        )

        validation_dates = set(
            dates[
                train_end:
                validation_end
            ]
        )

        test_dates = set(
            dates[
                validation_end:
                test_end
            ]
        )

        if (
            train_dates
            & validation_dates
            or train_dates
            & test_dates
            or validation_dates
            & test_dates
        ):
            raise RuntimeError(
                f"Date leakage in fold {fold_number}"
            )

        if (
            previous_test_dates
            & test_dates
        ):
            raise RuntimeError(
                "Untouched test blocks overlap"
            )

        previous_test_dates.update(
            test_dates
        )

        train = frame[
            frame[
                "_game_date_dt"
            ].isin(
                train_dates
            )
        ].copy()

        validation = frame[
            frame[
                "_game_date_dt"
            ].isin(
                validation_dates
            )
        ].copy()

        test = frame[
            frame[
                "_game_date_dt"
            ].isin(
                test_dates
            )
        ].copy()

        if (
            train.empty
            or validation.empty
            or test.empty
        ):
            raise RuntimeError(
                f"Fold {fold_number} contains an empty partition"
            )

        folds.append(
            {
                "fold":
                    fold_number,
                "train":
                    train,
                "validation":
                    validation,
                "test":
                    test,
            }
        )

    return folds


def add_park_features(
    x: pd.DataFrame,
    source: pd.DataFrame,
) -> pd.DataFrame:
    out = (
        x
        .reset_index(
            drop=True
        )
        .copy()
    )

    source = (
        source
        .reset_index(
            drop=True
        )
    )

    for col in (
        PARK_FEATURE_COLUMNS
    ):
        if col not in source.columns:
            raise RuntimeError(
                f"Missing park feature: {col}"
            )

        out[col] = pd.to_numeric(
            source[col],
            errors="coerce",
        )

        bad = (
            out[col].notna()
            & ~np.isfinite(
                out[col]
            )
        )

        if bad.any():
            raise RuntimeError(
                f"Non-finite park values in {col}"
            )

    return out


def make_model(
    params: dict,
) -> HistGradientBoostingClassifier:
    return (
        HistGradientBoostingClassifier(
            loss="log_loss",
            learning_rate=params[
                "learning_rate"
            ],
            max_leaf_nodes=params[
                "max_leaf_nodes"
            ],
            min_samples_leaf=params[
                "min_samples_leaf"
            ],
            l2_regularization=params[
                "l2_regularization"
            ],
            random_state=(
                TRAIN.RANDOM_STATE
            ),
        )
    )


def predict_over_probability(
    model: HistGradientBoostingClassifier,
    x: pd.DataFrame,
) -> np.ndarray:
    if 1 not in model.classes_:
        raise RuntimeError(
            "Classifier contains no Over class"
        )

    over_index = int(
        np.where(
            model.classes_
            == 1
        )[0][0]
    )

    probability = (
        model
        .predict_proba(
            x
        )[
            :,
            over_index
        ]
    )

    probability = np.asarray(
        probability,
        dtype=float,
    )

    if np.any(
        ~np.isfinite(
            probability
        )
    ):
        raise RuntimeError(
            "Classifier returned non-finite probability"
        )

    return np.clip(
        probability,
        EPS,
        1.0 - EPS,
    )


def tune_classifier(
    train_x: pd.DataFrame,
    train_y: pd.Series,
    validation_x: pd.DataFrame,
    validation_y: pd.Series,
) -> tuple[
    dict,
    float,
    float,
]:
    best_params = None
    best_log_loss = np.inf
    best_brier = np.inf

    candidate_count = 0

    for params in (
        TRAIN
        .hyperparameter_candidates()
    ):
        candidate_count += 1

        model = make_model(
            params
        )

        model.fit(
            train_x,
            train_y,
        )

        probability = (
            predict_over_probability(
                model,
                validation_x,
            )
        )

        ll = float(
            log_loss(
                validation_y,
                probability,
                labels=[
                    0,
                    1,
                ],
            )
        )

        brier = float(
            brier_score_loss(
                validation_y,
                probability,
            )
        )

        if (
            ll
            < best_log_loss
            - EPS
            or (
                abs(
                    ll
                    - best_log_loss
                )
                <= EPS
                and brier
                < best_brier
            )
        ):
            best_params = dict(
                params
            )

            best_log_loss = ll
            best_brier = brier

    if candidate_count != 81:
        raise RuntimeError(
            "Expected 81 parameter combinations; "
            f"evaluated {candidate_count}"
        )

    if best_params is None:
        raise RuntimeError(
            "No classifier parameters selected"
        )

    return (
        best_params,
        best_log_loss,
        best_brier,
    )


def recency_weights(
    frame: pd.DataFrame,
    half_life_days: int,
) -> np.ndarray:
    dates = pd.to_datetime(
        frame[
            "_game_date_dt"
        ],
        errors="coerce",
    )

    if dates.isna().any():
        raise RuntimeError(
            "Invalid dates for recency weighting"
        )

    newest = (
        dates.max()
    )

    age_days = (
        (
            newest
            - dates
        )
        .dt.total_seconds()
        / 86400.0
    )

    weights = np.power(
        0.5,
        (
            age_days.to_numpy(
                dtype=float
            )
            / float(
                half_life_days
            )
        ),
    )

    if (
        np.any(
            ~np.isfinite(
                weights
            )
        )
        or np.any(
            weights <= 0
        )
    ):
        raise RuntimeError(
            "Invalid recency weights"
        )

    return (
        weights
        / np.mean(
            weights
        )
    )


def choose_recency_half_life(
    train: pd.DataFrame,
    train_x: pd.DataFrame,
    train_y: pd.Series,
    validation_x: pd.DataFrame,
    validation_y: pd.Series,
    params: dict,
) -> tuple[
    int,
    float,
    float,
]:
    best_half_life = None
    best_log_loss = np.inf
    best_brier = np.inf

    for half_life in (
        RECENCY_HALF_LIVES
    ):
        model = make_model(
            params
        )

        model.fit(
            train_x,
            train_y,
            sample_weight=(
                recency_weights(
                    train,
                    half_life,
                )
            ),
        )

        probability = (
            predict_over_probability(
                model,
                validation_x,
            )
        )

        ll = float(
            log_loss(
                validation_y,
                probability,
                labels=[
                    0,
                    1,
                ],
            )
        )

        brier = float(
            brier_score_loss(
                validation_y,
                probability,
            )
        )

        if (
            ll
            < best_log_loss
            - EPS
            or (
                abs(
                    ll
                    - best_log_loss
                )
                <= EPS
                and brier
                < best_brier
            )
        ):
            best_half_life = int(
                half_life
            )

            best_log_loss = ll
            best_brier = brier

    if best_half_life is None:
        raise RuntimeError(
            "No recency half-life selected"
        )

    return (
        best_half_life,
        best_log_loss,
        best_brier,
    )


def fit_final(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    train_x: pd.DataFrame,
    validation_x: pd.DataFrame,
    train_y: pd.Series,
    validation_y: pd.Series,
    params: dict,
    half_life_days: int | None,
) -> HistGradientBoostingClassifier:
    fit_frame = pd.concat(
        [
            train,
            validation,
        ],
        ignore_index=True,
        sort=False,
    )

    fit_x = pd.concat(
        [
            train_x,
            validation_x,
        ],
        ignore_index=True,
        sort=False,
    )

    fit_y = pd.concat(
        [
            train_y,
            validation_y,
        ],
        ignore_index=True,
    )

    sample_weight = None

    if half_life_days is not None:
        sample_weight = (
            recency_weights(
                fit_frame,
                half_life_days,
            )
        )

    model = make_model(
        params
    )

    model.fit(
        fit_x,
        fit_y,
        sample_weight=sample_weight,
    )

    return model


def candidate_predictions(
    fold_number: int,
    variant: str,
    test: pd.DataFrame,
    p_over: np.ndarray,
) -> pd.DataFrame:
    test = (
        test
        .reset_index(
            drop=True
        )
    )

    if len(test) != len(
        p_over
    ):
        raise RuntimeError(
            "Prediction length mismatch"
        )

    rows = []

    for (
        idx,
        row,
    ) in test.iterrows():
        over_probability = float(
            p_over[idx]
        )

        under_probability = (
            1.0
            - over_probability
        )

        actual_over = int(
            row[
                "_over_target"
            ]
        )

        game_date = (
            pd.Timestamp(
                row[
                    "_game_date_dt"
                ]
            )
            .strftime(
                "%Y-%m-%d"
            )
        )

        common = {
            "fold":
                fold_number,
            "variant":
                variant,
            "game_date":
                game_date,
            "game_id":
                str(
                    row[
                        "game_id"
                    ]
                ),
            "home_team":
                row.get(
                    "home_team_train",
                    row.get(
                        "home_team"
                    ),
                ),
            "away_team":
                row.get(
                    "away_team_train",
                    row.get(
                        "away_team"
                    ),
                ),
            "total_line":
                float(
                    row[
                        "_total_line"
                    ]
                ),
            "actual_total":
                float(
                    row[
                        "_actual_total"
                    ]
                ),
            "actual_over":
                actual_over,
            "over_probability":
                over_probability,
            "park_run_factor_asof":
                row.get(
                    "park_run_factor_asof",
                    np.nan,
                ),
            "park_prior_avg_total":
                row.get(
                    "park_prior_avg_total",
                    np.nan,
                ),
            "park_prior_games":
                row.get(
                    "park_prior_games",
                    np.nan,
                ),
        }

        rows.append(
            {
                **common,
                "side":
                    "over",
                "model_prob":
                    over_probability,
                "win_binary":
                    float(
                        actual_over
                    ),
            }
        )

        rows.append(
            {
                **common,
                "side":
                    "under",
                "model_prob":
                    under_probability,
                "win_binary":
                    float(
                        1
                        - actual_over
                    ),
            }
        )

    return pd.DataFrame(
        rows
    )


def probability_buckets(
    predictions: pd.DataFrame,
    variant: str,
    fold_number: int | None,
) -> pd.DataFrame:
    work = predictions[
        predictions[
            "variant"
        ]
        == variant
    ].copy()

    if fold_number is not None:
        work = work[
            work[
                "fold"
            ]
            == fold_number
        ].copy()

    if work.empty:
        raise RuntimeError(
            f"No predictions for {variant}"
        )

    work[
        "_rank"
    ] = (
        work[
            "model_prob"
        ]
        .rank(
            method="first"
        )
    )

    work[
        "bucket"
    ] = (
        pd.qcut(
            work[
                "_rank"
            ],
            q=5,
            labels=False,
        )
        + 1
    )

    out = (
        work
        .groupby(
            "bucket",
            as_index=False,
        )
        .agg(
            count=(
                "win_binary",
                "size",
            ),
            mean_model_probability=(
                "model_prob",
                "mean",
            ),
            actual_win_rate=(
                "win_binary",
                "mean",
            ),
            min_model_probability=(
                "model_prob",
                "min",
            ),
            max_model_probability=(
                "model_prob",
                "max",
            ),
        )
    )

    out.insert(
        0,
        "variant",
        variant,
    )

    out.insert(
        0,
        "fold",
        (
            fold_number
            if fold_number is not None
            else "ALL"
        ),
    )

    return out


def evaluate_predictions(
    predictions: pd.DataFrame,
    variant: str,
    fold_number: int | None,
) -> tuple[
    dict,
    pd.DataFrame,
]:
    work = predictions[
        predictions[
            "variant"
        ]
        == variant
    ].copy()

    if fold_number is not None:
        work = work[
            work[
                "fold"
            ]
            == fold_number
        ].copy()

    if work.empty:
        raise RuntimeError(
            f"No rows for {variant}"
        )

    over_rows = work[
        work[
            "side"
        ]
        == "over"
    ].copy()

    y_true = (
        over_rows[
            "actual_over"
        ]
        .astype(int)
        .to_numpy()
    )

    p_over = (
        over_rows[
            "over_probability"
        ]
        .astype(float)
        .to_numpy()
    )

    probability_win_corr = (
        safe_corr(
            work[
                "model_prob"
            ],
            work[
                "win_binary"
            ],
        )
    )

    brier = float(
        brier_score_loss(
            y_true,
            p_over,
        )
    )

    ll = float(
        log_loss(
            y_true,
            p_over,
            labels=[
                0,
                1,
            ],
        )
    )

    auc = (
        float(
            roc_auc_score(
                y_true,
                p_over,
            )
        )
        if len(
            np.unique(
                y_true
            )
        ) >= 2
        else np.nan
    )

    buckets = (
        probability_buckets(
            predictions,
            variant,
            fold_number,
        )
    )

    win_rates = (
        buckets[
            "actual_win_rate"
        ]
        .to_numpy(
            dtype=float
        )
    )

    rising_steps = int(
        np.sum(
            np.diff(
                win_rates
            )
            > 0
        )
    )

    low = float(
        buckets.iloc[
            0
        ][
            "actual_win_rate"
        ]
    )

    high = float(
        buckets.iloc[
            -1
        ][
            "actual_win_rate"
        ]
    )

    return (
        {
            "fold":
                (
                    fold_number
                    if fold_number is not None
                    else "ALL"
                ),
            "variant":
                variant,
            "resolved_games":
                int(
                    len(
                        over_rows
                    )
                ),
            "candidate_rows":
                int(
                    len(
                        work
                    )
                ),
            "probability_win_corr":
                probability_win_corr,
            "over_auc":
                auc,
            "brier":
                brier,
            "log_loss":
                ll,
            "low_bucket_win_rate":
                low,
            "high_bucket_win_rate":
                high,
            "high_minus_low":
                (
                    high
                    - low
                ),
            "rising_bucket_steps":
                rising_steps,
            "test_over_rate":
                float(
                    np.mean(
                        y_true
                    )
                ),
        },
        buckets,
    )


def date_text(
    frame: pd.DataFrame,
    which: str,
) -> str:
    value = (
        frame[
            "_game_date_dt"
        ].min()
        if which == "min"
        else frame[
            "_game_date_dt"
        ].max()
    )

    return (
        pd.Timestamp(
            value
        )
        .strftime(
            "%Y-%m-%d"
        )
    )


def write_report(
    aggregate: pd.DataFrame,
    folds: pd.DataFrame,
    buckets: pd.DataFrame,
    verdict: str,
) -> None:
    generated_at = (
        datetime.now(
            UTC
        ).isoformat()
    )

    aggregate_html = (
        aggregate
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
    )

    folds_html = (
        folds
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
    )

    buckets_html = (
        buckets
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
    )

    REPORT_FILE.write_text(
        f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Totals Repeated Holdout</title>
<style>
body {{
    font-family: Arial, sans-serif;
    margin: 30px;
}}
table {{
    border-collapse: collapse;
    margin-bottom: 30px;
}}
th, td {{
    border: 1px solid #ccc;
    padding: 6px 10px;
    text-align: right;
}}
th:first-child, td:first-child {{
    text-align: left;
}}
</style>
</head>
<body>
<h1>MLB Totals Repeated Holdout</h1>
<p>Generated: {generated_at}</p>
<h2>{verdict}</h2>
<h2>Aggregate untouched tests</h2>
{aggregate_html}
<h2>Individual folds</h2>
{folds_html}
<h2>Probability buckets</h2>
{buckets_html}
</body>
</html>
""",
        encoding="utf-8",
    )


def main() -> None:
    print(
        "MLB TOTALS REPEATED HOLDOUT TEST"
    )

    print(
        "Loading exact totals-classifier training data..."
    )

    (
        frame,
        production_features,
    ) = (
        TM.load_training()
    )

    print(
        "Building leakage-safe historical park features..."
    )

    frame = (
        build_leakage_safe_park_features(
            frame
        )
    )

    print(
        "Attaching historical sportsbook total lines..."
    )

    resolved = (
        attach_resolved_totals(
            frame
        )
    )

    unique_dates = (
        resolved[
            "_game_date_dt"
        ]
        .nunique()
    )

    print(
        f"Resolved data: "
        f"{len(resolved):,} games, "
        f"{unique_dates:,} dates"
    )

    folds = make_folds(
        resolved
    )

    all_predictions = []
    fold_metric_rows = []
    bucket_frames = []

    for fold_data in folds:
        fold_number = int(
            fold_data[
                "fold"
            ]
        )

        train = (
            fold_data[
                "train"
            ]
            .sort_values(
                [
                    "_game_date_dt",
                    "game_id",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        validation = (
            fold_data[
                "validation"
            ]
            .sort_values(
                [
                    "_game_date_dt",
                    "game_id",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        test = (
            fold_data[
                "test"
            ]
            .sort_values(
                [
                    "_game_date_dt",
                    "game_id",
                ]
            )
            .reset_index(
                drop=True
            )
        )

        print()
        print(
            f"FOLD {fold_number}"
        )

        print(
            f"Train: "
            f"{date_text(train, 'min')} "
            f"through "
            f"{date_text(train, 'max')} "
            f"({len(train):,} games)"
        )

        print(
            f"Validation: "
            f"{date_text(validation, 'min')} "
            f"through "
            f"{date_text(validation, 'max')} "
            f"({len(validation):,} games)"
        )

        print(
            f"Test: "
            f"{date_text(test, 'min')} "
            f"through "
            f"{date_text(test, 'max')} "
            f"({len(test):,} games)"
        )

        venue_levels = (
            CLASSIFIER
            .venue_levels_from(
                train
            )
        )

        train_x_base = (
            CLASSIFIER
            .classifier_feature_frame(
                train,
                production_features,
                venue_levels,
            )
            .reset_index(
                drop=True
            )
        )

        validation_x_base = (
            CLASSIFIER
            .classifier_feature_frame(
                validation,
                production_features,
                venue_levels,
            )
            .reset_index(
                drop=True
            )
        )

        test_x_base = (
            CLASSIFIER
            .classifier_feature_frame(
                test,
                production_features,
                venue_levels,
            )
            .reset_index(
                drop=True
            )
        )

        train_x_park = (
            add_park_features(
                train_x_base,
                train,
            )
        )

        validation_x_park = (
            add_park_features(
                validation_x_base,
                validation,
            )
        )

        test_x_park = (
            add_park_features(
                test_x_base,
                test,
            )
        )

        train_y = (
            train[
                "_over_target"
            ]
            .astype(int)
            .reset_index(
                drop=True
            )
        )

        validation_y = (
            validation[
                "_over_target"
            ]
            .astype(int)
            .reset_index(
                drop=True
            )
        )

        print(
            "Tuning baseline..."
        )

        (
            baseline_params,
            baseline_val_ll,
            baseline_val_brier,
        ) = tune_classifier(
            train_x_base,
            train_y,
            validation_x_base,
            validation_y,
        )

        print(
            "Tuning park model..."
        )

        (
            park_params,
            park_val_ll,
            park_val_brier,
        ) = tune_classifier(
            train_x_park,
            train_y,
            validation_x_park,
            validation_y,
        )

        print(
            "Selecting recent-game weighting..."
        )

        (
            half_life,
            recent_val_ll,
            recent_val_brier,
        ) = (
            choose_recency_half_life(
                train,
                train_x_park,
                train_y,
                validation_x_park,
                validation_y,
                park_params,
            )
        )

        print(
            f"Selected recency half-life: "
            f"{half_life} days"
        )

        baseline_model = (
            fit_final(
                train,
                validation,
                train_x_base,
                validation_x_base,
                train_y,
                validation_y,
                baseline_params,
                None,
            )
        )

        park_recent_model = (
            fit_final(
                train,
                validation,
                train_x_park,
                validation_x_park,
                train_y,
                validation_y,
                park_params,
                half_life,
            )
        )

        baseline_probability = (
            predict_over_probability(
                baseline_model,
                test_x_base,
            )
        )

        park_recent_probability = (
            predict_over_probability(
                park_recent_model,
                test_x_park,
            )
        )

        baseline_predictions = (
            candidate_predictions(
                fold_number,
                "baseline",
                test,
                baseline_probability,
            )
        )

        park_recent_predictions = (
            candidate_predictions(
                fold_number,
                "park_recent",
                test,
                park_recent_probability,
            )
        )

        fold_predictions = pd.concat(
            [
                baseline_predictions,
                park_recent_predictions,
            ],
            ignore_index=True,
            sort=False,
        )

        all_predictions.append(
            fold_predictions
        )

        for variant in (
            "baseline",
            "park_recent",
        ):
            (
                metrics,
                buckets,
            ) = (
                evaluate_predictions(
                    fold_predictions,
                    variant,
                    fold_number,
                )
            )

            metrics[
                "train_start"
            ] = date_text(
                train,
                "min",
            )

            metrics[
                "train_end"
            ] = date_text(
                train,
                "max",
            )

            metrics[
                "validation_start"
            ] = date_text(
                validation,
                "min",
            )

            metrics[
                "validation_end"
            ] = date_text(
                validation,
                "max",
            )

            metrics[
                "test_start"
            ] = date_text(
                test,
                "min",
            )

            metrics[
                "test_end"
            ] = date_text(
                test,
                "max",
            )

            if variant == "baseline":
                metrics[
                    "recency_half_life_days"
                ] = np.nan

                metrics[
                    "validation_log_loss"
                ] = baseline_val_ll

                metrics[
                    "validation_brier"
                ] = baseline_val_brier

                metrics[
                    "selected_hyperparameters"
                ] = json.dumps(
                    baseline_params,
                    sort_keys=True,
                )

            else:
                metrics[
                    "recency_half_life_days"
                ] = half_life

                metrics[
                    "validation_log_loss"
                ] = recent_val_ll

                metrics[
                    "validation_brier"
                ] = recent_val_brier

                metrics[
                    "selected_hyperparameters"
                ] = json.dumps(
                    park_params,
                    sort_keys=True,
                )

            fold_metric_rows.append(
                metrics
            )

            bucket_frames.append(
                buckets
            )

    predictions = pd.concat(
        all_predictions,
        ignore_index=True,
        sort=False,
    )

    fold_metrics = pd.DataFrame(
        fold_metric_rows
    )

    aggregate_rows = []

    for variant in (
        "baseline",
        "park_recent",
    ):
        (
            metrics,
            buckets,
        ) = (
            evaluate_predictions(
                predictions,
                variant,
                None,
            )
        )

        aggregate_rows.append(
            metrics
        )

        bucket_frames.append(
            buckets
        )

    aggregate = pd.DataFrame(
        aggregate_rows
    )

    baseline_aggregate = (
        aggregate[
            aggregate[
                "variant"
            ]
            == "baseline"
        ]
        .iloc[0]
    )

    park_aggregate = (
        aggregate[
            aggregate[
                "variant"
            ]
            == "park_recent"
        ]
        .iloc[0]
    )

    baseline_folds = (
        fold_metrics[
            fold_metrics[
                "variant"
            ]
            == "baseline"
        ]
        .set_index(
            "fold"
        )
    )

    park_folds = (
        fold_metrics[
            fold_metrics[
                "variant"
            ]
            == "park_recent"
        ]
        .set_index(
            "fold"
        )
    )

    correlation_fold_wins = int(
        (
            park_folds[
                "probability_win_corr"
            ]
            > baseline_folds[
                "probability_win_corr"
            ]
        ).sum()
    )

    auc_fold_wins = int(
        (
            park_folds[
                "over_auc"
            ]
            > baseline_folds[
                "over_auc"
            ]
        ).sum()
    )

    brier_fold_wins = int(
        (
            park_folds[
                "brier"
            ]
            < baseline_folds[
                "brier"
            ]
        ).sum()
    )

    log_loss_fold_wins = int(
        (
            park_folds[
                "log_loss"
            ]
            < baseline_folds[
                "log_loss"
            ]
        ).sum()
    )

    aggregate[
        "correlation_fold_wins"
    ] = np.nan

    aggregate[
        "auc_fold_wins"
    ] = np.nan

    aggregate[
        "brier_fold_wins"
    ] = np.nan

    aggregate[
        "log_loss_fold_wins"
    ] = np.nan

    aggregate.loc[
        aggregate[
            "variant"
        ]
        == "park_recent",
        "correlation_fold_wins",
    ] = correlation_fold_wins

    aggregate.loc[
        aggregate[
            "variant"
        ]
        == "park_recent",
        "auc_fold_wins",
    ] = auc_fold_wins

    aggregate.loc[
        aggregate[
            "variant"
        ]
        == "park_recent",
        "brier_fold_wins",
    ] = brier_fold_wins

    aggregate.loc[
        aggregate[
            "variant"
        ]
        == "park_recent",
        "log_loss_fold_wins",
    ] = log_loss_fold_wins

    aggregate_corr_better = (
        float(
            park_aggregate[
                "probability_win_corr"
            ]
        )
        > float(
            baseline_aggregate[
                "probability_win_corr"
            ]
        )
    )

    aggregate_auc_better = (
        float(
            park_aggregate[
                "over_auc"
            ]
        )
        > float(
            baseline_aggregate[
                "over_auc"
            ]
        )
    )

    aggregate_brier_better = (
        float(
            park_aggregate[
                "brier"
            ]
        )
        < float(
            baseline_aggregate[
                "brier"
            ]
        )
    )

    aggregate_log_loss_better = (
        float(
            park_aggregate[
                "log_loss"
            ]
        )
        < float(
            baseline_aggregate[
                "log_loss"
            ]
        )
    )

    primary_pass = (
        aggregate_corr_better
        and correlation_fold_wins >= 3
    )

    secondary_wins = sum(
        [
            aggregate_auc_better,
            aggregate_brier_better,
            aggregate_log_loss_better,
        ]
    )

    if (
        primary_pass
        and secondary_wins >= 2
    ):
        verdict = (
            "PARK + RECENT: PASS"
        )

    elif primary_pass:
        verdict = (
            "PARK + RECENT: MIXED"
        )

    else:
        verdict = (
            "PARK + RECENT: FAIL"
        )

    buckets = pd.concat(
        bucket_frames,
        ignore_index=True,
        sort=False,
    )

    aggregate.to_csv(
        SUMMARY_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    fold_metrics.to_csv(
        FOLD_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    buckets.to_csv(
        BUCKET_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    predictions.to_csv(
        PREDICTIONS_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    write_report(
        aggregate,
        fold_metrics,
        buckets,
        verdict,
    )

    print()
    print(
        "AGGREGATE RESULTS"
    )

    print(
        aggregate[
            [
                "variant",
                "resolved_games",
                "probability_win_corr",
                "over_auc",
                "brier",
                "log_loss",
                "low_bucket_win_rate",
                "high_bucket_win_rate",
                "high_minus_low",
                "rising_bucket_steps",
            ]
        ]
        .to_string(
            index=False
        )
    )

    print()
    print(
        "FOLD CORRELATION RESULTS"
    )

    fold_display = (
        fold_metrics[
            [
                "fold",
                "variant",
                "resolved_games",
                "probability_win_corr",
                "over_auc",
                "brier",
                "log_loss",
                "high_minus_low",
                "rising_bucket_steps",
                "recency_half_life_days",
            ]
        ]
        .sort_values(
            [
                "fold",
                "variant",
            ]
        )
    )

    print(
        fold_display.to_string(
            index=False
        )
    )

    print()
    print(
        f"Park+recent correlation wins: "
        f"{correlation_fold_wins}/5"
    )

    print(
        f"Park+recent AUC wins: "
        f"{auc_fold_wins}/5"
    )

    print(
        f"Park+recent Brier wins: "
        f"{brier_fold_wins}/5"
    )

    print(
        f"Park+recent log-loss wins: "
        f"{log_loss_fold_wins}/5"
    )

    print()
    print(
        verdict
    )

    print()
    print(
        f"Summary: {SUMMARY_FILE}"
    )

    print(
        f"Folds: {FOLD_FILE}"
    )

    print(
        f"Buckets: {BUCKET_FILE}"
    )

    print(
        f"Predictions: {PREDICTIONS_FILE}"
    )

    print(
        f"Report: {REPORT_FILE}"
    )


if __name__ == "__main__":
    main()