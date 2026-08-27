#!/usr/bin/env python3
"""
Leakage-safe MLB totals classifier experiment.

Compares four direct Over/Under classifier variants:

1. baseline
   Current direct totals classifier feature set.

2. park
   Baseline plus a leakage-safe historical park scoring factor built using
   ONLY completed games from dates before each target game.

3. recent
   Baseline with newer training games receiving more weight.

4. park_recent
   Leakage-safe park factor plus recent-game weighting.

IMPORTANT:
The repository's static park-factor files are currently labeled 2024-2026.
Those files are NOT used here because they can contain information from later
2026 games and therefore are not safe for a historical 2026 holdout test.

Sportsbook total LINE is a model input because the classifier predicts whether
the game will finish over that threshold.

Sportsbook odds/prices/juice are NEVER model inputs.

Outputs:
    mlb_total_park_recency_summary.csv
    mlb_total_park_recency_buckets.csv
    mlb_total_park_recency_predictions.csv
    mlb_total_park_recency_report.html
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

# Shrink a venue's observed scoring toward the league scoring environment
# until the venue has accumulated a useful number of prior games.
PARK_SHRINK_GAMES = 10.0

# Do not calculate a park factor until enough league games have occurred
# to establish a minimally useful league scoring baseline.
MIN_PRIOR_LEAGUE_GAMES = 30

# Small recency grid. Half-life means a game this many days old receives
# half the weight of the newest training game.
RECENCY_HALF_LIVES = (
    14,
    30,
    60,
    90,
)


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

BASE_CLASSIFIER_SCRIPT = (
    BASE
    / "scripts/modeling/test_total_classifier.py"
)

SUMMARY_FILE = (
    ROOT
    / "mlb_total_park_recency_summary.csv"
)

BUCKET_FILE = (
    ROOT
    / "mlb_total_park_recency_buckets.csv"
)

PREDICTIONS_FILE = (
    ROOT
    / "mlb_total_park_recency_predictions.csv"
)

REPORT_FILE = (
    ROOT
    / "mlb_total_park_recency_report.html"
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


BASE_CLASSIFIER = load_module(
    "total_classifier_existing",
    BASE_CLASSIFIER_SCRIPT,
)

TM = BASE_CLASSIFIER.TM
TRAIN = BASE_CLASSIFIER.TRAIN

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
    """
    Build a park scoring factor using only games completed BEFORE each date.

    All games on the same date receive factors calculated before any results
    from that date are added to history.
    """

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
            "Cannot build leakage-safe park factors; "
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

        # Calculate every game on this date BEFORE adding any result
        # from this date to historical totals.
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

            if (
                prior_count > 0
            ):
                venue_prior_avg = (
                    prior_sum
                    / prior_count
                )

                out.at[
                    idx,
                    "park_prior_avg_total",
                ] = (
                    venue_prior_avg
                )

            if (
                league_count
                < MIN_PRIOR_LEAGUE_GAMES
                or not np.isfinite(
                    league_prior_avg
                )
                or league_prior_avg
                <= 0
            ):
                continue

            out.at[
                idx,
                "league_prior_avg_total",
            ] = (
                league_prior_avg
            )

            # Bayesian-style shrinkage toward the prior league scoring
            # environment prevents tiny park samples from producing
            # extreme factors.
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
                    "Non-finite leakage-safe park factor "
                    f"for game_id={out.at[idx, 'game_id']}"
                )

            out.at[
                idx,
                "park_run_factor_asof",
            ] = float(
                park_factor
            )

        # Only after all features for the date have been calculated
        # are that day's results allowed into history.
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
                    "Invalid actual total while building park history: "
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
                    0
                )
                + 1
            )

    out = out.drop(
        columns=[
            "_actual_total_for_park",
            "_venue_for_park",
        ]
    )

    return out


def attach_and_resolve(
    frame: pd.DataFrame,
    label: str,
) -> pd.DataFrame:
    attached = (
        BASE_CLASSIFIER
        .attach_lines(
            frame
        )
    )

    resolved = attached[
        ~attached[
            "_is_push"
        ]
    ].copy()

    resolved = (
        resolved
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

    if resolved.empty:
        raise RuntimeError(
            f"{label} has no resolved totals"
        )

    if (
        resolved["game_id"]
        .astype(str)
        .duplicated()
        .any()
    ):
        duplicates = (
            resolved.loc[
                resolved[
                    "game_id"
                ]
                .astype(str)
                .duplicated(
                    keep=False
                ),
                "game_id",
            ]
            .astype(str)
            .tolist()
        )

        raise RuntimeError(
            f"{label} contains duplicate game_id values: "
            f"{duplicates[:20]}"
        )

    return resolved


def add_park_features(
    feature_frame: pd.DataFrame,
    source_frame: pd.DataFrame,
) -> pd.DataFrame:
    out = (
        feature_frame
        .reset_index(
            drop=True
        )
        .copy()
    )

    source = (
        source_frame
        .reset_index(
            drop=True
        )
    )

    for col in PARK_FEATURE_COLUMNS:
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
                f"Non-finite park feature values in {col}"
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
    features: pd.DataFrame,
) -> np.ndarray:
    if 1 not in model.classes_:
        raise RuntimeError(
            "Classifier has no Over class"
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
            features
        )[
            :,
            over_index
        ]
    )

    probability = np.asarray(
        probability,
        dtype=float,
    )

    if (
        np.any(
            ~np.isfinite(
                probability
            )
        )
        or np.any(
            probability <= 0
        )
        or np.any(
            probability >= 1
        )
    ):
        probability = np.clip(
            probability,
            EPS,
            1.0 - EPS,
        )

    return probability


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
            "Recency weighting received invalid dates"
        )

    newest_date = (
        dates.max()
    )

    age_days = (
        (
            newest_date
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
            "Invalid recency sample weights"
        )

    # Normalize so average training weight remains 1.
    weights = (
        weights
        / np.mean(
            weights
        )
    )

    return weights


def tune_classifier(
    train_x: pd.DataFrame,
    train_y: pd.Series,
    validation_x: pd.DataFrame,
    validation_y: pd.Series,
    sample_weight: np.ndarray | None = None,
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
            sample_weight=sample_weight,
        )

        probability = (
            predict_over_probability(
                model,
                validation_x,
            )
        )

        validation_log_loss = float(
            log_loss(
                validation_y,
                probability,
                labels=[
                    0,
                    1,
                ],
            )
        )

        validation_brier = float(
            brier_score_loss(
                validation_y,
                probability,
            )
        )

        if (
            validation_log_loss
            < best_log_loss
            - EPS
            or (
                abs(
                    validation_log_loss
                    - best_log_loss
                )
                <= EPS
                and validation_brier
                < best_brier
            )
        ):
            best_params = dict(
                params
            )

            best_log_loss = (
                validation_log_loss
            )

            best_brier = (
                validation_brier
            )

    if candidate_count != 81:
        raise RuntimeError(
            "Classifier grid expected 81 candidates; "
            f"evaluated {candidate_count}"
        )

    if best_params is None:
        raise RuntimeError(
            "Classifier failed to select parameters"
        )

    return (
        best_params,
        best_log_loss,
        best_brier,
    )


def select_recency_half_life(
    train_frame: pd.DataFrame,
    train_x: pd.DataFrame,
    train_y: pd.Series,
    validation_x: pd.DataFrame,
    validation_y: pd.Series,
    params: dict,
    label: str,
) -> tuple[
    int,
    list[dict],
]:
    trials = []

    best_half_life = None
    best_log_loss = np.inf
    best_brier = np.inf

    for half_life in (
        RECENCY_HALF_LIVES
    ):
        weights = (
            recency_weights(
                train_frame,
                half_life,
            )
        )

        model = make_model(
            params
        )

        model.fit(
            train_x,
            train_y,
            sample_weight=weights,
        )

        probability = (
            predict_over_probability(
                model,
                validation_x,
            )
        )

        score = float(
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

        trials.append(
            {
                "half_life_days":
                    int(
                        half_life
                    ),
                "validation_log_loss":
                    score,
                "validation_brier":
                    brier,
            }
        )

        print(
            f"{label} recency "
            f"half_life={half_life}d "
            f"validation_log_loss="
            f"{score:.6f} "
            f"validation_brier="
            f"{brier:.6f}"
        )

        if (
            score
            < best_log_loss
            - EPS
            or (
                abs(
                    score
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

            best_log_loss = (
                score
            )

            best_brier = (
                brier
            )

    if best_half_life is None:
        raise RuntimeError(
            f"{label} failed to select "
            "a recency half-life"
        )

    return (
        best_half_life,
        trials,
    )


def fit_final_classifier(
    train_frame: pd.DataFrame,
    validation_frame: pd.DataFrame,
    train_x: pd.DataFrame,
    validation_x: pd.DataFrame,
    train_y: pd.Series,
    validation_y: pd.Series,
    params: dict,
    half_life_days: int | None,
) -> HistGradientBoostingClassifier:
    fit_frame = pd.concat(
        [
            train_frame,
            validation_frame,
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
    variant: str,
    test_frame: pd.DataFrame,
    p_over: np.ndarray,
) -> pd.DataFrame:
    if len(test_frame) != len(
        p_over
    ):
        raise RuntimeError(
            f"{variant} prediction length mismatch"
        )

    rows = []

    for i, row in (
        test_frame
        .reset_index(
            drop=True
        )
        .iterrows()
    ):
        over_probability = float(
            p_over[i]
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

        actual_total = float(
            row[
                "_actual_total"
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
                actual_total,
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
) -> pd.DataFrame:
    work = (
        predictions[
            predictions[
                "variant"
            ]
            == variant
        ]
        .copy()
    )

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

    buckets = (
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

    buckets.insert(
        0,
        "variant",
        variant,
    )

    return buckets


def evaluate_variant(
    variant: str,
    predictions: pd.DataFrame,
) -> tuple[
    dict,
    pd.DataFrame,
]:
    work = (
        predictions[
            predictions[
                "variant"
            ]
            == variant
        ]
        .copy()
    )

    if work.empty:
        raise RuntimeError(
            f"No predictions for {variant}"
        )

    over_rows = (
        work[
            work[
                "side"
            ]
            == "over"
        ]
        .copy()
    )

    if over_rows.empty:
        raise RuntimeError(
            f"No Over predictions for {variant}"
        )

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

    if (
        len(
            np.unique(
                y_true
            )
        )
        >= 2
    ):
        auc = float(
            roc_auc_score(
                y_true,
                p_over,
            )
        )
    else:
        auc = np.nan

    buckets = (
        probability_buckets(
            predictions,
            variant,
        )
    )

    bucket_win_rates = (
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
                bucket_win_rates
            )
            > 0
        )
    )

    low_bucket = float(
        buckets.iloc[
            0
        ][
            "actual_win_rate"
        ]
    )

    high_bucket = float(
        buckets.iloc[
            -1
        ][
            "actual_win_rate"
        ]
    )

    summary = {
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
            low_bucket,
        "high_bucket_win_rate":
            high_bucket,
        "high_minus_low":
            (
                high_bucket
                - low_bucket
            ),
        "rising_bucket_steps":
            rising_steps,
        "mean_model_probability":
            float(
                work[
                    "model_prob"
                ].mean()
            ),
        "mean_actual_win_rate":
            float(
                work[
                    "win_binary"
                ].mean()
            ),
        "test_over_rate":
            float(
                np.mean(
                    y_true
                )
            ),
    }

    return (
        summary,
        buckets,
    )


def write_report(
    summary: pd.DataFrame,
    buckets: pd.DataFrame,
    recency_trials: dict,
) -> None:
    generated_at = (
        datetime.now(
            UTC
        ).isoformat()
    )

    summary_html = (
        summary
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
    )

    bucket_html = (
        buckets
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
    )

    recency_rows = []

    for (
        variant,
        trials,
    ) in recency_trials.items():
        for trial in trials:
            recency_rows.append(
                {
                    "variant":
                        variant,
                    **trial,
                }
            )

    recency_frame = (
        pd.DataFrame(
            recency_rows
        )
    )

    recency_html = (
        recency_frame
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
        if not recency_frame.empty
        else "<p>No recency trials.</p>"
    )

    html = f"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Total Park + Recency Test</title>
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
h1, h2 {{
    margin-top: 28px;
}}
.note {{
    max-width: 1000px;
    line-height: 1.5;
}}
</style>
</head>
<body>

<h1>MLB Total Park + Recency Test</h1>

<p class="note">
Generated: {generated_at}
</p>

<p class="note">
Static repository park-factor files were intentionally not used because
they are labeled 2024-2026 and are therefore not leakage-safe for a
historical 2026 holdout test.
</p>

<p class="note">
The park variant uses only completed games from dates before the target
game. Games on the same date cannot affect one another's park factor.
</p>

<h2>Summary</h2>
{summary_html}

<h2>Probability Buckets</h2>
{bucket_html}

<h2>Recency Half-Life Validation</h2>
{recency_html}

</body>
</html>
"""

    REPORT_FILE.write_text(
        html,
        encoding="utf-8",
    )


def main() -> None:
    print(
        "TOTAL PARK + RECENCY TEST"
    )

    print(
        "Static 2024-2026 park-factor files "
        "are NOT used because they are not "
        "safe for historical 2026 testing."
    )

    print(
        "Building current production-feature "
        "training data..."
    )

    (
        frame,
        production_features,
    ) = (
        TM.load_training()
    )

    print(
        "Building leakage-safe as-of park factors..."
    )

    frame = (
        build_leakage_safe_park_features(
            frame
        )
    )

    splits = (
        TRAIN
        .chronological_date_split(
            frame
        )
    )

    train_base = (
        splits[
            "train"
        ]
        .copy()
    )

    validation_base = (
        splits[
            "validation"
        ]
        .copy()
    )

    test_base = (
        splits[
            "test"
        ]
        .copy()
    )

    train = (
        attach_and_resolve(
            train_base,
            "train",
        )
    )

    validation = (
        attach_and_resolve(
            validation_base,
            "validation",
        )
    )

    test = (
        attach_and_resolve(
            test_base,
            "test",
        )
    )

    print(
        f"Resolved games: "
        f"train={len(train):,} "
        f"validation={len(validation):,} "
        f"test={len(test):,}"
    )

    print(
        "Over rates: "
        f"train={train['_over_target'].mean():.3f} "
        f"validation={validation['_over_target'].mean():.3f} "
        f"test={test['_over_target'].mean():.3f}"
    )

    venue_levels = (
        BASE_CLASSIFIER
        .venue_levels_from(
            train
        )
    )

    train_x_base = (
        BASE_CLASSIFIER
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
        BASE_CLASSIFIER
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
        BASE_CLASSIFIER
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

    if (
        list(
            train_x_base.columns
        )
        != list(
            validation_x_base.columns
        )
        or list(
            train_x_base.columns
        )
        != list(
            test_x_base.columns
        )
    ):
        raise RuntimeError(
            "Baseline classifier feature columns "
            "do not match across splits"
        )

    if (
        list(
            train_x_park.columns
        )
        != list(
            validation_x_park.columns
        )
        or list(
            train_x_park.columns
        )
        != list(
            test_x_park.columns
        )
    ):
        raise RuntimeError(
            "Park classifier feature columns "
            "do not match across splits"
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
        f"Baseline classifier features: "
        f"{train_x_base.shape[1]}"
    )

    print(
        f"Park classifier features: "
        f"{train_x_park.shape[1]}"
    )

    print(
        "Tuning baseline classifier..."
    )

    (
        baseline_params,
        baseline_validation_log_loss,
        baseline_validation_brier,
    ) = tune_classifier(
        train_x_base,
        train_y,
        validation_x_base,
        validation_y,
    )

    print(
        "Baseline selected: "
        f"log_loss="
        f"{baseline_validation_log_loss:.6f} "
        f"brier="
        f"{baseline_validation_brier:.6f} "
        f"params={baseline_params}"
    )

    print(
        "Tuning park classifier..."
    )

    (
        park_params,
        park_validation_log_loss,
        park_validation_brier,
    ) = tune_classifier(
        train_x_park,
        train_y,
        validation_x_park,
        validation_y,
    )

    print(
        "Park selected: "
        f"log_loss="
        f"{park_validation_log_loss:.6f} "
        f"brier="
        f"{park_validation_brier:.6f} "
        f"params={park_params}"
    )

    print(
        "Testing recency weighting on baseline..."
    )

    (
        baseline_half_life,
        baseline_recency_trials,
    ) = select_recency_half_life(
        train,
        train_x_base,
        train_y,
        validation_x_base,
        validation_y,
        baseline_params,
        "baseline",
    )

    print(
        "Testing recency weighting with park factors..."
    )

    (
        park_half_life,
        park_recency_trials,
    ) = select_recency_half_life(
        train,
        train_x_park,
        train_y,
        validation_x_park,
        validation_y,
        park_params,
        "park",
    )

    print(
        f"Selected baseline recency half-life: "
        f"{baseline_half_life} days"
    )

    print(
        f"Selected park recency half-life: "
        f"{park_half_life} days"
    )

    variant_specs = [
        {
            "variant":
                "baseline",
            "train_x":
                train_x_base,
            "validation_x":
                validation_x_base,
            "test_x":
                test_x_base,
            "params":
                baseline_params,
            "half_life":
                None,
        },
        {
            "variant":
                "park",
            "train_x":
                train_x_park,
            "validation_x":
                validation_x_park,
            "test_x":
                test_x_park,
            "params":
                park_params,
            "half_life":
                None,
        },
        {
            "variant":
                "recent",
            "train_x":
                train_x_base,
            "validation_x":
                validation_x_base,
            "test_x":
                test_x_base,
            "params":
                baseline_params,
            "half_life":
                baseline_half_life,
        },
        {
            "variant":
                "park_recent",
            "train_x":
                train_x_park,
            "validation_x":
                validation_x_park,
            "test_x":
                test_x_park,
            "params":
                park_params,
            "half_life":
                park_half_life,
        },
    ]

    prediction_frames = []
    summary_rows = []
    bucket_frames = []

    for spec in variant_specs:
        variant = spec[
            "variant"
        ]

        print(
            f"Fitting final {variant} classifier..."
        )

        model = (
            fit_final_classifier(
                train,
                validation,
                spec[
                    "train_x"
                ],
                spec[
                    "validation_x"
                ],
                train_y,
                validation_y,
                spec[
                    "params"
                ],
                spec[
                    "half_life"
                ],
            )
        )

        p_over = (
            predict_over_probability(
                model,
                spec[
                    "test_x"
                ],
            )
        )

        predictions = (
            candidate_predictions(
                variant,
                test,
                p_over,
            )
        )

        (
            metrics,
            buckets,
        ) = (
            evaluate_variant(
                variant,
                predictions,
            )
        )

        metrics[
            "feature_count"
        ] = int(
            spec[
                "train_x"
            ].shape[1]
        )

        metrics[
            "recency_half_life_days"
        ] = (
            spec[
                "half_life"
            ]
            if spec[
                "half_life"
            ]
            is not None
            else np.nan
        )

        metrics[
            "selected_hyperparameters"
        ] = json.dumps(
            spec[
                "params"
            ],
            sort_keys=True,
        )

        prediction_frames.append(
            predictions
        )

        summary_rows.append(
            metrics
        )

        bucket_frames.append(
            buckets
        )

    summary = pd.DataFrame(
        summary_rows
    )

    predictions = pd.concat(
        prediction_frames,
        ignore_index=True,
        sort=False,
    )

    buckets = pd.concat(
        bucket_frames,
        ignore_index=True,
        sort=False,
    )

    baseline_row = (
        summary[
            summary[
                "variant"
            ]
            == "baseline"
        ]
        .iloc[0]
    )

    summary[
        "corr_change_vs_baseline"
    ] = (
        summary[
            "probability_win_corr"
        ]
        - float(
            baseline_row[
                "probability_win_corr"
            ]
        )
    )

    summary[
        "gap_change_vs_baseline"
    ] = (
        summary[
            "high_minus_low"
        ]
        - float(
            baseline_row[
                "high_minus_low"
            ]
        )
    )

    summary[
        "brier_change_vs_baseline"
    ] = (
        summary[
            "brier"
        ]
        - float(
            baseline_row[
                "brier"
            ]
        )
    )

    summary[
        "log_loss_change_vs_baseline"
    ] = (
        summary[
            "log_loss"
        ]
        - float(
            baseline_row[
                "log_loss"
            ]
        )
    )

    summary = (
        summary
        .sort_values(
            [
                "probability_win_corr",
                "high_minus_low",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .reset_index(
            drop=True
        )
    )

    summary.to_csv(
        SUMMARY_FILE,
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
        summary,
        buckets,
        {
            "recent":
                baseline_recency_trials,
            "park_recent":
                park_recency_trials,
        },
    )

    print()
    print(
        "RESULTS"
    )

    print(
        summary[
            [
                "variant",
                "probability_win_corr",
                "over_auc",
                "brier",
                "log_loss",
                "low_bucket_win_rate",
                "high_bucket_win_rate",
                "high_minus_low",
                "rising_bucket_steps",
                "corr_change_vs_baseline",
                "gap_change_vs_baseline",
            ]
        ].to_string(
            index=False
        )
    )

    winner = (
        summary.iloc[
            0
        ][
            "variant"
        ]
    )

    print()
    print(
        "BEST PROBABILITY/WIN "
        f"RELATIONSHIP: {winner}"
    )

    print()
    print(
        f"Summary: {SUMMARY_FILE}"
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