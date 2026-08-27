#!/usr/bin/env python3
"""
Repeated chronological holdout test for simplified MLB totals models.

Compares four direct Over/Under models:

1. line_only
   Logistic regression using only the sportsbook total line.

2. line_dratings
   Logistic regression using the total line plus DRatings scoring information.

3. compact
   Logistic regression using the total line, DRatings, and a small set of
   starting-pitcher / bullpen summary features.

4. current_full
   The current large HistGradientBoostingClassifier feature set.

Five expanding chronological holdouts are used.

Each fold:
    earlier dates -> training
    next dates    -> validation
    following     -> untouched test

Primary question:
    Does higher model probability correspond to higher actual win rate?

Secondary metrics:
    AUC
    Brier score
    log loss
    low-to-high probability bucket win-rate gap
    rising probability buckets

Sportsbook total LINE is allowed because the model predicts whether the game
finishes above or below that threshold.

Sportsbook odds, prices, juice, implied probability, EV, and Kelly are NEVER
model features.

Outputs:
    mlb_total_simplified_summary.csv
    mlb_total_simplified_folds.csv
    mlb_total_simplified_buckets.csv
    mlb_total_simplified_predictions.csv
    mlb_total_simplified_report.html
"""

from __future__ import annotations

import importlib.util
import json
import warnings
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


warnings.filterwarnings(
    "ignore",
    category=PerformanceWarning,
)

warnings.filterwarnings(
    "ignore",
    message=r".*sklearn\.utils\.parallel\.delayed.*",
    category=UserWarning,
)


EPS = 1e-12

LOGISTIC_C_VALUES = (
    0.01,
    0.03,
    0.10,
    0.30,
    1.00,
    3.00,
    10.00,
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
    / "mlb_total_simplified_summary.csv"
)

FOLD_FILE = (
    ROOT
    / "mlb_total_simplified_folds.csv"
)

BUCKET_FILE = (
    ROOT
    / "mlb_total_simplified_buckets.csv"
)

PREDICTIONS_FILE = (
    ROOT
    / "mlb_total_simplified_predictions.csv"
)

REPORT_FILE = (
    ROOT
    / "mlb_total_simplified_report.html"
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


MODEL_FEATURES = {
    "line_only": [
        "sportsbook_total_line",
    ],

    "line_dratings": [
        "sportsbook_total_line",
        "dratings_total_projected_runs",
        "dratings_total_edge",
        "dratings_sum_edge",
        "dratings_scoring_balance",
    ],

    "compact": [
        "sportsbook_total_line",
        "dratings_total_projected_runs",
        "dratings_total_edge",
        "dratings_sum_edge",
        "dratings_scoring_balance",
        "sp_xera_sum",
        "sp_xera_30d_sum",
        "sp_xwoba_30d_sum",
        "bp_woba_7d_sum",
        "bp_k_7d_sum",
        "bp_bb_7d_sum",
        "bp_hard_7d_sum",
        "bp_pitches_3d_sum",
    ],
}


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


def prepare_resolved_data():
    print(
        "Loading exact current totals training data..."
    )

    (
        frame,
        production_features,
    ) = (
        TM.load_training()
    )

    print(
        "Attaching historical sportsbook total lines..."
    )

    frame = (
        CLASSIFIER
        .attach_lines(
            frame
        )
    )

    frame = frame[
        ~frame[
            "_is_push"
        ]
    ].copy()

    if frame.empty:
        raise RuntimeError(
            "No resolved totals available"
        )

    frame = (
        frame
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
        frame["game_id"]
        .astype(str)
        .duplicated()
        .any()
    ):
        raise RuntimeError(
            "Resolved totals contain duplicate game_id"
        )

    print(
        f"Resolved data: "
        f"{len(frame):,} games, "
        f"{frame['_game_date_dt'].nunique():,} dates"
    )

    return (
        frame,
        production_features,
    )


def make_folds(
    frame: pd.DataFrame,
):
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
            "Not enough dates for repeated holdout"
        )

    folds = []
    used_test_dates = set()

    for (
        fold_number,
        train_fraction,
    ) in enumerate(
        FOLD_TRAIN_ENDS,
        start=1,
    ):
        train_end = int(
            np.floor(
                n_dates
                * train_fraction
            )
        )

        validation_end = int(
            np.floor(
                n_dates
                * (
                    train_fraction
                    + FOLD_WIDTH
                )
            )
        )

        if fold_number == len(
            FOLD_TRAIN_ENDS
        ):
            test_end = n_dates

        else:
            test_end = int(
                np.floor(
                    n_dates
                    * (
                        train_fraction
                        + 2 * FOLD_WIDTH
                    )
                )
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
            used_test_dates
            & test_dates
        ):
            raise RuntimeError(
                "Test folds overlap"
            )

        used_test_dates.update(
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
                f"Fold {fold_number} has empty partition"
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


def build_full_feature_frames(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
    production_features: list[str],
):
    venue_levels = (
        CLASSIFIER
        .venue_levels_from(
            train
        )
    )

    train_x = (
        CLASSIFIER
        .classifier_feature_frame(
            train,
            production_features,
            venue_levels,
        )
        .reset_index(
            drop=True
        )
        .copy()
    )

    validation_x = (
        CLASSIFIER
        .classifier_feature_frame(
            validation,
            production_features,
            venue_levels,
        )
        .reset_index(
            drop=True
        )
        .copy()
    )

    test_x = (
        CLASSIFIER
        .classifier_feature_frame(
            test,
            production_features,
            venue_levels,
        )
        .reset_index(
            drop=True
        )
        .copy()
    )

    if (
        list(
            train_x.columns
        )
        != list(
            validation_x.columns
        )
        or list(
            train_x.columns
        )
        != list(
            test_x.columns
        )
    ):
        raise RuntimeError(
            "Full feature columns differ across splits"
        )

    required_simple = sorted(
        {
            feature
            for features
            in MODEL_FEATURES.values()
            for feature in features
        }
    )

    missing = [
        col
        for col in required_simple
        if col not in train_x.columns
    ]

    if missing:
        raise RuntimeError(
            "Current classifier did not create "
            f"required simplified features: {missing}"
        )

    return (
        train_x,
        validation_x,
        test_x,
    )


def make_logistic_model(
    columns: list[str],
    c_value: float,
):
    numeric_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="median",
                    add_indicator=True,
                ),
            ),
            (
                "scale",
                StandardScaler(),
            ),
        ]
    )

    processor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                numeric_pipeline,
                columns,
            ),
        ],
        remainder="drop",
    )

    return Pipeline(
        steps=[
            (
                "prepare",
                processor,
            ),
            (
                "model",
                LogisticRegression(
                    penalty="l2",
                    C=float(
                        c_value
                    ),
                    solver="lbfgs",
                    max_iter=5000,
                    random_state=(
                        TRAIN.RANDOM_STATE
                    ),
                ),
            ),
        ]
    )


def predict_probability(
    model,
    x: pd.DataFrame,
) -> np.ndarray:
    classes = np.asarray(
        model.classes_
        if hasattr(
            model,
            "classes_",
        )
        else model.named_steps[
            "model"
        ].classes_
    )

    if 1 not in classes:
        raise RuntimeError(
            "Model contains no Over class"
        )

    over_index = int(
        np.where(
            classes
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
            "Model returned non-finite probability"
        )

    return np.clip(
        probability,
        EPS,
        1.0 - EPS,
    )


def tune_logistic(
    train_x: pd.DataFrame,
    train_y: pd.Series,
    validation_x: pd.DataFrame,
    validation_y: pd.Series,
    columns: list[str],
):
    best_c = None
    best_log_loss = np.inf
    best_brier = np.inf

    for c_value in (
        LOGISTIC_C_VALUES
    ):
        model = (
            make_logistic_model(
                columns,
                c_value,
            )
        )

        model.fit(
            train_x,
            train_y,
        )

        probability = (
            predict_probability(
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
            best_c = float(
                c_value
            )

            best_log_loss = ll
            best_brier = brier

    if best_c is None:
        raise RuntimeError(
            "Logistic regression failed parameter selection"
        )

    return (
        best_c,
        best_log_loss,
        best_brier,
    )


def fit_final_logistic(
    train_x: pd.DataFrame,
    validation_x: pd.DataFrame,
    train_y: pd.Series,
    validation_y: pd.Series,
    columns: list[str],
    c_value: float,
):
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

    model = (
        make_logistic_model(
            columns,
            c_value,
        )
    )

    model.fit(
        fit_x,
        fit_y,
    )

    return model


def make_hgb_model(
    params: dict,
):
    return HistGradientBoostingClassifier(
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


def tune_full_classifier(
    train_x: pd.DataFrame,
    train_y: pd.Series,
    validation_x: pd.DataFrame,
    validation_y: pd.Series,
):
    best_params = None
    best_log_loss = np.inf
    best_brier = np.inf

    candidate_count = 0

    for params in (
        TRAIN
        .hyperparameter_candidates()
    ):
        candidate_count += 1

        model = (
            make_hgb_model(
                params
            )
        )

        model.fit(
            train_x,
            train_y,
        )

        probability = (
            predict_probability(
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
            "Expected 81 HGB parameter combinations; "
            f"evaluated {candidate_count}"
        )

    if best_params is None:
        raise RuntimeError(
            "Full classifier failed parameter selection"
        )

    return (
        best_params,
        best_log_loss,
        best_brier,
    )


def fit_final_hgb(
    train_x: pd.DataFrame,
    validation_x: pd.DataFrame,
    train_y: pd.Series,
    validation_y: pd.Series,
    params: dict,
):
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

    model = (
        make_hgb_model(
            params
        )
    )

    model.fit(
        fit_x,
        fit_y,
    )

    return model


def build_candidate_predictions(
    fold_number: int,
    variant: str,
    test: pd.DataFrame,
    over_probability: np.ndarray,
):
    test = (
        test
        .reset_index(
            drop=True
        )
    )

    if len(test) != len(
        over_probability
    ):
        raise RuntimeError(
            f"{variant} prediction length mismatch"
        )

    rows = []

    for (
        index,
        row,
    ) in test.iterrows():
        p_over = float(
            over_probability[
                index
            ]
        )

        p_under = (
            1.0
            - p_over
        )

        actual_over = int(
            row[
                "_over_target"
            ]
        )

        common = {
            "fold":
                fold_number,
            "variant":
                variant,
            "game_date":
                pd.Timestamp(
                    row[
                        "_game_date_dt"
                    ]
                ).strftime(
                    "%Y-%m-%d"
                ),
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
                p_over,
        }

        rows.append(
            {
                **common,
                "side":
                    "over",
                "model_prob":
                    p_over,
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
                    p_under,
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


def build_buckets(
    predictions: pd.DataFrame,
    variant: str,
    fold_number,
):
    work = predictions[
        predictions[
            "variant"
        ]
        == variant
    ].copy()

    if fold_number != "ALL":
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

    buckets.insert(
        0,
        "fold",
        fold_number,
    )

    return buckets


def evaluate_variant(
    predictions: pd.DataFrame,
    variant: str,
    fold_number,
):
    work = predictions[
        predictions[
            "variant"
        ]
        == variant
    ].copy()

    if fold_number != "ALL":
        work = work[
            work[
                "fold"
            ]
            == fold_number
        ].copy()

    over_rows = work[
        work[
            "side"
        ]
        == "over"
    ].copy()

    if over_rows.empty:
        raise RuntimeError(
            f"No Over rows for {variant}"
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

    corr = (
        safe_corr(
            work[
                "model_prob"
            ],
            work[
                "win_binary"
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

    buckets = (
        build_buckets(
            predictions,
            variant,
            fold_number,
        )
    )

    rates = (
        buckets[
            "actual_win_rate"
        ]
        .to_numpy(
            dtype=float
        )
    )

    low_rate = float(
        rates[
            0
        ]
    )

    high_rate = float(
        rates[
            -1
        ]
    )

    rising_steps = int(
        np.sum(
            np.diff(
                rates
            )
            > 0
        )
    )

    return (
        {
            "fold":
                fold_number,
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
                corr,
            "over_auc":
                auc,
            "brier":
                brier,
            "log_loss":
                ll,
            "low_bucket_win_rate":
                low_rate,
            "high_bucket_win_rate":
                high_rate,
            "high_minus_low":
                (
                    high_rate
                    - low_rate
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
    side: str,
):
    value = (
        frame[
            "_game_date_dt"
        ].min()
        if side == "start"
        else frame[
            "_game_date_dt"
        ].max()
    )

    return pd.Timestamp(
        value
    ).strftime(
        "%Y-%m-%d"
    )


def write_report(
    summary: pd.DataFrame,
    folds: pd.DataFrame,
    buckets: pd.DataFrame,
    verdict: str,
):
    generated_at = (
        datetime.now(
            UTC
        ).isoformat()
    )

    SUMMARY_HTML = (
        summary
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
    )

    FOLDS_HTML = (
        folds
        .to_html(
            index=False,
            float_format=(
                lambda x:
                    f"{x:.6f}"
            ),
        )
    )

    BUCKETS_HTML = (
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
<title>MLB Simplified Totals Models</title>
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
<h1>MLB Simplified Totals Models</h1>
<p>Generated: {generated_at}</p>
<h2>{verdict}</h2>
<h2>Aggregate Results</h2>
{SUMMARY_HTML}
<h2>Fold Results</h2>
{FOLDS_HTML}
<h2>Probability Buckets</h2>
{BUCKETS_HTML}
</body>
</html>
""",
        encoding="utf-8",
    )


def main():
    (
        frame,
        production_features,
    ) = prepare_resolved_data()

    folds = make_folds(
        frame
    )

    all_predictions = []
    fold_rows = []
    bucket_frames = []

    variants = [
        "line_only",
        "line_dratings",
        "compact",
        "current_full",
    ]

    for fold_info in folds:
        fold_number = int(
            fold_info[
                "fold"
            ]
        )

        train = (
            fold_info[
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
            fold_info[
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
            fold_info[
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
            f"{date_text(train, 'start')} "
            f"through "
            f"{date_text(train, 'end')} "
            f"({len(train):,} games)"
        )

        print(
            f"Validation: "
            f"{date_text(validation, 'start')} "
            f"through "
            f"{date_text(validation, 'end')} "
            f"({len(validation):,} games)"
        )

        print(
            f"Test: "
            f"{date_text(test, 'start')} "
            f"through "
            f"{date_text(test, 'end')} "
            f"({len(test):,} games)"
        )

        (
            train_x,
            validation_x,
            test_x,
        ) = (
            build_full_feature_frames(
                train,
                validation,
                test,
                production_features,
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

        fold_predictions = []

        for variant in (
            "line_only",
            "line_dratings",
            "compact",
        ):
            columns = (
                MODEL_FEATURES[
                    variant
                ]
            )

            print(
                f"Tuning {variant}..."
            )

            (
                best_c,
                validation_ll,
                validation_brier,
            ) = tune_logistic(
                train_x,
                train_y,
                validation_x,
                validation_y,
                columns,
            )

            model = (
                fit_final_logistic(
                    train_x,
                    validation_x,
                    train_y,
                    validation_y,
                    columns,
                    best_c,
                )
            )

            probability = (
                predict_probability(
                    model,
                    test_x,
                )
            )

            prediction_frame = (
                build_candidate_predictions(
                    fold_number,
                    variant,
                    test,
                    probability,
                )
            )

            fold_predictions.append(
                prediction_frame
            )

            print(
                f"{variant}: "
                f"C={best_c} "
                f"validation_log_loss="
                f"{validation_ll:.6f}"
            )

        print(
            "Tuning current_full..."
        )

        (
            full_params,
            full_validation_ll,
            full_validation_brier,
        ) = tune_full_classifier(
            train_x,
            train_y,
            validation_x,
            validation_y,
        )

        full_model = (
            fit_final_hgb(
                train_x,
                validation_x,
                train_y,
                validation_y,
                full_params,
            )
        )

        full_probability = (
            predict_probability(
                full_model,
                test_x,
            )
        )

        full_predictions = (
            build_candidate_predictions(
                fold_number,
                "current_full",
                test,
                full_probability,
            )
        )

        fold_predictions.append(
            full_predictions
        )

        print(
            "current_full: "
            f"validation_log_loss="
            f"{full_validation_ll:.6f} "
            f"params={full_params}"
        )

        fold_predictions = pd.concat(
            fold_predictions,
            ignore_index=True,
            sort=False,
        )

        all_predictions.append(
            fold_predictions
        )

        for variant in variants:
            (
                metrics,
                buckets,
            ) = evaluate_variant(
                fold_predictions,
                variant,
                fold_number,
            )

            metrics[
                "train_start"
            ] = date_text(
                train,
                "start",
            )

            metrics[
                "train_end"
            ] = date_text(
                train,
                "end",
            )

            metrics[
                "validation_start"
            ] = date_text(
                validation,
                "start",
            )

            metrics[
                "validation_end"
            ] = date_text(
                validation,
                "end",
            )

            metrics[
                "test_start"
            ] = date_text(
                test,
                "start",
            )

            metrics[
                "test_end"
            ] = date_text(
                test,
                "end",
            )

            fold_rows.append(
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
        fold_rows
    )

    summary_rows = []

    for variant in variants:
        (
            metrics,
            buckets,
        ) = evaluate_variant(
            predictions,
            variant,
            "ALL",
        )

        variant_folds = (
            fold_metrics[
                fold_metrics[
                    "variant"
                ]
                == variant
            ]
        )

        metrics[
            "positive_corr_folds"
        ] = int(
            (
                variant_folds[
                    "probability_win_corr"
                ]
                > 0
            ).sum()
        )

        metrics[
            "positive_gap_folds"
        ] = int(
            (
                variant_folds[
                    "high_minus_low"
                ]
                > 0
            ).sum()
        )

        metrics[
            "mean_fold_corr"
        ] = float(
            variant_folds[
                "probability_win_corr"
            ].mean()
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

    current = (
        summary[
            summary[
                "variant"
            ]
            == "current_full"
        ]
        .iloc[0]
    )

    summary[
        "corr_change_vs_current"
    ] = (
        summary[
            "probability_win_corr"
        ]
        - float(
            current[
                "probability_win_corr"
            ]
        )
    )

    summary[
        "brier_change_vs_current"
    ] = (
        summary[
            "brier"
        ]
        - float(
            current[
                "brier"
            ]
        )
    )

    summary[
        "log_loss_change_vs_current"
    ] = (
        summary[
            "log_loss"
        ]
        - float(
            current[
                "log_loss"
            ]
        )
    )

    summary[
        "gap_change_vs_current"
    ] = (
        summary[
            "high_minus_low"
        ]
        - float(
            current[
                "high_minus_low"
            ]
        )
    )

    summary = (
        summary
        .sort_values(
            [
                "probability_win_corr",
                "positive_corr_folds",
                "brier",
            ],
            ascending=[
                False,
                False,
                True,
            ],
        )
        .reset_index(
            drop=True
        )
    )

    best = (
        summary.iloc[
            0
        ]
    )

    best_variant = str(
        best[
            "variant"
        ]
    )

    best_corr = float(
        best[
            "probability_win_corr"
        ]
    )

    best_positive_folds = int(
        best[
            "positive_corr_folds"
        ]
    )

    best_gap = float(
        best[
            "high_minus_low"
        ]
    )

    if (
        best_corr > 0
        and best_positive_folds >= 3
        and best_gap > 0
    ):
        verdict = (
            f"PROMISING TOTALS MODEL: "
            f"{best_variant}"
        )

    else:
        verdict = (
            "TOTALS STILL NOT SOLVED"
        )

    buckets = pd.concat(
        bucket_frames,
        ignore_index=True,
        sort=False,
    )

    summary.to_csv(
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
        summary,
        fold_metrics,
        buckets,
        verdict,
    )

    print()
    print(
        "AGGREGATE RESULTS"
    )

    print(
        summary[
            [
                "variant",
                "resolved_games",
                "probability_win_corr",
                "mean_fold_corr",
                "positive_corr_folds",
                "over_auc",
                "brier",
                "log_loss",
                "low_bucket_win_rate",
                "high_bucket_win_rate",
                "high_minus_low",
                "positive_gap_folds",
                "rising_bucket_steps",
            ]
        ]
        .to_string(
            index=False
        )
    )

    print()
    print(
        "FOLD RESULTS"
    )

    print(
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
            ]
        ]
        .sort_values(
            [
                "fold",
                "variant",
            ]
        )
        .to_string(
            index=False
        )
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