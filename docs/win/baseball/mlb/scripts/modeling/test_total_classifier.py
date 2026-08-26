#!/usr/bin/env python3
"""Leakage-safe direct MLB Over/Under classifier experiment.

Target:
    1 = game finished OVER sportsbook total
    0 = game finished UNDER sportsbook total

Pushes are excluded from classifier training and evaluation.

Classifier inputs:
    - exact current production pregame run-model features
    - sportsbook total line
    - total-specific engineered combinations
    - venue
    - day/night

Sportsbook odds/prices/juice are NEVER model features.

Comparison:
    current_sum = current home + away models + current totals probability formula
    classifier  = direct probability that the game finishes over the posted total

No production files or models are modified.
"""

from __future__ import annotations

import importlib.util
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

TOTAL_TEST_SCRIPT = (
    BASE
    / "scripts/modeling/test_total_model.py"
)

SUMMARY_FILE = (
    ROOT
    / "mlb_total_classifier_summary.csv"
)

BUCKET_FILE = (
    ROOT
    / "mlb_total_classifier_buckets.csv"
)

PREDICTIONS_FILE = (
    ROOT
    / "mlb_total_classifier_predictions.csv"
)

REPORT_FILE = (
    ROOT
    / "mlb_total_classifier_report.html"
)

EPS = 1e-12


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


TM = load_module(
    "total_classifier_base",
    TOTAL_TEST_SCRIPT,
)

TRAIN = TM.TRAIN
JUICE = TM.JUICE

TRAIN._log = (
    lambda *args, **kwargs: None
)


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


def attach_lines(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    out = TM.sportsbook_test_rows(
        frame
    )

    out[
        "_total_line"
    ] = pd.to_numeric(
        out[
            "total"
        ],
        errors="coerce",
    )

    out = out[
        out[
            "_total_line"
        ].notna()
        & np.isfinite(
            out[
                "_total_line"
            ]
        )
    ].copy()

    if out.empty:
        raise RuntimeError(
            "No usable sportsbook "
            "total lines"
        )

    out[
        "_actual_total"
    ] = (
        pd.to_numeric(
            out[
                "target_home_runs"
            ],
            errors="coerce",
        )
        + pd.to_numeric(
            out[
                "target_away_runs"
            ],
            errors="coerce",
        )
    )

    bad_actual = (
        out[
            "_actual_total"
        ].isna()
        | ~np.isfinite(
            out[
                "_actual_total"
            ]
        )
        | (
            out[
                "_actual_total"
            ]
            < 0
        )
    )

    if bad_actual.any():
        raise RuntimeError(
            "Invalid actual totals"
        )

    out[
        "_is_push"
    ] = (
        np.abs(
            out[
                "_actual_total"
            ]
            - out[
                "_total_line"
            ]
        )
        <= EPS
    )

    out[
        "_over_target"
    ] = (
        out[
            "_actual_total"
        ]
        > out[
            "_total_line"
        ]
    ).astype(
        int
    )

    return out


def venue_levels_from(
    frame: pd.DataFrame,
):
    venue = pd.to_numeric(
        frame[
            "venue_id"
        ],
        errors="coerce",
    )

    return sorted(
        {
            str(
                int(
                    value
                )
            )
            for value
            in venue.dropna().unique()
        }
    )


def classifier_feature_frame(
    frame: pd.DataFrame,
    production_features: list[str],
    venue_levels: list[str],
) -> pd.DataFrame:
    x = pd.DataFrame(
        index=frame.index
    )

    # Exact numeric production feature set.
    for col in production_features:
        x[
            col
        ] = numeric(
            frame,
            col,
        )

    # The posted total line is the market threshold being predicted.
    x[
        "sportsbook_total_line"
    ] = numeric(
        frame,
        "_total_line",
    )

    # Direct projection-vs-line context.
    x[
        "dratings_total_edge"
    ] = (
        numeric(
            frame,
            "dratings_total_projected_runs",
        )
        - x[
            "sportsbook_total_line"
        ]
    )

    x[
        "dratings_sum_edge"
    ] = (
        numeric(
            frame,
            "dratings_home_projected_runs",
        )
        + numeric(
            frame,
            "dratings_away_projected_runs",
        )
        - x[
            "sportsbook_total_line"
        ]
    )

    x[
        "dratings_scoring_balance"
    ] = (
        numeric(
            frame,
            "dratings_home_projected_runs",
        )
        - numeric(
            frame,
            "dratings_away_projected_runs",
        )
    ).abs()

    # Starting-pitcher combined run-environment signals.
    x[
        "sp_xera_sum"
    ] = (
        numeric(
            frame,
            "home_sp_xera",
        )
        + numeric(
            frame,
            "away_sp_xera",
        )
    )

    x[
        "sp_xera_30d_sum"
    ] = (
        numeric(
            frame,
            "home_sp_xera_30d",
        )
        + numeric(
            frame,
            "away_sp_xera_30d",
        )
    )

    x[
        "sp_xwoba_sum"
    ] = (
        numeric(
            frame,
            "home_sp_xwoba",
        )
        + numeric(
            frame,
            "away_sp_xwoba",
        )
    )

    x[
        "sp_xwoba_30d_sum"
    ] = (
        numeric(
            frame,
            "home_sp_xwoba_30d",
        )
        + numeric(
            frame,
            "away_sp_xwoba_30d",
        )
    )

    x[
        "sp_pitch_quality_sum"
    ] = (
        numeric(
            frame,
            "home_sp_pitch_quality_plus",
        )
        + numeric(
            frame,
            "away_sp_pitch_quality_plus",
        )
    )

    x[
        "sp_command_sum"
    ] = (
        numeric(
            frame,
            "home_sp_command_plus",
        )
        + numeric(
            frame,
            "away_sp_command_plus",
        )
    )

    x[
        "sp_pitches_sum"
    ] = (
        numeric(
            frame,
            "home_sp_pitches",
        )
        + numeric(
            frame,
            "away_sp_pitches",
        )
    )

    x[
        "sp_pitches_30d_sum"
    ] = (
        numeric(
            frame,
            "home_sp_pitches_30d",
        )
        + numeric(
            frame,
            "away_sp_pitches_30d",
        )
    )

    # Bullpen combined run-environment signals.
    x[
        "bp_woba_14d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_woba_allowed_14d",
        )
        + numeric(
            frame,
            "away_bp_woba_allowed_14d",
        )
    )

    x[
        "bp_woba_7d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_woba_allowed_7d",
        )
        + numeric(
            frame,
            "away_bp_woba_allowed_7d",
        )
    )

    x[
        "bp_k_14d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_k_rate_14d",
        )
        + numeric(
            frame,
            "away_bp_k_rate_14d",
        )
    )

    x[
        "bp_k_7d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_k_rate_7d",
        )
        + numeric(
            frame,
            "away_bp_k_rate_7d",
        )
    )

    x[
        "bp_bb_14d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_bb_rate_14d",
        )
        + numeric(
            frame,
            "away_bp_bb_rate_14d",
        )
    )

    x[
        "bp_bb_7d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_bb_rate_7d",
        )
        + numeric(
            frame,
            "away_bp_bb_rate_7d",
        )
    )

    x[
        "bp_hard_14d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_hard_rate_14d",
        )
        + numeric(
            frame,
            "away_bp_hard_rate_14d",
        )
    )

    x[
        "bp_hard_7d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_hard_rate_7d",
        )
        + numeric(
            frame,
            "away_bp_hard_rate_7d",
        )
    )

    x[
        "bp_pitches_3d_sum"
    ] = (
        numeric(
            frame,
            "home_bp_pitches_3d",
        )
        + numeric(
            frame,
            "away_bp_pitches_3d",
        )
    )

    # Day/night.
    day_night = (
        frame[
            "day_night"
        ]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    x[
        "is_night"
    ] = (
        day_night
        == "night"
    ).astype(
        float
    )

    # Venue is categorical, so one-hot encode instead of treating ID as magnitude.
    venue = (
        pd.to_numeric(
            frame[
                "venue_id"
            ],
            errors="coerce",
        )
        .round()
        .astype("Int64")
        .astype("string")
    )

    for level in venue_levels:
        x[
            f"venue_{level}"
        ] = (
            venue
            == level
        ).astype(
            float
        )

    return x


def tune_classifier(
    train_frame: pd.DataFrame,
    validation_frame: pd.DataFrame,
    feature_columns: list[str],
):
    best_params = None
    best_log_loss = np.inf
    best_brier = np.inf
    candidate_count = 0

    y_train = (
        train_frame[
            "_over_target"
        ]
        .astype(int)
    )

    y_validation = (
        validation_frame[
            "_over_target"
        ]
        .astype(int)
    )

    for params in (
        TRAIN.hyperparameter_candidates()
    ):
        candidate_count += 1

        model = (
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
                random_state=TRAIN.RANDOM_STATE,
            )
        )

        model.fit(
            train_frame[
                feature_columns
            ],
            y_train,
        )

        if 1 not in model.classes_:
            raise RuntimeError(
                "Classifier training "
                "contains no Over class"
            )

        over_index = int(
            np.where(
                model.classes_
                == 1
            )[0][0]
        )

        probability = (
            model.predict_proba(
                validation_frame[
                    feature_columns
                ]
            )[
                :,
                over_index
            ]
        )

        probability = np.clip(
            probability,
            EPS,
            1.0 - EPS,
        )

        score = float(
            log_loss(
                y_validation,
                probability,
                labels=[
                    0,
                    1,
                ],
            )
        )

        brier = float(
            brier_score_loss(
                y_validation,
                probability,
            )
        )

        if (
            score
            < best_log_loss
            - 1e-12
            or (
                abs(
                    score
                    - best_log_loss
                )
                <= 1e-12
                and brier
                < best_brier
            )
        ):
            best_log_loss = score
            best_brier = brier
            best_params = dict(
                params
            )

    if candidate_count != 81:
        raise RuntimeError(
            "Classifier grid expected "
            f"81 candidates; evaluated "
            f"{candidate_count}"
        )

    if best_params is None:
        raise RuntimeError(
            "Classifier failed to select "
            "hyperparameters"
        )

    return (
        best_params,
        best_log_loss,
        best_brier,
    )


def fit_classifier(
    train_frame: pd.DataFrame,
    validation_frame: pd.DataFrame,
    feature_columns: list[str],
    params: dict,
):
    fit_frame = pd.concat(
        [
            train_frame,
            validation_frame,
        ],
        ignore_index=True,
        sort=False,
    )

    model = (
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
            random_state=TRAIN.RANDOM_STATE,
        )
    )

    model.fit(
        fit_frame[
            feature_columns
        ],
        fit_frame[
            "_over_target"
        ].astype(
            int
        ),
    )

    return model


def probability_over(
    model,
    frame: pd.DataFrame,
    feature_columns: list[str],
):
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
        model.predict_proba(
            frame[
                feature_columns
            ]
        )[
            :,
            over_index
        ]
    )

    return np.clip(
        probability,
        EPS,
        1.0 - EPS,
    )


def add_current_predictions(
    test: pd.DataFrame,
    home_model,
    away_model,
    production_features: list[str],
):
    out = test.copy()

    out[
        "_current_home_runs"
    ] = home_model.predict(
        out[
            production_features
        ]
    )

    out[
        "_current_away_runs"
    ] = away_model.predict(
        out[
            production_features
        ]
    )

    for col in (
        "_current_home_runs",
        "_current_away_runs",
    ):
        values = pd.to_numeric(
            out[
                col
            ],
            errors="coerce",
        )

        bad = (
            values.isna()
            | ~np.isfinite(
                values
            )
            | (
                values
                < 0
            )
        )

        if bad.any():
            raise RuntimeError(
                "Current model produced "
                f"invalid values in {col}"
            )

    return out


def build_predictions(
    test: pd.DataFrame,
    classifier_over_probability,
):
    rows = []

    if (
        len(
            classifier_over_probability
        )
        != len(
            test
        )
    ):
        raise RuntimeError(
            "Classifier probability length "
            "does not match test rows"
        )

    for position, (_, row) in enumerate(
        test.iterrows()
    ):
        line = float(
            row[
                "_total_line"
            ]
        )

        actual_total = float(
            row[
                "_actual_total"
            ]
        )

        current_home = float(
            row[
                "_current_home_runs"
            ]
        )

        current_away = float(
            row[
                "_current_away_runs"
            ]
        )

        (
            current_over_raw,
            current_under_raw,
            current_push,
        ) = (
            JUICE.totals_probabilities(
                current_home,
                current_away,
                line,
            )
        )

        resolved_mass = (
            current_over_raw
            + current_under_raw
        )

        if (
            not np.isfinite(
                resolved_mass
            )
            or resolved_mass
            <= 0
        ):
            raise RuntimeError(
                "Current totals formula "
                "produced invalid resolved mass"
            )

        current_over = float(
            current_over_raw
            / resolved_mass
        )

        classifier_over = float(
            classifier_over_probability[
                position
            ]
        )

        actual_over = int(
            actual_total
            > line
        )

        variants = {
            "current_sum":
                current_over,

            "classifier":
                classifier_over,
        }

        for (
            variant,
            p_over,
        ) in variants.items():
            p_over = float(
                np.clip(
                    p_over,
                    EPS,
                    1.0 - EPS,
                )
            )

            for (
                side,
                model_prob,
                win_binary,
            ) in (
                (
                    "over",
                    p_over,
                    float(
                        actual_over
                    ),
                ),
                (
                    "under",
                    1.0 - p_over,
                    float(
                        1
                        - actual_over
                    ),
                ),
            ):
                rows.append(
                    {
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

                        "side":
                            side,

                        "total_line":
                            line,

                        "actual_total":
                            actual_total,

                        "current_home_runs":
                            current_home,

                        "current_away_runs":
                            current_away,

                        "current_sum_total":
                            (
                                current_home
                                + current_away
                            ),

                        "model_prob":
                            model_prob,

                        "win_binary":
                            win_binary,

                        "current_raw_push_prob":
                            float(
                                current_push
                            )
                            if variant
                            == "current_sum"
                            else 0.0,
                    }
                )

    return pd.DataFrame(
        rows
    )


def correlation(
    x,
    y,
):
    x = np.asarray(
        x,
        dtype=float,
    )

    y = np.asarray(
        y,
        dtype=float,
    )

    mask = (
        np.isfinite(
            x
        )
        & np.isfinite(
            y
        )
    )

    if (
        int(
            mask.sum()
        )
        < 2
    ):
        return np.nan

    x = x[
        mask
    ]

    y = y[
        mask
    ]

    if (
        np.std(
            x
        )
        <= 0
        or np.std(
            y
        )
        <= 0
    ):
        return np.nan

    return float(
        np.corrcoef(
            x,
            y,
        )[0, 1]
    )


def build_buckets(
    predictions: pd.DataFrame,
):
    rows = []

    for (
        variant,
        part,
    ) in predictions.groupby(
        "variant",
        sort=False,
    ):
        part = part.copy()

        part[
            "bucket"
        ] = pd.qcut(
            part[
                "model_prob"
            ],
            q=5,
            labels=False,
            duplicates="drop",
        )

        part = part.dropna(
            subset=[
                "bucket"
            ]
        )

        part[
            "bucket"
        ] = (
            part[
                "bucket"
            ]
            .astype(int)
            + 1
        )

        for (
            bucket,
            group,
        ) in part.groupby(
            "bucket",
            sort=True,
        ):
            rows.append(
                {
                    "variant":
                        variant,

                    "bucket":
                        int(
                            bucket
                        ),

                    "n":
                        int(
                            len(
                                group
                            )
                        ),

                    "prob_min":
                        float(
                            group[
                                "model_prob"
                            ].min()
                        ),

                    "prob_mean":
                        float(
                            group[
                                "model_prob"
                            ].mean()
                        ),

                    "prob_max":
                        float(
                            group[
                                "model_prob"
                            ].max()
                        ),

                    "actual_win_rate":
                        float(
                            group[
                                "win_binary"
                            ].mean()
                        ),
                }
            )

    return pd.DataFrame(
        rows
    )


def build_summary(
    predictions: pd.DataFrame,
    buckets: pd.DataFrame,
):
    rows = []

    for variant in (
        "current_sum",
        "classifier",
    ):
        part = predictions[
            predictions[
                "variant"
            ]
            == variant
        ].copy()

        bucket_part = (
            buckets[
                buckets[
                    "variant"
                ]
                == variant
            ]
            .sort_values(
                "bucket"
            )
        )

        if (
            part.empty
            or bucket_part.empty
        ):
            raise RuntimeError(
                f"Missing evaluation "
                f"data for {variant}"
            )

        y = (
            part[
                "win_binary"
            ]
            .to_numpy(
                dtype=float
            )
        )

        p = np.clip(
            part[
                "model_prob"
            ]
            .to_numpy(
                dtype=float
            ),
            EPS,
            1.0 - EPS,
        )

        win_rates = (
            bucket_part[
                "actual_win_rate"
            ]
            .to_numpy(
                dtype=float
            )
        )

        game_over = (
            part[
                part[
                    "side"
                ]
                == "over"
            ]
            .copy()
        )

        game_y = (
            game_over[
                "win_binary"
            ]
            .to_numpy(
                dtype=float
            )
        )

        game_p = (
            game_over[
                "model_prob"
            ]
            .to_numpy(
                dtype=float
            )
        )

        if (
            len(
                np.unique(
                    game_y
                )
            )
            == 2
        ):
            auc = float(
                roc_auc_score(
                    game_y,
                    game_p,
                )
            )
        else:
            auc = np.nan

        rows.append(
            {
                "variant":
                    variant,

                "resolved_games":
                    int(
                        len(
                            game_over
                        )
                    ),

                "resolved_candidates":
                    int(
                        len(
                            part
                        )
                    ),

                "probability_win_corr":
                    correlation(
                        p,
                        y,
                    ),

                "over_auc":
                    auc,

                "brier_score":
                    float(
                        brier_score_loss(
                            y,
                            p,
                        )
                    ),

                "log_loss":
                    float(
                        log_loss(
                            y,
                            p,
                            labels=[
                                0.0,
                                1.0,
                            ],
                        )
                    ),

                "low_probability_bucket_win_rate":
                    float(
                        win_rates[
                            0
                        ]
                    ),

                "high_probability_bucket_win_rate":
                    float(
                        win_rates[
                            -1
                        ]
                    ),

                "high_minus_low_win_rate":
                    float(
                        win_rates[
                            -1
                        ]
                        - win_rates[
                            0
                        ]
                    ),

                "rising_bucket_steps_out_of_4":
                    int(
                        np.sum(
                            np.diff(
                                win_rates
                            )
                            > 0
                        )
                    ),

                "mean_model_probability":
                    float(
                        p.mean()
                    ),

                "mean_actual_win_rate":
                    float(
                        y.mean()
                    ),
            }
        )

    return pd.DataFrame(
        rows
    )


def write_report(
    summary,
    buckets,
    predictions,
):
    REPORT_FILE.write_text(
        f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Total Classifier Test</title>
<style>
body {{
    font-family: Segoe UI, Arial, sans-serif;
    margin: 24px;
}}
table {{
    border-collapse: collapse;
    font-size: 12px;
    margin-bottom: 24px;
}}
th, td {{
    border: 1px solid #ddd;
    padding: 6px 8px;
    text-align: right;
}}
th {{
    background: #f2f2f2;
}}
</style>
</head>
<body>

<h1>MLB Direct Over/Under Classifier Test</h1>

<p>
Generated:
{datetime.now(UTC).isoformat()}
</p>

<p>
Classifier predicts Over vs Under directly.
Pushes are excluded.
The posted sportsbook total line is a feature.
Sportsbook prices and odds are not features.
</p>

<h2>Summary</h2>

{summary.to_html(
    index=False,
    border=0,
)}

<h2>Probability buckets</h2>

{buckets.to_html(
    index=False,
    border=0,
)}

<h2>Predictions</h2>

{predictions.to_html(
    index=False,
    border=0,
)}

</body>
</html>
""",
        encoding="utf-8",
    )


def main():
    (
        training,
        production_features,
    ) = TM.load_training()

    split = (
        TRAIN
        .chronological_date_split(
            training
        )
    )

    print(
        "Main split: "
        f"train={len(split['train']):,}, "
        f"validation="
        f"{len(split['validation']):,}, "
        f"test={len(split['test']):,}"
    )

    print(
        "Joining historical total lines..."
    )

    classifier_train = attach_lines(
        split[
            "train"
        ]
    )

    classifier_validation = attach_lines(
        split[
            "validation"
        ]
    )

    classifier_test = attach_lines(
        split[
            "test"
        ]
    )

    classifier_train = classifier_train[
        ~classifier_train[
            "_is_push"
        ]
    ].copy()

    classifier_validation = (
        classifier_validation[
            ~classifier_validation[
                "_is_push"
            ]
        ]
        .copy()
    )

    classifier_test = classifier_test[
        ~classifier_test[
            "_is_push"
        ]
    ].copy()

    if (
        classifier_train.empty
        or classifier_validation.empty
        or classifier_test.empty
    ):
        raise RuntimeError(
            "Push removal produced "
            "an empty classifier partition"
        )

    print(
        "Resolved classifier rows: "
        f"train={len(classifier_train):,}, "
        f"validation="
        f"{len(classifier_validation):,}, "
        f"test={len(classifier_test):,}"
    )

    print(
        "Over rates: "
        f"train="
        f"{classifier_train['_over_target'].mean():.3f}, "
        f"validation="
        f"{classifier_validation['_over_target'].mean():.3f}, "
        f"test="
        f"{classifier_test['_over_target'].mean():.3f}"
    )

    venue_levels = venue_levels_from(
        pd.concat(
            [
                classifier_train,
                classifier_validation,
            ],
            ignore_index=True,
            sort=False,
        )
    )

    train_x = classifier_feature_frame(
        classifier_train,
        production_features,
        venue_levels,
    )

    validation_x = classifier_feature_frame(
        classifier_validation,
        production_features,
        venue_levels,
    )

    test_x = classifier_feature_frame(
        classifier_test,
        production_features,
        venue_levels,
    )

    classifier_train = pd.concat(
        [
            classifier_train.reset_index(
                drop=True
            ),
            train_x.add_prefix(
                "_clf_"
            ).reset_index(
                drop=True
            ),
        ],
        axis=1,
    )

    classifier_validation = pd.concat(
        [
            classifier_validation.reset_index(
                drop=True
            ),
            validation_x.add_prefix(
                "_clf_"
            ).reset_index(
                drop=True
            ),
        ],
        axis=1,
    )

    classifier_test = pd.concat(
        [
            classifier_test.reset_index(
                drop=True
            ),
            test_x.add_prefix(
                "_clf_"
            ).reset_index(
                drop=True
            ),
        ],
        axis=1,
    )

    classifier_features = [
        f"_clf_{col}"
        for col in train_x.columns
    ]

    if (
        classifier_features
        != [
            f"_clf_{col}"
            for col
            in validation_x.columns
        ]
        or classifier_features
        != [
            f"_clf_{col}"
            for col
            in test_x.columns
        ]
    ):
        raise RuntimeError(
            "Classifier feature contract "
            "differs across partitions"
        )

    print(
        f"Classifier features: "
        f"{len(classifier_features)}"
    )

    print(
        "Tuning direct Over/Under "
        "classifier once..."
    )

    (
        classifier_params,
        validation_log_loss,
        validation_brier,
    ) = tune_classifier(
        classifier_train,
        classifier_validation,
        classifier_features,
    )

    print(
        "classifier: "
        f"validation_log_loss="
        f"{validation_log_loss:.6f} "
        f"validation_brier="
        f"{validation_brier:.6f} "
        f"params={classifier_params}"
    )

    classifier = fit_classifier(
        classifier_train,
        classifier_validation,
        classifier_features,
        classifier_params,
    )

    print(
        "Tuning current home model once..."
    )

    current_home = TM.fit_one(
        split[
            "train"
        ],
        split[
            "validation"
        ],
        production_features,
        "target_home_runs",
        "home_runs",
    )

    print(
        "Tuning current away model once..."
    )

    current_away = TM.fit_one(
        split[
            "train"
        ],
        split[
            "validation"
        ],
        production_features,
        "target_away_runs",
        "away_runs",
    )

    classifier_test = (
        add_current_predictions(
            classifier_test,
            current_home,
            current_away,
            production_features,
        )
    )

    classifier_probability = (
        probability_over(
            classifier,
            classifier_test,
            classifier_features,
        )
    )

    predictions = build_predictions(
        classifier_test,
        classifier_probability,
    )

    buckets = build_buckets(
        predictions
    )

    summary = build_summary(
        predictions,
        buckets,
    )

    summary.to_csv(
        SUMMARY_FILE,
        index=False,
    )

    buckets.to_csv(
        BUCKET_FILE,
        index=False,
    )

    predictions.to_csv(
        PREDICTIONS_FILE,
        index=False,
    )

    write_report(
        summary,
        buckets,
        predictions,
    )

    current = (
        summary[
            summary[
                "variant"
            ]
            == "current_sum"
        ]
        .iloc[0]
    )

    challenger = (
        summary[
            summary[
                "variant"
            ]
            == "classifier"
        ]
        .iloc[0]
    )

    checks = {
        "higher_probability_win_correlation":
            (
                challenger[
                    "probability_win_corr"
                ]
                > current[
                    "probability_win_corr"
                ]
            ),

        "higher_AUC":
            (
                challenger[
                    "over_auc"
                ]
                > current[
                    "over_auc"
                ]
            ),

        "lower_Brier":
            (
                challenger[
                    "brier_score"
                ]
                < current[
                    "brier_score"
                ]
            ),

        "lower_log_loss":
            (
                challenger[
                    "log_loss"
                ]
                < current[
                    "log_loss"
                ]
            ),

        "better_high_minus_low":
            (
                challenger[
                    "high_minus_low_win_rate"
                ]
                > current[
                    "high_minus_low_win_rate"
                ]
            ),
    }

    wins = int(
        sum(
            bool(
                value
            )
            for value
            in checks.values()
        )
    )

    print(
        "\nTOTAL CLASSIFIER TEST COMPLETE"
    )

    print(
        summary.to_string(
            index=False
        )
    )

    print(
        "\nCLASSIFIER VS CURRENT:"
    )

    for (
        name,
        passed,
    ) in checks.items():
        print(
            f"  {name}: "
            f"{'BETTER' if passed else 'WORSE'}"
        )

    print(
        f"\nClassifier improved "
        f"{wins}/5 primary metrics."
    )

    print(
        f"\nSUMMARY: "
        f"{SUMMARY_FILE}"
    )

    print(
        f"BUCKETS: "
        f"{BUCKET_FILE}"
    )

    print(
        f"PREDICTIONS: "
        f"{PREDICTIONS_FILE}"
    )

    print(
        f"REPORT: "
        f"{REPORT_FILE}"
    )


if __name__ == "__main__":
    main()