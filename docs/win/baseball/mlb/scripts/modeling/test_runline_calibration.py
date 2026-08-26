#!/usr/bin/env python3
"""Leakage-safe run-line probability calibration experiment.

Experiment only; production is not modified.

The committed mlb_run_training_set.csv predates the bullpen production update.
This script reads that actual committed base training data, attaches the exact
production 14-day + 3-day workload + 7-day bullpen features from the existing
moneyline Statcast cache, then replays the current run models and current
run_line_probabilities() formula walk-forward.

Calibration is fit on earlier dates only. The final 30% of dates are untouched
out-of-sample evaluation data.
"""

from __future__ import annotations

import importlib.util
import math
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import brier_score_loss, log_loss


def repo_root() -> Path:
    for start in (Path.cwd().resolve(), Path(__file__).resolve().parent):
        for candidate in (start, *start.parents):
            if (candidate / "docs/win/baseball/mlb").exists():
                return candidate
    raise RuntimeError("Could not locate repository root")


ROOT = repo_root()
BASE = ROOT / "docs/win/baseball/mlb"
TRAINING_FILE = BASE / "modeling/data/mlb_run_training_set.csv"
BACKTEST_SCRIPT = BASE / "scripts/modeling/backtest_new_pipeline_correlations.py"
BULLPEN_SCRIPT = BASE / "scripts/modeling/test_moneyline_bullpen_detail.py"

SUMMARY_FILE = ROOT / "mlb_runline_calibration_summary.csv"
BUCKET_FILE = ROOT / "mlb_runline_calibration_buckets.csv"
PREDICTIONS_FILE = ROOT / "mlb_runline_calibration_predictions.csv"
REPORT_FILE = ROOT / "mlb_runline_calibration_report.html"

EVALUATION_FRACTION = 0.30
MIN_CALIBRATION_DATES = 45
EPS = 1e-6


def load_module(name: str, path: Path):
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location(name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


BT = load_module(
    "runline_calibration_backtest",
    BACKTEST_SCRIPT,
)

BP = load_module(
    "runline_calibration_bullpen",
    BULLPEN_SCRIPT,
)

TRAIN = BT.TRAIN
JUICE = BT.JUICE

TRAIN._log = lambda *args, **kwargs: None


PRODUCTION_BULLPEN_FEATURES = list(
    dict.fromkeys(
        [
            *BP.CURRENT,
            *BP.PLUS_7D,
        ]
    )
)


def validate_feature_contract() -> None:
    trainer_bp = [
        col
        for col in TRAIN.CORE_FEATURE_COLUMNS
        if "_bp_" in col
    ]

    if set(trainer_bp) != set(PRODUCTION_BULLPEN_FEATURES):
        raise RuntimeError(
            "Production trainer bullpen feature contract does not match "
            "the proven bullpen_plus_7d builder. "
            f"trainer_only="
            f"{sorted(set(trainer_bp) - set(PRODUCTION_BULLPEN_FEATURES))}; "
            f"builder_only="
            f"{sorted(set(PRODUCTION_BULLPEN_FEATURES) - set(trainer_bp))}"
        )


def load_current_training() -> tuple[pd.DataFrame, list[str]]:
    if not TRAINING_FILE.exists():
        raise FileNotFoundError(TRAINING_FILE)

    frame = pd.read_csv(
        TRAINING_FILE,
        encoding="utf-8-sig",
    )

    if frame.empty:
        raise RuntimeError(
            f"Training set is empty: {TRAINING_FILE}"
        )

    validate_feature_contract()

    base_core = [
        col
        for col in TRAIN.CORE_FEATURE_COLUMNS
        if col not in PRODUCTION_BULLPEN_FEATURES
    ]

    base_features = list(base_core)

    base_features.extend(
        col
        for col in TRAIN.OPTIONAL_NUMERIC_FEATURE_COLUMNS
        if col in frame.columns
    )

    required = (
        list(TRAIN.AUDIT_COLUMNS)
        + list(TRAIN.TARGET_COLUMNS)
        + base_core
    )

    missing = [
        col
        for col in required
        if col not in frame.columns
    ]

    if missing:
        raise RuntimeError(
            "Committed base training set missing actual required columns: "
            f"{missing}"
        )

    frame = TRAIN.coerce_and_validate_training_data(
        frame,
        base_features,
    )

    frame["game_id"] = (
        frame["game_id"]
        .astype("string")
        .str.strip()
    )

    frame["_gamePk"] = frame["gamePk"].map(
        BT.gamepk
    )

    frame["_home_code"] = frame["home_team"].map(
        BP.EXP.canonical_team
    )

    frame["_away_code"] = frame["away_team"].map(
        BP.EXP.canonical_team
    )

    if (
        frame["game_id"].isna().any()
        or (frame["game_id"] == "").any()
    ):
        raise RuntimeError(
            "Training data contains blank game_id"
        )

    if frame["game_id"].duplicated().any():
        raise RuntimeError(
            "Training data contains duplicate game_id"
        )

    if frame["_gamePk"].isna().any():
        raise RuntimeError(
            "Training data contains missing gamePk"
        )

    bad_teams = frame[
        frame["_home_code"].isna()
        | frame["_away_code"].isna()
    ]

    if not bad_teams.empty:
        sample = (
            bad_teams[
                [
                    "home_team",
                    "away_team",
                ]
            ]
            .drop_duplicates()
            .head(20)
            .to_dict("records")
        )

        raise RuntimeError(
            f"Could not map training teams: {sample}"
        )

    print(
        "Loading existing moneyline Statcast cache..."
    )

    raw_statcast = BP.load_cache()

    if raw_statcast.empty:
        raise RuntimeError(
            "Moneyline Statcast cache is empty"
        )

    cache_max = pd.to_datetime(
        raw_statcast["_game_date_dt"],
        errors="coerce",
    ).max()

    required_through = (
        frame["_game_date_dt"].max()
        - pd.Timedelta(days=1)
    )

    if (
        pd.isna(cache_max)
        or cache_max < required_through
    ):
        raise RuntimeError(
            "Existing Statcast cache does not cover "
            "the required historical period. "
            f"cache_max={cache_max}; "
            f"required_through={required_through}"
        )

    print(
        "Building exact 14-day + 3-day + 7-day bullpen features..."
    )

    team_daily, pitcher_daily = BP.build_tables(
        raw_statcast
    )

    frame = BP.attach_features(
        frame,
        team_daily,
        pitcher_daily,
    )

    missing_bp = [
        col
        for col in PRODUCTION_BULLPEN_FEATURES
        if col not in frame.columns
    ]

    if missing_bp:
        raise RuntimeError(
            "Bullpen builder failed to create: "
            f"{missing_bp}"
        )

    features = TRAIN.determine_feature_columns(
        frame
    )

    expected_order = [
        col
        for col in TRAIN.CORE_FEATURE_COLUMNS
        if "_bp_" in col
    ]

    actual_order = [
        col
        for col in features
        if "_bp_" in col
    ]

    if actual_order != expected_order:
        raise RuntimeError(
            "Bullpen feature order differs from production: "
            f"actual={actual_order}; "
            f"expected={expected_order}"
        )

    frame = TRAIN.coerce_and_validate_training_data(
        frame,
        features,
    )

    frame = (
        frame.sort_values(
            [
                "_game_date_dt",
                "game_id",
            ]
        )
        .reset_index(drop=True)
    )

    print(
        f"Training data ready: "
        f"{len(frame):,} rows, "
        f"{frame['_game_date_dt'].nunique():,} dates, "
        f"{len(features)} features"
    )

    return frame, features


def build_runline_predictions() -> pd.DataFrame:
    training, features = load_current_training()

    books = BT.sportsbook_files()

    dates = sorted(
        set(
            training["_game_date_dt"]
        ).intersection(
            books
        )
    )

    if not dates:
        raise RuntimeError(
            "No overlapping training/sportsbook dates"
        )

    rows: list[dict] = []
    skipped: list[str] = []

    for index, target_date in enumerate(
        dates,
        1,
    ):
        label = pd.Timestamp(
            target_date
        ).strftime(
            "%Y-%m-%d"
        )

        prior = training[
            training["_game_date_dt"]
            < target_date
        ]

        if (
            prior["_game_date_dt"].nunique()
            < 3
        ):
            skipped.append(
                label
            )

            continue

        print(
            f"BACKTEST "
            f"{index}/{len(dates)} "
            f"{label}"
        )

        try:
            target = training[
                training["_game_date_dt"]
                == target_date
            ].copy()

            (
                home_model,
                away_model,
                _,
            ) = BT.train_models(
                training,
                target_date,
                features,
            )

            target[
                "model_home_runs"
            ] = home_model.predict(
                target[features]
            )

            target[
                "model_away_runs"
            ] = away_model.predict(
                target[features]
            )

            for col in (
                "model_home_runs",
                "model_away_runs",
            ):
                values = pd.to_numeric(
                    target[col],
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
                        f"Invalid historical "
                        f"{col} predictions"
                    )

            book = BT.load_sportsbook(
                books[target_date]
            )

            joined = target.merge(
                book,
                on="game_id",
                how="inner",
                suffixes=(
                    "_train",
                    "_book",
                ),
                validate="one_to_one",
            )

            for _, row in joined.iterrows():
                home_line = BT.f(
                    row.get(
                        "home_run_line"
                    )
                )

                away_line = BT.f(
                    row.get(
                        "away_run_line"
                    )
                )

                if (
                    home_line is None
                    or away_line is None
                ):
                    continue

                mh = float(
                    row[
                        "model_home_runs"
                    ]
                )

                ma = float(
                    row[
                        "model_away_runs"
                    ]
                )

                hs = float(
                    row[
                        "target_home_runs"
                    ]
                )

                aws = float(
                    row[
                        "target_away_runs"
                    ]
                )

                (
                    home_prob,
                    away_prob,
                ) = JUICE.run_line_probabilities(
                    mh,
                    ma,
                    home_line,
                    away_line,
                )

                for (
                    side,
                    line,
                    raw_prob,
                ) in (
                    (
                        "home",
                        home_line,
                        home_prob,
                    ),
                    (
                        "away",
                        away_line,
                        away_prob,
                    ),
                ):
                    raw_prob = float(
                        raw_prob
                    )

                    if (
                        not np.isfinite(
                            raw_prob
                        )
                        or not (
                            0.0
                            < raw_prob
                            < 1.0
                        )
                    ):
                        raise RuntimeError(
                            "Invalid run-line "
                            f"probability {raw_prob} "
                            f"for game_id="
                            f"{row['game_id']} "
                            f"side={side}"
                        )

                    result = BT.grade_run_line(
                        side,
                        line,
                        hs,
                        aws,
                    )

                    if result not in {
                        "Win",
                        "Loss",
                    }:
                        continue

                    rows.append(
                        {
                            "game_date":
                                label,
                            "game_id":
                                str(
                                    row[
                                        "game_id"
                                    ]
                                ),
                            "home_team":
                                row.get(
                                    "home_team_train"
                                ),
                            "away_team":
                                row.get(
                                    "away_team_train"
                                ),
                            "side":
                                side,
                            "run_line":
                                float(
                                    line
                                ),
                            "model_home_runs":
                                mh,
                            "model_away_runs":
                                ma,
                            "raw_prob":
                                raw_prob,
                            "win_binary":
                                (
                                    1.0
                                    if result
                                    == "Win"
                                    else 0.0
                                ),
                        }
                    )

        except Exception as exc:
            print(
                f"  SKIPPED: {exc}"
            )

            skipped.append(
                label
            )

    if not rows:
        raise RuntimeError(
            "No resolved run-line "
            "predictions were produced"
        )

    frame = pd.DataFrame(
        rows
    )

    frame["_date"] = pd.to_datetime(
        frame["game_date"],
        errors="coerce",
    ).dt.normalize()

    frame = frame.dropna(
        subset=[
            "_date",
            "raw_prob",
            "win_binary",
        ]
    )

    frame = (
        frame.sort_values(
            [
                "_date",
                "game_id",
                "side",
            ]
        )
        .reset_index(drop=True)
    )

    print(
        f"Run-line replay complete: "
        f"{len(frame):,} resolved rows, "
        f"{frame['_date'].nunique():,} dates, "
        f"{len(set(skipped))} skipped dates"
    )

    return frame


def split_calibration_data(
    frame: pd.DataFrame,
):
    dates = [
        pd.Timestamp(
            value
        ).normalize()
        for value in sorted(
            frame["_date"].unique()
        )
    ]

    eval_count = max(
        1,
        int(
            math.ceil(
                len(dates)
                * EVALUATION_FRACTION
            )
        ),
    )

    fit_dates = dates[
        :-eval_count
    ]

    eval_dates = dates[
        -eval_count:
    ]

    if (
        len(fit_dates)
        < MIN_CALIBRATION_DATES
    ):
        raise RuntimeError(
            f"Need at least "
            f"{MIN_CALIBRATION_DATES} "
            f"calibration-fit dates; "
            f"found {len(fit_dates)}"
        )

    fit = frame[
        frame["_date"].isin(
            fit_dates
        )
    ].copy()

    evaluation = frame[
        frame["_date"].isin(
            eval_dates
        )
    ].copy()

    if (
        fit.empty
        or evaluation.empty
    ):
        raise RuntimeError(
            "Calibration split produced "
            "an empty partition"
        )

    return (
        fit,
        evaluation,
        fit_dates,
        eval_dates,
    )


def clip_prob(
    values,
) -> np.ndarray:
    return np.clip(
        np.asarray(
            values,
            dtype=float,
        ),
        EPS,
        1.0 - EPS,
    )


def logit(
    values,
) -> np.ndarray:
    p = clip_prob(
        values
    )

    return np.log(
        p
        / (
            1.0
            - p
        )
    )


def fit_logistic_calibrator(
    fit: pd.DataFrame,
) -> tuple[float, float]:
    x = logit(
        fit["raw_prob"]
    )

    y = fit[
        "win_binary"
    ].to_numpy(
        dtype=float
    )

    def objective(
        theta,
    ) -> float:
        intercept = float(
            theta[0]
        )

        slope = float(
            theta[1]
        )

        p = clip_prob(
            expit(
                intercept
                + slope
                * x
            )
        )

        return float(
            -np.sum(
                y
                * np.log(
                    p
                )
                + (
                    1.0
                    - y
                )
                * np.log(
                    1.0
                    - p
                )
            )
        )

    result = minimize(
        objective,
        np.array(
            [
                0.0,
                1.0,
            ]
        ),
        method="L-BFGS-B",
        bounds=[
            (
                None,
                None,
            ),
            (
                1e-8,
                None,
            ),
        ],
    )

    if not result.success:
        raise RuntimeError(
            "Logistic calibration failed: "
            f"{result.message}"
        )

    intercept = float(
        result.x[0]
    )

    slope = float(
        result.x[1]
    )

    if slope <= 0:
        raise RuntimeError(
            "Logistic calibration slope "
            f"is not positive: {slope}"
        )

    return (
        intercept,
        slope,
    )


def apply_logistic(
    raw_prob,
    intercept: float,
    slope: float,
) -> np.ndarray:
    return clip_prob(
        expit(
            intercept
            + slope
            * logit(
                raw_prob
            )
        )
    )


def count_rank_reversals(
    raw_prob,
    calibrated_prob,
) -> int:
    raw = np.asarray(
        raw_prob,
        dtype=float,
    )

    calibrated = np.asarray(
        calibrated_prob,
        dtype=float,
    )

    order = np.argsort(
        raw,
        kind="mergesort",
    )

    return int(
        np.sum(
            np.diff(
                calibrated[
                    order
                ]
            )
            < -1e-12
        )
    )


def probability_metrics(
    y_true,
    probabilities,
) -> dict[str, float]:
    y = np.asarray(
        y_true,
        dtype=float,
    )

    p = clip_prob(
        probabilities
    )

    return {
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
        "mean_probability":
            float(
                np.mean(
                    p
                )
            ),
        "actual_win_rate":
            float(
                np.mean(
                    y
                )
            ),
    }


def build_buckets(
    evaluation: pd.DataFrame,
):
    output = evaluation.copy()

    output["bucket"] = pd.qcut(
        output["raw_prob"],
        q=5,
        labels=False,
        duplicates="drop",
    )

    if output["bucket"].isna().any():
        raise RuntimeError(
            "Could not assign every "
            "evaluation row to a bucket"
        )

    output["bucket"] = (
        output["bucket"]
        .astype(int)
        + 1
    )

    rows = []

    for (
        bucket,
        group,
    ) in output.groupby(
        "bucket",
        sort=True,
    ):
        actual = float(
            group[
                "win_binary"
            ].mean()
        )

        raw_mean = float(
            group[
                "raw_prob"
            ].mean()
        )

        logistic_mean = float(
            group[
                "logistic_prob"
            ].mean()
        )

        isotonic_mean = float(
            group[
                "isotonic_prob"
            ].mean()
        )

        rows.append(
            {
                "bucket":
                    int(
                        bucket
                    ),
                "n":
                    len(
                        group
                    ),
                "raw_prob_min":
                    float(
                        group[
                            "raw_prob"
                        ].min()
                    ),
                "raw_prob_mean":
                    raw_mean,
                "raw_prob_max":
                    float(
                        group[
                            "raw_prob"
                        ].max()
                    ),
                "logistic_prob_mean":
                    logistic_mean,
                "isotonic_prob_mean":
                    isotonic_mean,
                "actual_win_rate":
                    actual,
                "raw_abs_error":
                    abs(
                        raw_mean
                        - actual
                    ),
                "logistic_abs_error":
                    abs(
                        logistic_mean
                        - actual
                    ),
                "isotonic_abs_error":
                    abs(
                        isotonic_mean
                        - actual
                    ),
            }
        )

    return (
        pd.DataFrame(
            rows
        ),
        output,
    )


def weighted_bucket_error(
    buckets: pd.DataFrame,
    column: str,
) -> float:
    return float(
        np.average(
            buckets[
                column
            ],
            weights=buckets[
                "n"
            ],
        )
    )


def build_summary(
    fit: pd.DataFrame,
    evaluation: pd.DataFrame,
    fit_dates,
    eval_dates,
    buckets: pd.DataFrame,
    intercept: float,
    slope: float,
) -> pd.DataFrame:
    raw_metrics = probability_metrics(
        evaluation[
            "win_binary"
        ],
        evaluation[
            "raw_prob"
        ],
    )

    win_rates = buckets[
        "actual_win_rate"
    ].tolist()

    high_minus_low = float(
        win_rates[-1]
        - win_rates[0]
    )

    rising_steps = int(
        sum(
            right
            >= left
            for (
                left,
                right,
            ) in zip(
                win_rates,
                win_rates[
                    1:
                ],
            )
        )
    )

    rows = []

    for (
        method,
        prob_col,
        error_col,
    ) in (
        (
            "raw",
            "raw_prob",
            "raw_abs_error",
        ),
        (
            "logistic",
            "logistic_prob",
            "logistic_abs_error",
        ),
        (
            "isotonic",
            "isotonic_prob",
            "isotonic_abs_error",
        ),
    ):
        metrics = probability_metrics(
            evaluation[
                "win_binary"
            ],
            evaluation[
                prob_col
            ],
        )

        reversals = count_rank_reversals(
            evaluation[
                "raw_prob"
            ],
            evaluation[
                prob_col
            ],
        )

        rows.append(
            {
                "method":
                    method,
                "fit_rows":
                    len(
                        fit
                    ),
                "evaluation_rows":
                    len(
                        evaluation
                    ),
                "fit_dates":
                    len(
                        fit_dates
                    ),
                "evaluation_dates":
                    len(
                        eval_dates
                    ),
                "fit_start":
                    fit_dates[
                        0
                    ].strftime(
                        "%Y-%m-%d"
                    ),
                "fit_end":
                    fit_dates[
                        -1
                    ].strftime(
                        "%Y-%m-%d"
                    ),
                "evaluation_start":
                    eval_dates[
                        0
                    ].strftime(
                        "%Y-%m-%d"
                    ),
                "evaluation_end":
                    eval_dates[
                        -1
                    ].strftime(
                        "%Y-%m-%d"
                    ),
                "brier_score":
                    metrics[
                        "brier_score"
                    ],
                "brier_change_vs_raw":
                    (
                        metrics[
                            "brier_score"
                        ]
                        - raw_metrics[
                            "brier_score"
                        ]
                    ),
                "log_loss":
                    metrics[
                        "log_loss"
                    ],
                "log_loss_change_vs_raw":
                    (
                        metrics[
                            "log_loss"
                        ]
                        - raw_metrics[
                            "log_loss"
                        ]
                    ),
                "weighted_bucket_error":
                    weighted_bucket_error(
                        buckets,
                        error_col,
                    ),
                "max_bucket_error":
                    float(
                        buckets[
                            error_col
                        ].max()
                    ),
                "mean_probability":
                    metrics[
                        "mean_probability"
                    ],
                "actual_win_rate":
                    metrics[
                        "actual_win_rate"
                    ],
                "low_bucket_win_rate":
                    float(
                        win_rates[
                            0
                        ]
                    ),
                "high_bucket_win_rate":
                    float(
                        win_rates[
                            -1
                        ]
                    ),
                "high_minus_low_win_rate":
                    high_minus_low,
                "rising_bucket_steps_out_of_4":
                    rising_steps,
                "rank_reversals_vs_raw":
                    reversals,
                "ranking_preserved":
                    (
                        reversals
                        == 0
                    ),
                "logistic_intercept":
                    (
                        intercept
                        if method
                        == "logistic"
                        else np.nan
                    ),
                "logistic_slope":
                    (
                        slope
                        if method
                        == "logistic"
                        else np.nan
                    ),
            }
        )

    return pd.DataFrame(
        rows
    )


def write_report(
    summary,
    buckets,
    evaluation,
    intercept: float,
    slope: float,
) -> None:
    REPORT_FILE.write_text(
        f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Run-Line Calibration Test</title>
<style>
body {{
    font-family: Segoe UI, Arial, sans-serif;
    margin: 24px;
}}
table {{
    border-collapse: collapse;
    font-size: 12px;
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

<h1>MLB Run-Line Calibration Test</h1>

<p>
Generated:
{datetime.now(UTC).isoformat()}
</p>

<p>
Underlying run model and run-line probability formula are unchanged.
</p>

<p>
Calibration fit uses earlier dates only; final 30% of dates are untouched
evaluation data.
</p>

<p>
<strong>Logistic calibrator:</strong>
sigmoid(
{intercept:.12f}
+
{slope:.12f}
× logit(raw probability)
)
</p>

<h2>Summary</h2>

{summary.to_html(
    index=False,
    border=0,
)}

<h2>Evaluation buckets</h2>

{buckets.to_html(
    index=False,
    border=0,
)}

<h2>Evaluation rows</h2>

{evaluation.drop(
    columns=[
        "_date"
    ]
).to_html(
    index=False,
    border=0,
)}

</body>
</html>
""",
        encoding="utf-8",
    )


def main() -> None:
    predictions = build_runline_predictions()

    (
        fit,
        evaluation,
        fit_dates,
        eval_dates,
    ) = split_calibration_data(
        predictions
    )

    print(
        f"CALIBRATION FIT: "
        f"{len(fit):,} rows / "
        f"{len(fit_dates)} dates"
    )

    print(
        f"UNTOUCHED EVALUATION: "
        f"{len(evaluation):,} rows / "
        f"{len(eval_dates)} dates"
    )

    (
        intercept,
        slope,
    ) = fit_logistic_calibrator(
        fit
    )

    isotonic = IsotonicRegression(
        y_min=EPS,
        y_max=1.0 - EPS,
        increasing=True,
        out_of_bounds="clip",
    )

    isotonic.fit(
        fit[
            "raw_prob"
        ],
        fit[
            "win_binary"
        ],
    )

    evaluation = evaluation.copy()

    evaluation[
        "logistic_prob"
    ] = apply_logistic(
        evaluation[
            "raw_prob"
        ],
        intercept,
        slope,
    )

    evaluation[
        "isotonic_prob"
    ] = clip_prob(
        isotonic.predict(
            evaluation[
                "raw_prob"
            ]
        )
    )

    (
        buckets,
        evaluation,
    ) = build_buckets(
        evaluation
    )

    summary = build_summary(
        fit,
        evaluation,
        fit_dates,
        eval_dates,
        buckets,
        intercept,
        slope,
    )

    output = predictions.drop(
        columns=[
            "_date"
        ]
    ).copy()

    extra = evaluation[
        [
            "game_date",
            "game_id",
            "side",
            "logistic_prob",
            "isotonic_prob",
            "bucket",
        ]
    ]

    output = output.merge(
        extra,
        on=[
            "game_date",
            "game_id",
            "side",
        ],
        how="left",
        validate="one_to_one",
    )

    summary.to_csv(
        SUMMARY_FILE,
        index=False,
    )

    buckets.to_csv(
        BUCKET_FILE,
        index=False,
    )

    output.to_csv(
        PREDICTIONS_FILE,
        index=False,
    )

    write_report(
        summary,
        buckets,
        evaluation,
        intercept,
        slope,
    )

    print(
        "\nRUN-LINE CALIBRATION TEST COMPLETE"
    )

    print(
        summary.to_string(
            index=False
        )
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
    try:
        main()

    except KeyboardInterrupt:
        raise SystemExit(
            130
        )

    except Exception as exc:
        print(
            f"ERROR: {exc}"
        )

        print(
            traceback.format_exc()
        )

        raise SystemExit(
            1
        )