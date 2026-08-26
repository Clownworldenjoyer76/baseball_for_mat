#!/usr/bin/env python3
"""Leakage-safe holdout test for a dedicated MLB total-runs model.

Compares on one untouched chronological test set:
- current_sum: production-style home model + production-style away model
- direct_total: one model trained directly on total runs

No production files or models are modified.
"""

from __future__ import annotations

import importlib.util
import warnings
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_poisson_deviance,
)

warnings.filterwarnings(
    "ignore",
    message=r"`sklearn\.utils\.parallel\.delayed` should be used with .*",
    category=UserWarning,
)

ROOT = next(
    p
    for start in (
        Path.cwd().resolve(),
        Path(__file__).resolve().parent,
    )
    for p in (start, *start.parents)
    if (p / "docs/win/baseball/mlb").exists()
)

BASE = ROOT / "docs/win/baseball/mlb"

TRAINING_FILE = (
    BASE
    / "modeling/data/mlb_run_training_set.csv"
)

SUMMARY_FILE = (
    ROOT
    / "mlb_total_model_test_summary.csv"
)

BUCKET_FILE = (
    ROOT
    / "mlb_total_model_test_buckets.csv"
)

PREDICTIONS_FILE = (
    ROOT
    / "mlb_total_model_test_predictions.csv"
)

REPORT_FILE = (
    ROOT
    / "mlb_total_model_test_report.html"
)

EPS = 1e-12


def load_module(
    name: str,
    path: Path,
):
    if not path.exists():
        raise FileNotFoundError(path)

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

    mod = (
        importlib.util
        .module_from_spec(
            spec
        )
    )

    spec.loader.exec_module(
        mod
    )

    return mod


TRAIN = load_module(
    "total_train",
    BASE
    / "scripts/modeling/train_run_model.py",
)

BT = load_module(
    "total_backtest",
    BASE
    / "scripts/modeling/backtest_new_pipeline_correlations.py",
)

BP = load_module(
    "total_bp",
    BASE
    / "scripts/modeling/test_moneyline_bullpen_detail.py",
)

JUICE = load_module(
    "total_juice",
    BASE
    / "scripts/01_merge/build_juice_files.py",
)

TRAIN._log = (
    lambda *args, **kwargs: None
)

BP_FEATURES = list(
    dict.fromkeys(
        [
            *BP.CURRENT,
            *BP.PLUS_7D,
        ]
    )
)


def load_training():
    frame = pd.read_csv(
        TRAINING_FILE,
        encoding="utf-8-sig",
    )

    if frame.empty:
        raise RuntimeError(
            f"Training set is empty: "
            f"{TRAINING_FILE}"
        )

    trainer_bp = [
        col
        for col in TRAIN.CORE_FEATURE_COLUMNS
        if "_bp_" in col
    ]

    if trainer_bp != BP_FEATURES:
        raise RuntimeError(
            "Bullpen feature contract mismatch: "
            f"trainer={trainer_bp}; "
            f"builder={BP_FEATURES}"
        )

    base_core = [
        col
        for col in TRAIN.CORE_FEATURE_COLUMNS
        if col not in BP_FEATURES
    ]

    base_features = (
        list(base_core)
        + [
            col
            for col
            in TRAIN.OPTIONAL_NUMERIC_FEATURE_COLUMNS
            if col in frame.columns
        ]
    )

    required = (
        list(
            TRAIN.AUDIT_COLUMNS
        )
        + list(
            TRAIN.TARGET_COLUMNS
        )
        + base_core
    )

    missing = [
        col
        for col in required
        if col not in frame.columns
    ]

    if missing:
        raise RuntimeError(
            "Base training set missing "
            f"required columns: {missing}"
        )

    frame = (
        TRAIN
        .coerce_and_validate_training_data(
            frame,
            base_features,
        )
    )

    frame["game_id"] = (
        frame["game_id"]
        .astype("string")
        .str.strip()
    )

    frame["_home_code"] = (
        frame["home_team"]
        .map(
            BP.EXP.canonical_team
        )
    )

    frame["_away_code"] = (
        frame["away_team"]
        .map(
            BP.EXP.canonical_team
        )
    )

    if (
        frame["game_id"]
        .duplicated()
        .any()
    ):
        raise RuntimeError(
            "Training data contains "
            "duplicate game_id"
        )

    if (
        frame["_home_code"]
        .isna()
        .any()
        or frame["_away_code"]
        .isna()
        .any()
    ):
        raise RuntimeError(
            "Training data contains "
            "unmapped teams"
        )

    print(
        "Loading existing Statcast cache..."
    )

    raw = BP.load_cache()

    if raw.empty:
        raise RuntimeError(
            "Statcast cache is empty"
        )

    cache_max = pd.to_datetime(
        raw["_game_date_dt"],
        errors="coerce",
    ).max()

    required_through = (
        frame["_game_date_dt"].max()
        - pd.Timedelta(
            days=1
        )
    )

    if (
        pd.isna(cache_max)
        or cache_max
        < required_through
    ):
        raise RuntimeError(
            f"Statcast cache ends "
            f"{cache_max}; "
            f"required through "
            f"{required_through}"
        )

    print(
        "Building exact production "
        "bullpen features..."
    )

    (
        team_daily,
        pitcher_daily,
    ) = BP.build_tables(
        raw
    )

    frame = BP.attach_features(
        frame,
        team_daily,
        pitcher_daily,
    )

    missing_bp = [
        col
        for col in BP_FEATURES
        if col not in frame.columns
    ]

    if missing_bp:
        raise RuntimeError(
            "Bullpen builder failed "
            f"to create: {missing_bp}"
        )

    features = (
        TRAIN
        .determine_feature_columns(
            frame
        )
    )

    frame = (
        TRAIN
        .coerce_and_validate_training_data(
            frame,
            features,
        )
    )

    frame[
        "target_total_runs"
    ] = (
        frame[
            "target_home_runs"
        ]
        + frame[
            "target_away_runs"
        ]
    )

    if (
        frame[
            "target_total_runs"
        ].isna().any()
        or (
            ~np.isfinite(
                frame[
                    "target_total_runs"
                ]
            )
        ).any()
        or (
            frame[
                "target_total_runs"
            ]
            < 0
        ).any()
    ):
        raise RuntimeError(
            "Invalid target_total_runs"
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

    print(
        f"Training data ready: "
        f"{len(frame):,} rows, "
        f"{frame['_game_date_dt'].nunique():,} "
        f"dates, "
        f"{len(features)} features"
    )

    return (
        frame,
        features,
    )


def fit_one(
    train,
    validation,
    features,
    target,
    label,
):
    print(
        f"Tuning {label} once..."
    )

    (
        params,
        score,
    ) = (
        TRAIN
        .select_hyperparameters(
            train,
            validation,
            features,
            target,
            label,
        )
    )

    model = (
        TRAIN
        .fit_final_model(
            train,
            validation,
            features,
            target,
            params,
        )
    )

    print(
        f"{label}: "
        f"validation_poisson="
        f"{score:.6f} "
        f"params={params}"
    )

    return model


def sportsbook_test_rows(
    test,
):
    files = (
        BT.sportsbook_files()
    )

    pieces = []

    for (
        dt,
        games,
    ) in test.groupby(
        "_game_date_dt",
        sort=True,
    ):
        dt = (
            pd.Timestamp(
                dt
            )
            .normalize()
        )

        path = files.get(
            dt
        )

        if path is None:
            continue

        book = (
            BT.load_sportsbook(
                path
            )
        )

        joined = games.merge(
            book,
            on="game_id",
            how="inner",
            suffixes=(
                "_train",
                "_book",
            ),
            validate="one_to_one",
        )

        if not joined.empty:
            pieces.append(
                joined
            )

    if not pieces:
        raise RuntimeError(
            "No sportsbook totals overlap "
            "the untouched test set"
        )

    out = pd.concat(
        pieces,
        ignore_index=True,
        sort=False,
    )

    out["total"] = (
        pd.to_numeric(
            out["total"],
            errors="coerce",
        )
    )

    out = out[
        out["total"].notna()
        & np.isfinite(
            out["total"]
        )
    ].copy()

    if out.empty:
        raise RuntimeError(
            "No usable sportsbook totals "
            "in untouched test set"
        )

    return out


def grade_total(
    side,
    line,
    actual,
):
    margin = (
        actual
        - line
    )

    if (
        abs(margin)
        <= EPS
    ):
        return "Push"

    if side == "over":
        return (
            "Win"
            if margin > 0
            else "Loss"
        )

    if side == "under":
        return (
            "Win"
            if margin < 0
            else "Loss"
        )

    raise ValueError(
        f"Unknown side: {side}"
    )


def make_predictions(
    test_rows,
):
    rows = []

    for _, r in (
        test_rows.iterrows()
    ):
        mh = float(
            r[
                "current_home_runs"
            ]
        )

        ma = float(
            r[
                "current_away_runs"
            ]
        )

        current_total = float(
            r[
                "current_sum_total"
            ]
        )

        direct_total = float(
            r[
                "direct_total"
            ]
        )

        line = float(
            r["total"]
        )

        actual = float(
            r[
                "target_home_runs"
            ]
            + r[
                "target_away_runs"
            ]
        )

        current_probs = (
            JUICE
            .totals_probabilities(
                mh,
                ma,
                line,
            )
        )

        direct_probs = (
            JUICE
            .totals_probabilities(
                direct_total,
                0.0,
                line,
            )
        )

        for (
            variant,
            projection,
            probs,
        ) in (
            (
                "current_sum",
                current_total,
                current_probs,
            ),
            (
                "direct_total",
                direct_total,
                direct_probs,
            ),
        ):
            (
                p_over,
                p_under,
                p_push,
            ) = probs

            for (
                side,
                p_win,
                p_loss,
            ) in (
                (
                    "over",
                    p_over,
                    p_under,
                ),
                (
                    "under",
                    p_under,
                    p_over,
                ),
            ):
                result = grade_total(
                    side,
                    line,
                    actual,
                )

                resolved = (
                    p_win
                    + p_loss
                )

                if (
                    not np.isfinite(
                        resolved
                    )
                    or resolved <= 0
                ):
                    raise RuntimeError(
                        "Invalid resolved "
                        "probability mass"
                    )

                rows.append(
                    {
                        "variant":
                            variant,

                        "game_date":
                            pd.Timestamp(
                                r[
                                    "_game_date_dt"
                                ]
                            ).strftime(
                                "%Y-%m-%d"
                            ),

                        "game_id":
                            str(
                                r[
                                    "game_id"
                                ]
                            ),

                        "home_team":
                            r.get(
                                "home_team_train",
                                r.get(
                                    "home_team"
                                ),
                            ),

                        "away_team":
                            r.get(
                                "away_team_train",
                                r.get(
                                    "away_team"
                                ),
                            ),

                        "side":
                            side,

                        "total_line":
                            line,

                        "projected_total":
                            projection,

                        "actual_total":
                            actual,

                        "model_prob":
                            float(
                                p_win
                            ),

                        "resolved_prob":
                            float(
                                p_win
                                / resolved
                            ),

                        "p_loss":
                            float(
                                p_loss
                            ),

                        "p_push":
                            float(
                                p_push
                            ),

                        "result":
                            result,

                        "win_binary":
                            (
                                1.0
                                if result
                                == "Win"
                                else (
                                    0.0
                                    if result
                                    == "Loss"
                                    else np.nan
                                )
                            ),
                    }
                )

    return pd.DataFrame(
        rows
    )


def corr(
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
        mask.sum()
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
        np.std(x)
        == 0
        or np.std(y)
        == 0
    ):
        return np.nan

    return float(
        np.corrcoef(
            x,
            y,
        )[0, 1]
    )


def make_buckets(
    resolved,
):
    rows = []

    for (
        variant,
        part,
    ) in resolved.groupby(
        "variant",
        sort=False,
    ):
        part = (
            part.copy()
        )

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

        part = (
            part.dropna(
                subset=[
                    "bucket"
                ]
            )
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
            g,
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
                        len(g),

                    "prob_mean":
                        float(
                            g[
                                "model_prob"
                            ].mean()
                        ),

                    "actual_win_rate":
                        float(
                            g[
                                "win_binary"
                            ].mean()
                        ),
                }
            )

    return pd.DataFrame(
        rows
    )


def projection_metrics(
    games,
    col,
):
    actual = (
        games[
            "actual_total"
        ]
        .to_numpy(
            dtype=float
        )
    )

    pred = (
        games[
            col
        ]
        .to_numpy(
            dtype=float
        )
    )

    return {
        "projection_mae":
            float(
                mean_absolute_error(
                    actual,
                    pred,
                )
            ),

        "projection_poisson_deviance":
            float(
                mean_poisson_deviance(
                    actual,
                    np.maximum(
                        pred,
                        EPS,
                    ),
                )
            ),

        "projection_actual_corr":
            corr(
                pred,
                actual,
            ),

        "mean_projected_total":
            float(
                np.mean(
                    pred
                )
            ),

        "mean_actual_total":
            float(
                np.mean(
                    actual
                )
            ),
    }


def make_summary(
    predictions,
    games,
    buckets,
):
    resolved = (
        predictions
        .dropna(
            subset=[
                "win_binary"
            ]
        )
        .copy()
    )

    rows = []

    for (
        variant,
        projection_col,
    ) in (
        (
            "current_sum",
            "current_sum_total",
        ),
        (
            "direct_total",
            "direct_total",
        ),
    ):
        part = resolved[
            resolved[
                "variant"
            ]
            == variant
        ].copy()

        bucket_rows = (
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
            or bucket_rows.empty
        ):
            raise RuntimeError(
                f"No evaluation rows "
                f"for {variant}"
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
                "resolved_prob"
            ]
            .to_numpy(
                dtype=float
            ),
            EPS,
            1.0 - EPS,
        )

        win_rates = (
            bucket_rows[
                "actual_win_rate"
            ]
            .to_numpy(
                dtype=float
            )
        )

        rows.append(
            {
                "variant":
                    variant,

                "games":
                    len(
                        games
                    ),

                "resolved_candidates":
                    len(
                        part
                    ),

                **projection_metrics(
                    games,
                    projection_col,
                ),

                "model_probability_win_corr":
                    corr(
                        part[
                            "model_prob"
                        ],
                        part[
                            "win_binary"
                        ],
                    ),

                "resolved_probability_win_corr":
                    corr(
                        part[
                            "resolved_prob"
                        ],
                        part[
                            "win_binary"
                        ],
                    ),

                "brier_score_resolved":
                    float(
                        brier_score_loss(
                            y,
                            p,
                        )
                    ),

                "log_loss_resolved":
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
<title>MLB Dedicated Total Model Test</title>
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

<h1>MLB Dedicated Total Model Test</h1>

<p>
Generated:
{datetime.now(UTC).isoformat()}
</p>

<p>
One chronological 70% train /
15% validation /
15% untouched test split.
</p>

<p>
current_sum =
production-style home model +
away model.
</p>

<p>
direct_total =
model trained directly on total runs.
</p>

<p>
Production totals probability formula
is unchanged.
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
        features,
    ) = load_training()

    split = (
        TRAIN
        .chronological_date_split(
            training
        )
    )

    print(
        "Split: "
        f"train="
        f"{len(split['train']):,}, "
        f"validation="
        f"{len(split['validation']):,}, "
        f"test="
        f"{len(split['test']):,}"
    )

    home_model = fit_one(
        split[
            "train"
        ],
        split[
            "validation"
        ],
        features,
        "target_home_runs",
        "home_runs",
    )

    away_model = fit_one(
        split[
            "train"
        ],
        split[
            "validation"
        ],
        features,
        "target_away_runs",
        "away_runs",
    )

    total_model = fit_one(
        split[
            "train"
        ],
        split[
            "validation"
        ],
        features,
        "target_total_runs",
        "total_runs",
    )

    test = (
        split[
            "test"
        ]
        .copy()
    )

    test[
        "current_home_runs"
    ] = home_model.predict(
        test[
            features
        ]
    )

    test[
        "current_away_runs"
    ] = away_model.predict(
        test[
            features
        ]
    )

    test[
        "current_sum_total"
    ] = (
        test[
            "current_home_runs"
        ]
        + test[
            "current_away_runs"
        ]
    )

    test[
        "direct_total"
    ] = total_model.predict(
        test[
            features
        ]
    )

    for col in (
        "current_home_runs",
        "current_away_runs",
        "current_sum_total",
        "direct_total",
    ):
        values = (
            pd.to_numeric(
                test[
                    col
                ],
                errors="coerce",
            )
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
                f"Invalid predictions "
                f"in {col}"
            )

    games = (
        sportsbook_test_rows(
            test
        )
    )

    games[
        "actual_total"
    ] = (
        games[
            "target_home_runs"
        ]
        + games[
            "target_away_runs"
        ]
    )

    predictions = (
        make_predictions(
            games
        )
    )

    resolved = (
        predictions
        .dropna(
            subset=[
                "win_binary"
            ]
        )
        .copy()
    )

    if resolved.empty:
        raise RuntimeError(
            "No resolved total candidates"
        )

    buckets = (
        make_buckets(
            resolved
        )
    )

    summary = (
        make_summary(
            predictions,
            games,
            buckets,
        )
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

    print(
        "\nTOTAL MODEL TEST COMPLETE"
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
    main()