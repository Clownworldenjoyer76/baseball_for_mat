#!/usr/bin/env python3
"""Test expected-lineup hitting features for MLB moneyline.

Experiment only. Does not modify production models or outputs.

Uses:
    - existing historical training data
    - existing Statcast cache
    - existing best 7-day bullpen model
    - projected lineups inferred ONLY from games before each target date

Reference versions:
    baseline
    bullpen_plus_7d

New versions:
    lineup_30d
    bullpen_7d_plus_lineup_30d
    lineup_14d
    bullpen_7d_plus_lineup_14d
    bullpen_7d_plus_lineup_both

Outputs:
    mlb_moneyline_expected_lineup_test_summary.csv
    mlb_moneyline_expected_lineup_test_predictions.csv
    mlb_moneyline_expected_lineup_test_report.html
"""

from __future__ import annotations

import html
import importlib.util
import sys
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor


def repo_root() -> Path:
    for start in (
        Path.cwd().resolve(),
        Path(__file__).resolve().parent,
    ):
        for candidate in (start, *start.parents):
            if (candidate / "docs/win/baseball/mlb").exists():
                return candidate

    raise RuntimeError("Could not locate repository root")


ROOT = repo_root()
BASE = ROOT / "docs/win/baseball/mlb"

FIRST_TEST = (
    BASE
    / "scripts/modeling/test_moneyline_feature_expansion.py"
)

BULLPEN_TEST = (
    BASE
    / "scripts/modeling/test_moneyline_bullpen_detail.py"
)

REFERENCE_PREDICTIONS = (
    ROOT
    / "mlb_moneyline_bullpen_detail_test_predictions.csv"
)

SUMMARY_FILE = (
    ROOT
    / "mlb_moneyline_expected_lineup_test_summary.csv"
)

PREDICTIONS_FILE = (
    ROOT
    / "mlb_moneyline_expected_lineup_test_predictions.csv"
)

REPORT_FILE = (
    ROOT
    / "mlb_moneyline_expected_lineup_test_report.html"
)


def load_module(name: str, path: Path):
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location(
        name,
        path,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Could not load module: {path}"
        )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


EXP = load_module(
    "moneyline_expected_lineup_base",
    FIRST_TEST,
)

BP = load_module(
    "moneyline_expected_lineup_bp",
    BULLPEN_TEST,
)

TRAIN = EXP.TRAIN
JUICE = EXP.JUICE

TRAIN._log = lambda *args, **kwargs: None


BP7_FEATURES = list(
    dict.fromkeys(
        [
            *BP.CURRENT,
            *BP.PLUS_7D,
        ]
    )
)


LINEUP_30D_FEATURES = [
    "home_expected_lineup_count",
    "away_expected_lineup_count",
    "home_expected_lineup_confidence",
    "away_expected_lineup_confidence",
    "home_lineup_pa_30d",
    "away_lineup_pa_30d",
    "home_lineup_woba_30d",
    "away_lineup_woba_30d",
    "home_lineup_k_rate_30d",
    "away_lineup_k_rate_30d",
    "home_lineup_bb_rate_30d",
    "away_lineup_bb_rate_30d",
    "home_lineup_hard_rate_30d",
    "away_lineup_hard_rate_30d",
    "home_lineup_avg_ev_30d",
    "away_lineup_avg_ev_30d",
]


LINEUP_14D_FEATURES = [
    "home_expected_lineup_count",
    "away_expected_lineup_count",
    "home_expected_lineup_confidence",
    "away_expected_lineup_confidence",
    "home_lineup_pa_14d",
    "away_lineup_pa_14d",
    "home_lineup_woba_14d",
    "away_lineup_woba_14d",
    "home_lineup_k_rate_14d",
    "away_lineup_k_rate_14d",
    "home_lineup_bb_rate_14d",
    "away_lineup_bb_rate_14d",
    "home_lineup_hard_rate_14d",
    "away_lineup_hard_rate_14d",
    "home_lineup_avg_ev_14d",
    "away_lineup_avg_ev_14d",
]


LINEUP_BOTH_FEATURES = list(
    dict.fromkeys(
        [
            *LINEUP_30D_FEATURES,
            *LINEUP_14D_FEATURES,
        ]
    )
)


NEW_VARIANTS = {
    "lineup_30d":
        LINEUP_30D_FEATURES,

    "bullpen_7d_plus_lineup_30d":
        [
            *BP7_FEATURES,
            *LINEUP_30D_FEATURES,
        ],

    "lineup_14d":
        LINEUP_14D_FEATURES,

    "bullpen_7d_plus_lineup_14d":
        [
            *BP7_FEATURES,
            *LINEUP_14D_FEATURES,
        ],

    "bullpen_7d_plus_lineup_both":
        [
            *BP7_FEATURES,
            *LINEUP_BOTH_FEATURES,
        ],
}


VARIANT_ORDER = [
    "baseline",
    "bullpen_plus_7d",
    "lineup_30d",
    "bullpen_7d_plus_lineup_30d",
    "lineup_14d",
    "bullpen_7d_plus_lineup_14d",
    "bullpen_7d_plus_lineup_both",
]


def load_reference():
    if not REFERENCE_PREDICTIONS.exists():
        raise RuntimeError(
            "Missing prior bullpen prediction file: "
            f"{REFERENCE_PREDICTIONS}"
        )

    df = pd.read_csv(
        REFERENCE_PREDICTIONS,
        encoding="utf-8-sig",
    )

    required = [
        "variant",
        "game_date",
        "gamePk",
        "home_team",
        "away_team",
        "model_home_runs",
        "model_away_runs",
        "model_home_probability",
        "model_away_probability",
        "actual_home_runs",
        "actual_away_runs",
        "actual_home_win",
    ]

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        raise RuntimeError(
            "Prior bullpen predictions missing "
            f"columns: {missing}"
        )

    df["_date"] = pd.to_datetime(
        df["game_date"],
        errors="coerce",
    ).dt.normalize()

    if df["_date"].isna().any():
        raise RuntimeError(
            "Prior predictions contain invalid dates"
        )

    baseline = df[
        df["variant"] == "baseline"
    ].copy()

    bullpen = df[
        df["variant"] == "bullpen_plus_7d"
    ].copy()

    if baseline.empty:
        raise RuntimeError(
            "Baseline missing from prior test"
        )

    if bullpen.empty:
        raise RuntimeError(
            "bullpen_plus_7d missing from prior test"
        )

    reference = pd.concat(
        [
            baseline,
            bullpen,
        ],
        ignore_index=True,
        sort=False,
    )

    dates = [
        pd.Timestamp(value).normalize()
        for value in sorted(
            bullpen["_date"]
            .dropna()
            .unique()
        )
    ]

    if not dates:
        raise RuntimeError(
            "No evaluation dates found"
        )

    return reference, dates


def load_statcast():
    print(
        "Loading existing Statcast cache..."
    )

    raw = BP.load_cache()

    print(
        f"Statcast rows: {len(raw):,}"
    )

    # Keep RAW data for BP.build_tables().
    # It performs its own prepare_statcast() call.
    prepared = EXP.prepare_statcast(
        raw
    )

    if "batter" not in prepared.columns:
        raise RuntimeError(
            "Prepared Statcast data missing batter"
        )

    prepared["_batter"] = (
        pd.to_numeric(
            prepared["batter"],
            errors="coerce",
        )
        .astype("Int64")
    )

    return raw, prepared


def build_historical_starting_lineups(
    statcast: pd.DataFrame,
) -> pd.DataFrame:

    pa = statcast[
        statcast["_is_pa"]
        & statcast["_batter"].notna()
    ].copy()

    if pa.empty:
        raise RuntimeError(
            "No batter plate appearances found"
        )

    pa = pa.sort_values(
        [
            "_game_date_dt",
            "_gamePk",
            "_batting_team",
            "_at_bat_number",
            "_pitch_number",
        ]
    )

    first_pa = (
        pa.drop_duplicates(
            subset=[
                "_gamePk",
                "_batting_team",
                "_batter",
            ],
            keep="first",
        )
        .copy()
    )

    first_pa["_lineup_order"] = (
        first_pa.groupby(
            [
                "_gamePk",
                "_batting_team",
            ]
        )
        .cumcount()
        + 1
    )

    starters = first_pa[
        first_pa["_lineup_order"] <= 9
    ].copy()

    if starters.empty:
        raise RuntimeError(
            "Could not infer historical starting lineups"
        )

    return starters[
        [
            "_game_date_dt",
            "_gamePk",
            "_batting_team",
            "_batter",
            "_lineup_order",
        ]
    ].copy()


def expected_lineup(
    historical_lineups: pd.DataFrame,
    team: str,
    target_date: pd.Timestamp,
):

    prior = historical_lineups[
        (
            historical_lineups[
                "_batting_team"
            ]
            == team
        )
        & (
            historical_lineups[
                "_game_date_dt"
            ]
            < target_date
        )
    ].copy()

    if prior.empty:
        return [], 0.0

    recent_games = (
        prior[
            [
                "_game_date_dt",
                "_gamePk",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "_game_date_dt",
                "_gamePk",
            ],
            ascending=[
                False,
                False,
            ],
        )
        .head(5)
    )

    if recent_games.empty:
        return [], 0.0

    recent = prior.merge(
        recent_games,
        on=[
            "_game_date_dt",
            "_gamePk",
        ],
        how="inner",
        validate="many_to_one",
    )

    game_order = (
        recent_games.reset_index(
            drop=True
        )
    )

    game_weights = {}

    for index, row in game_order.iterrows():
        game_weights[
            (
                row["_game_date_dt"],
                row["_gamePk"],
            )
        ] = float(
            5 - index
        )

    recent["_recency_weight"] = [
        game_weights[
            (
                row["_game_date_dt"],
                row["_gamePk"],
            )
        ]
        for _, row in recent.iterrows()
    ]

    player_summary = (
        recent.groupby(
            "_batter",
            as_index=False,
        )
        .agg(
            recent_starts=(
                "_gamePk",
                "nunique",
            ),
            recency_score=(
                "_recency_weight",
                "sum",
            ),
            last_start=(
                "_game_date_dt",
                "max",
            ),
            average_lineup_position=(
                "_lineup_order",
                "mean",
            ),
        )
    )

    player_summary = (
        player_summary.sort_values(
            [
                "recent_starts",
                "recency_score",
                "last_start",
                "average_lineup_position",
                "_batter",
            ],
            ascending=[
                False,
                False,
                False,
                True,
                True,
            ],
        )
        .head(9)
    )

    hitters = (
        player_summary[
            "_batter"
        ]
        .astype("Int64")
        .tolist()
    )

    if not hitters:
        return [], 0.0

    regulars = int(
        (
            player_summary[
                "recent_starts"
            ]
            >= 2
        ).sum()
    )

    confidence = float(
        regulars / 9.0
    )

    return hitters, confidence


def build_daily_batter_history(
    statcast: pd.DataFrame,
) -> pd.DataFrame:

    pa = statcast[
        statcast["_is_pa"]
        & statcast["_batter"].notna()
    ].copy()

    pa["_woba_sum_value"] = (
        pa["_woba_value"]
        .fillna(0.0)
    )

    pa["_woba_n_value"] = (
        pa["_woba_value"]
        .notna()
        .astype(int)
    )

    pa["_ev_sum_value"] = (
        pa["_launch_speed"]
        .fillna(0.0)
    )

    pa["_ev_n_value"] = (
        pa["_launch_speed"]
        .notna()
        .astype(int)
    )

    return (
        pa.groupby(
            [
                "_game_date_dt",
                "_batter",
            ],
            as_index=False,
        )
        .agg(
            pa=(
                "_is_pa",
                "sum",
            ),
            woba_sum=(
                "_woba_sum_value",
                "sum",
            ),
            woba_n=(
                "_woba_n_value",
                "sum",
            ),
            strikeouts=(
                "_is_k",
                "sum",
            ),
            walks=(
                "_is_bb",
                "sum",
            ),
            bbe=(
                "_is_bbe",
                "sum",
            ),
            hard_hits=(
                "_is_hard",
                "sum",
            ),
            ev_sum=(
                "_ev_sum_value",
                "sum",
            ),
            ev_n=(
                "_ev_n_value",
                "sum",
            ),
        )
    )


def lineup_hitting_stats(
    daily_batters: pd.DataFrame,
    hitters: list,
    target_date: pd.Timestamp,
    days: int,
):

    if not hitters:
        return {
            "pa": 0.0,
            "woba": np.nan,
            "k_rate": np.nan,
            "bb_rate": np.nan,
            "hard_rate": np.nan,
            "avg_ev": np.nan,
        }

    start_date = (
        target_date
        - pd.Timedelta(days=days)
    )

    part = daily_batters[
        (
            daily_batters["_batter"]
            .isin(hitters)
        )
        & (
            daily_batters["_game_date_dt"]
            >= start_date
        )
        & (
            daily_batters["_game_date_dt"]
            < target_date
        )
    ]

    if part.empty:
        return {
            "pa": 0.0,
            "woba": np.nan,
            "k_rate": np.nan,
            "bb_rate": np.nan,
            "hard_rate": np.nan,
            "avg_ev": np.nan,
        }

    pa_count = float(
        part["pa"].sum()
    )

    woba_n = float(
        part["woba_n"].sum()
    )

    bbe = float(
        part["bbe"].sum()
    )

    ev_n = float(
        part["ev_n"].sum()
    )

    return {
        "pa":
            pa_count,

        "woba":
            EXP.safe_ratio(
                float(
                    part["woba_sum"].sum()
                ),
                woba_n,
            ),

        "k_rate":
            EXP.safe_ratio(
                float(
                    part["strikeouts"].sum()
                ),
                pa_count,
            ),

        "bb_rate":
            EXP.safe_ratio(
                float(
                    part["walks"].sum()
                ),
                pa_count,
            ),

        "hard_rate":
            EXP.safe_ratio(
                float(
                    part["hard_hits"].sum()
                ),
                bbe,
            ),

        "avg_ev":
            EXP.safe_ratio(
                float(
                    part["ev_sum"].sum()
                ),
                ev_n,
            ),
    }


def attach_lineup_features(
    frame: pd.DataFrame,
    historical_lineups: pd.DataFrame,
    daily_batters: pd.DataFrame,
) -> pd.DataFrame:

    out = frame.copy()

    all_cols = list(
        dict.fromkeys(
            [
                *LINEUP_30D_FEATURES,
                *LINEUP_14D_FEATURES,
            ]
        )
    )

    values = {
        col: []
        for col in all_cols
    }

    lineup_cache = {}
    stat_cache = {}

    for position, (_, row) in enumerate(
        out.iterrows(),
        start=1,
    ):
        if (
            position == 1
            or position % 200 == 0
            or position == len(out)
        ):
            print(
                f"LINEUP FEATURES "
                f"{position}/{len(out)}"
            )

        target_date = pd.Timestamp(
            row["_game_date_dt"]
        ).normalize()

        for side in ("home", "away"):
            team = row[
                f"_{side}_code"
            ]

            lineup_key = (
                target_date,
                team,
            )

            if lineup_key not in lineup_cache:
                lineup_cache[
                    lineup_key
                ] = expected_lineup(
                    historical_lineups,
                    team,
                    target_date,
                )

            hitters, confidence = (
                lineup_cache[
                    lineup_key
                ]
            )

            values[
                f"{side}_expected_lineup_count"
            ].append(
                float(len(hitters))
            )

            values[
                f"{side}_expected_lineup_confidence"
            ].append(
                confidence
            )

            for days in (30, 14):
                stat_key = (
                    target_date,
                    team,
                    days,
                )

                if stat_key not in stat_cache:
                    stat_cache[
                        stat_key
                    ] = lineup_hitting_stats(
                        daily_batters,
                        hitters,
                        target_date,
                        days,
                    )

                stats = stat_cache[
                    stat_key
                ]

                values[
                    f"{side}_lineup_pa_{days}d"
                ].append(
                    stats["pa"]
                )

                values[
                    f"{side}_lineup_woba_{days}d"
                ].append(
                    stats["woba"]
                )

                values[
                    f"{side}_lineup_k_rate_{days}d"
                ].append(
                    stats["k_rate"]
                )

                values[
                    f"{side}_lineup_bb_rate_{days}d"
                ].append(
                    stats["bb_rate"]
                )

                values[
                    f"{side}_lineup_hard_rate_{days}d"
                ].append(
                    stats["hard_rate"]
                )

                values[
                    f"{side}_lineup_avg_ev_{days}d"
                ].append(
                    stats["avg_ev"]
                )

    for col, data in values.items():
        if len(data) != len(out):
            raise RuntimeError(
                f"Feature row count mismatch for {col}: "
                f"{len(data)} vs {len(out)}"
            )

        out[col] = pd.to_numeric(
            pd.Series(
                data,
                index=out.index,
            ),
            errors="coerce",
        )

    return out


def select_params(
    development: pd.DataFrame,
    variants: dict,
):

    split = (
        TRAIN.chronological_date_split(
            development
        )
    )

    selected = {}

    for index, (
        name,
        features,
    ) in enumerate(
        variants.items(),
        start=1,
    ):
        print(
            f"TUNING "
            f"{index}/{len(variants)} "
            f"{name}"
        )

        home_params, home_score = (
            TRAIN.select_hyperparameters(
                split["train"],
                split["validation"],
                features,
                "target_home_runs",
                f"{name}_home",
            )
        )

        away_params, away_score = (
            TRAIN.select_hyperparameters(
                split["train"],
                split["validation"],
                features,
                "target_away_runs",
                f"{name}_away",
            )
        )

        selected[name] = {
            "home":
                home_params,

            "away":
                away_params,

            "home_score":
                home_score,

            "away_score":
                away_score,
        }

    return selected


def fit_model(
    prior: pd.DataFrame,
    features: list[str],
    target: str,
    params: dict,
):

    model = HistGradientBoostingRegressor(
        loss="poisson",
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

    model.fit(
        prior[features],
        prior[target],
    )

    return model


def predict_new_variants(
    frame: pd.DataFrame,
    dates: list[pd.Timestamp],
    baseline_features: list[str],
    selected: dict,
):

    variants = {
        name: list(
            dict.fromkeys(
                [
                    *baseline_features,
                    *extras,
                ]
            )
        )
        for name, extras
        in NEW_VARIANTS.items()
    }

    rows = []

    for date_index, target_date in enumerate(
        dates,
        start=1,
    ):
        print(
            f"TEST DATE "
            f"{date_index}/{len(dates)} "
            f"{target_date.date()}"
        )

        prior = frame[
            frame["_game_date_dt"]
            < target_date
        ].copy()

        current = frame[
            frame["_game_date_dt"]
            == target_date
        ].copy()

        if current.empty:
            continue

        for name, features in variants.items():
            home_model = fit_model(
                prior,
                features,
                "target_home_runs",
                selected[name]["home"],
            )

            away_model = fit_model(
                prior,
                features,
                "target_away_runs",
                selected[name]["away"],
            )

            home_runs = np.asarray(
                home_model.predict(
                    current[features]
                ),
                dtype=float,
            )

            away_runs = np.asarray(
                away_model.predict(
                    current[features]
                ),
                dtype=float,
            )

            if (
                np.any(~np.isfinite(home_runs))
                or np.any(home_runs < 0)
            ):
                raise RuntimeError(
                    f"{name} invalid home-run predictions"
                )

            if (
                np.any(~np.isfinite(away_runs))
                or np.any(away_runs < 0)
            ):
                raise RuntimeError(
                    f"{name} invalid away-run predictions"
                )

            for position, (_, game) in enumerate(
                current.iterrows()
            ):
                actual_home = float(
                    game["target_home_runs"]
                )

                actual_away = float(
                    game["target_away_runs"]
                )

                if actual_home == actual_away:
                    continue

                (
                    home_probability,
                    away_probability,
                    _,
                ) = JUICE.moneyline_probabilities(
                    float(home_runs[position]),
                    float(away_runs[position]),
                )

                rows.append(
                    {
                        "variant":
                            name,

                        "game_date":
                            target_date.strftime(
                                "%Y-%m-%d"
                            ),

                        "gamePk":
                            game["gamePk"],

                        "home_team":
                            game["home_team"],

                        "away_team":
                            game["away_team"],

                        "model_home_runs":
                            float(
                                home_runs[position]
                            ),

                        "model_away_runs":
                            float(
                                away_runs[position]
                            ),

                        "model_home_probability":
                            float(
                                home_probability
                            ),

                        "model_away_probability":
                            float(
                                away_probability
                            ),

                        "actual_home_runs":
                            actual_home,

                        "actual_away_runs":
                            actual_away,

                        "actual_home_win":
                            (
                                1.0
                                if actual_home > actual_away
                                else 0.0
                            ),
                    }
                )

    if not rows:
        raise RuntimeError(
            "No lineup-test predictions produced"
        )

    return pd.DataFrame(rows)


def score_variant(
    part: pd.DataFrame,
):

    work = (
        part[
            [
                "model_home_probability",
                "actual_home_win",
            ]
        ]
        .dropna()
        .copy()
    )

    if len(work) < 25:
        raise RuntimeError(
            f"Too few rows to score: {len(work)}"
        )

    probabilities = (
        work[
            "model_home_probability"
        ]
        .to_numpy(
            dtype=float
        )
    )

    results = (
        work[
            "actual_home_win"
        ]
        .to_numpy(
            dtype=float
        )
    )

    if (
        np.std(probabilities) > 0
        and np.std(results) > 0
    ):
        correlation = float(
            np.corrcoef(
                probabilities,
                results,
            )[0, 1]
        )

    else:
        correlation = np.nan

    probability_error = float(
        np.mean(
            (
                probabilities
                - results
            )
            ** 2
        )
    )

    work["bucket"] = pd.qcut(
        work[
            "model_home_probability"
        ].rank(
            method="first"
        ),
        q=5,
        labels=False,
    )

    buckets = (
        work.groupby(
            "bucket",
            as_index=False,
        )
        .agg(
            games=(
                "actual_home_win",
                "size",
            ),

            average_model_probability=(
                "model_home_probability",
                "mean",
            ),

            actual_home_win_rate=(
                "actual_home_win",
                "mean",
            ),
        )
        .sort_values("bucket")
        .reset_index(drop=True)
    )

    buckets["bucket"] += 1

    low = float(
        buckets.iloc[0][
            "actual_home_win_rate"
        ]
    )

    high = float(
        buckets.iloc[-1][
            "actual_home_win_rate"
        ]
    )

    differences = np.diff(
        buckets[
            "actual_home_win_rate"
        ].to_numpy(
            dtype=float
        )
    )

    rising = int(
        np.sum(
            differences > 0
        )
    )

    gaps = np.abs(
        buckets[
            "average_model_probability"
        ].to_numpy(
            dtype=float
        )
        - buckets[
            "actual_home_win_rate"
        ].to_numpy(
            dtype=float
        )
    )

    weights = (
        buckets[
            "games"
        ]
        .to_numpy(
            dtype=float
        )
    )

    average_gap = float(
        np.average(
            gaps,
            weights=weights,
        )
    )

    return (
        {
            "games":
                len(work),

            "probability_win_correlation":
                correlation,

            "lowest_group_actual_win_rate":
                low,

            "highest_group_actual_win_rate":
                high,

            "high_minus_low_win_rate":
                high - low,

            "rising_bucket_steps_out_of_4":
                rising,

            "probability_error":
                probability_error,

            "average_bucket_probability_gap":
                average_gap,
        },
        buckets,
    )


def summarize(
    predictions: pd.DataFrame,
):

    rows = []
    bucket_tables = {}

    for name in VARIANT_ORDER:
        part = predictions[
            predictions["variant"] == name
        ]

        if part.empty:
            raise RuntimeError(
                f"Missing result variant: {name}"
            )

        metrics, buckets = (
            score_variant(part)
        )

        rows.append(
            {
                "variant":
                    name,
                **metrics,
            }
        )

        bucket_tables[name] = buckets

    summary = pd.DataFrame(rows)

    best_existing = (
        summary[
            summary["variant"]
            == "bullpen_plus_7d"
        ]
        .iloc[0]
    )

    summary[
        "correlation_change_vs_bullpen_7d"
    ] = (
        summary[
            "probability_win_correlation"
        ]
        - best_existing[
            "probability_win_correlation"
        ]
    )

    summary[
        "win_spread_change_vs_bullpen_7d"
    ] = (
        summary[
            "high_minus_low_win_rate"
        ]
        - best_existing[
            "high_minus_low_win_rate"
        ]
    )

    summary[
        "probability_error_change_vs_bullpen_7d"
    ] = (
        summary[
            "probability_error"
        ]
        - best_existing[
            "probability_error"
        ]
    )

    summary[
        "bucket_gap_change_vs_bullpen_7d"
    ] = (
        summary[
            "average_bucket_probability_gap"
        ]
        - best_existing[
            "average_bucket_probability_gap"
        ]
    )

    return summary, bucket_tables


def feature_coverage(
    frame: pd.DataFrame,
):

    rows = []

    groups = {
        "lineup_30d":
            LINEUP_30D_FEATURES,

        "lineup_14d":
            LINEUP_14D_FEATURES,
    }

    for group, columns in groups.items():
        for col in columns:
            rows.append(
                {
                    "group":
                        group,

                    "feature":
                        col,

                    "coverage_pct":
                        float(
                            frame[col]
                            .notna()
                            .mean()
                            * 100.0
                        ),
                }
            )

    return pd.DataFrame(rows)


def pct(value):
    if pd.isna(value):
        return ""

    return f"{float(value) * 100:.2f}%"


def num(value):
    if pd.isna(value):
        return ""

    return f"{float(value):.4f}"


def build_report(
    summary: pd.DataFrame,
    buckets: dict,
    coverage: pd.DataFrame,
    dates: list[pd.Timestamp],
):

    shown = summary.copy()

    for col in [
        "lowest_group_actual_win_rate",
        "highest_group_actual_win_rate",
        "high_minus_low_win_rate",
        "win_spread_change_vs_bullpen_7d",
        "average_bucket_probability_gap",
        "bucket_gap_change_vs_bullpen_7d",
    ]:
        shown[col] = shown[col].map(
            pct
        )

    for col in [
        "probability_win_correlation",
        "correlation_change_vs_bullpen_7d",
        "probability_error",
        "probability_error_change_vs_bullpen_7d",
    ]:
        shown[col] = shown[col].map(
            num
        )

    bucket_sections = []

    for name, table in buckets.items():
        output = table.copy()

        output[
            "average_model_probability"
        ] = output[
            "average_model_probability"
        ].map(
            pct
        )

        output[
            "actual_home_win_rate"
        ] = output[
            "actual_home_win_rate"
        ].map(
            pct
        )

        bucket_sections.append(
            "<h3>"
            + html.escape(name)
            + "</h3>"
            + output.to_html(
                index=False,
                border=0,
            )
        )

    coverage_display = (
        coverage.copy()
    )

    coverage_display[
        "coverage_pct"
    ] = coverage_display[
        "coverage_pct"
    ].map(
        lambda value:
            f"{value:.1f}%"
    )

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Moneyline Expected Lineup Test</title>
<style>
body {{
    font-family: Arial, sans-serif;
    margin: 28px;
}}
table {{
    border-collapse: collapse;
    width: 100%;
    margin-bottom: 28px;
}}
th, td {{
    border: 1px solid #bbb;
    padding: 6px 8px;
}}
</style>
</head>
<body>

<h1>MLB Moneyline Expected Lineup Test</h1>

<p>
Expected lineups are estimated only from games before each target date.
The target game's actual lineup is never used.
</p>

<p>
Evaluation:
{dates[0].strftime("%Y-%m-%d")}
through
{dates[-1].strftime("%Y-%m-%d")}
</p>

<h2>Summary</h2>

{shown.to_html(index=False, border=0)}

<h2>Probability Groups</h2>

{''.join(bucket_sections)}

<h2>Feature Availability</h2>

{coverage_display.to_html(index=False, border=0)}

</body>
</html>
"""


def main():
    print(
        "Loading prior moneyline results..."
    )

    reference, evaluation_dates = (
        load_reference()
    )

    print(
        "Loading training data..."
    )

    frame, baseline_features = (
        EXP.load_training()
    )

    print(
        "Loading Statcast data..."
    )

    # FIX:
    # raw Statcast is used by BP.build_tables(),
    # prepared Statcast is used by lineup functions.
    raw_statcast, statcast = (
        load_statcast()
    )

    print(
        "Building existing 7-day bullpen features..."
    )

    team_daily, pitcher_daily = (
        BP.build_tables(
            raw_statcast
        )
    )

    frame = BP.attach_features(
        frame,
        team_daily,
        pitcher_daily,
    )

    print(
        "Building historical starting lineups..."
    )

    historical_lineups = (
        build_historical_starting_lineups(
            statcast
        )
    )

    print(
        "Building historical batter stats..."
    )

    daily_batters = (
        build_daily_batter_history(
            statcast
        )
    )

    print(
        "Building pregame expected-lineup features..."
    )

    frame = attach_lineup_features(
        frame,
        historical_lineups,
        daily_batters,
    )

    all_experimental_features = list(
        dict.fromkeys(
            [
                *BP7_FEATURES,
                *LINEUP_BOTH_FEATURES,
            ]
        )
    )

    for col in all_experimental_features:
        if col not in frame.columns:
            frame[col] = np.nan

        frame[col] = pd.to_numeric(
            frame[col],
            errors="coerce",
        )

        bad = (
            frame[col].notna()
            & ~np.isfinite(
                frame[col]
            )
        )

        if bad.any():
            raise RuntimeError(
                f"Invalid values in feature {col}"
            )

    evaluation_start = (
        evaluation_dates[0]
    )

    development = frame[
        frame["_game_date_dt"]
        < evaluation_start
    ].copy()

    training_variants = {
        name: list(
            dict.fromkeys(
                [
                    *baseline_features,
                    *extras,
                ]
            )
        )
        for name, extras
        in NEW_VARIANTS.items()
    }

    print(
        "Selecting model settings..."
    )

    selected = select_params(
        development,
        training_variants,
    )

    print(
        "Running walk-forward lineup test..."
    )

    new_predictions = (
        predict_new_variants(
            frame,
            evaluation_dates,
            baseline_features,
            selected,
        )
    )

    reference_columns = [
        "variant",
        "game_date",
        "gamePk",
        "home_team",
        "away_team",
        "model_home_runs",
        "model_away_runs",
        "model_home_probability",
        "model_away_probability",
        "actual_home_runs",
        "actual_away_runs",
        "actual_home_win",
    ]

    reference = reference[
        reference_columns
    ].copy()

    new_predictions = (
        new_predictions[
            reference_columns
        ].copy()
    )

    predictions = pd.concat(
        [
            reference,
            new_predictions,
        ],
        ignore_index=True,
        sort=False,
    )

    summary, buckets = (
        summarize(
            predictions
        )
    )

    coverage = feature_coverage(
        frame
    )

    predictions.to_csv(
        PREDICTIONS_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    summary.to_csv(
        SUMMARY_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    REPORT_FILE.write_text(
        build_report(
            summary,
            buckets,
            coverage,
            evaluation_dates,
        ),
        encoding="utf-8",
    )

    print("")
    print(
        "Moneyline expected-lineup test complete."
    )

    print(
        f"SUMMARY: {SUMMARY_FILE}"
    )

    print(
        f"PREDICTIONS: {PREDICTIONS_FILE}"
    )

    print(
        f"REPORT: {REPORT_FILE}"
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(
            main()
        )

    except SystemExit:
        raise

    except Exception as exc:
        print(
            f"ERROR: {exc}",
            file=sys.stderr,
        )

        traceback.print_exc()

        raise SystemExit(1)