#!/usr/bin/env python3
"""Test deeper bullpen features for MLB moneyline.

Requires the completed first experiment:
    test_moneyline_feature_expansion.py

Reuses its Statcast cache and its baseline/current-bullpen predictions.
Does not modify production files or production models.

New tests:
    bullpen_plus_7d
    bullpen_plus_workload
    bullpen_plus_key_reliever
    bullpen_all_detail

Outputs at repo root:
    mlb_moneyline_bullpen_detail_test_summary.csv
    mlb_moneyline_bullpen_detail_test_predictions.csv
    mlb_moneyline_bullpen_detail_test_report.html
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
    for start in (Path.cwd().resolve(), Path(__file__).resolve().parent):
        for p in (start, *start.parents):
            if (p / "docs/win/baseball/mlb").exists():
                return p
    raise RuntimeError("Could not locate repository root")


ROOT = repo_root()
BASE = ROOT / "docs/win/baseball/mlb"
FIRST_TEST = BASE / "scripts/modeling/test_moneyline_feature_expansion.py"
REFERENCE_PREDICTIONS = ROOT / "mlb_moneyline_feature_test_predictions.csv"
CACHE_DIR = BASE / "modeling/experiments/moneyline_feature_expansion/statcast_cache"

SUMMARY_FILE = ROOT / "mlb_moneyline_bullpen_detail_test_summary.csv"
PREDICTIONS_FILE = ROOT / "mlb_moneyline_bullpen_detail_test_predictions.csv"
REPORT_FILE = ROOT / "mlb_moneyline_bullpen_detail_test_report.html"


def load_module(name: str, path: Path):
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location(name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {path}")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    return mod


EXP = load_module(
    "moneyline_feature_expansion_existing",
    FIRST_TEST,
)

TRAIN = EXP.TRAIN
JUICE = EXP.JUICE

TRAIN._log = lambda *args, **kwargs: None


CURRENT = [
    "home_bp_pa_14d",
    "away_bp_pa_14d",
    "home_bp_woba_allowed_14d",
    "away_bp_woba_allowed_14d",
    "home_bp_k_rate_14d",
    "away_bp_k_rate_14d",
    "home_bp_bb_rate_14d",
    "away_bp_bb_rate_14d",
    "home_bp_hard_rate_14d",
    "away_bp_hard_rate_14d",
    "home_bp_pitches_3d",
    "away_bp_pitches_3d",
]


PLUS_7D = [
    "home_bp_pa_7d",
    "away_bp_pa_7d",
    "home_bp_woba_allowed_7d",
    "away_bp_woba_allowed_7d",
    "home_bp_k_rate_7d",
    "away_bp_k_rate_7d",
    "home_bp_bb_rate_7d",
    "away_bp_bb_rate_7d",
    "home_bp_hard_rate_7d",
    "away_bp_hard_rate_7d",
]


PLUS_WORKLOAD = [
    "home_bp_pitches_1d",
    "away_bp_pitches_1d",
    "home_bp_pitches_2d",
    "away_bp_pitches_2d",
    "home_bp_relievers_used_1d",
    "away_bp_relievers_used_1d",
    "home_bp_relievers_used_2d",
    "away_bp_relievers_used_2d",
    "home_bp_relievers_used_3d",
    "away_bp_relievers_used_3d",
    "home_bp_back_to_back_relievers",
    "away_bp_back_to_back_relievers",
]


PLUS_KEY = [
    "home_bp_key3_used_1d",
    "away_bp_key3_used_1d",
    "home_bp_key3_pitches_1d",
    "away_bp_key3_pitches_1d",
    "home_bp_key3_pitches_2d",
    "away_bp_key3_pitches_2d",
    "home_bp_key3_pitches_3d",
    "away_bp_key3_pitches_3d",
    "home_bp_key3_back_to_back",
    "away_bp_key3_back_to_back",
    "home_bp_key3_max_pitches_1d",
    "away_bp_key3_max_pitches_1d",
]


ALL_EXTRA = list(
    dict.fromkeys(
        [
            *CURRENT,
            *PLUS_7D,
            *PLUS_WORKLOAD,
            *PLUS_KEY,
        ]
    )
)


NEW_VARIANTS = {
    "bullpen_plus_7d": [
        *CURRENT,
        *PLUS_7D,
    ],
    "bullpen_plus_workload": [
        *CURRENT,
        *PLUS_WORKLOAD,
    ],
    "bullpen_plus_key_reliever": [
        *CURRENT,
        *PLUS_KEY,
    ],
    "bullpen_all_detail": ALL_EXTRA,
}


VARIANT_ORDER = [
    "baseline",
    "bullpen_current",
    "bullpen_plus_7d",
    "bullpen_plus_workload",
    "bullpen_plus_key_reliever",
    "bullpen_all_detail",
]


def load_reference():
    if not REFERENCE_PREDICTIONS.exists():
        raise RuntimeError(
            f"Missing prior result file: {REFERENCE_PREDICTIONS}"
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
        c
        for c in required
        if c not in df.columns
    ]

    if missing:
        raise RuntimeError(
            f"Prior predictions missing columns: {missing}"
        )

    baseline = df[
        df["variant"] == "baseline"
    ].copy()

    bullpen = df[
        df["variant"] == "bullpen"
    ].copy()

    if baseline.empty or bullpen.empty:
        raise RuntimeError(
            "Prior test must contain baseline and bullpen variants"
        )

    baseline["variant"] = "baseline"
    bullpen["variant"] = "bullpen_current"

    ref = pd.concat(
        [
            baseline,
            bullpen,
        ],
        ignore_index=True,
        sort=False,
    )

    ref["_date"] = pd.to_datetime(
        ref["game_date"],
        errors="coerce",
    ).dt.normalize()

    if ref["_date"].isna().any():
        raise RuntimeError(
            "Prior predictions contain invalid game_date"
        )

    dates = [
        pd.Timestamp(x).normalize()
        for x in sorted(
            ref["_date"].unique()
        )
    ]

    return ref, dates


def load_training():
    frame, baseline_features = EXP.load_training()

    return frame, baseline_features


def load_cache():
    paths = sorted(
        CACHE_DIR.glob("*.parquet")
    )

    if not paths:
        raise RuntimeError(
            "Statcast cache not found. "
            "The first moneyline feature test must be run first."
        )

    frames = []

    for i, path in enumerate(
        paths,
        1,
    ):
        print(
            f"CACHE {i}/{len(paths)} {path.name}"
        )

        frames.append(
            EXP.read_parquet(path)
        )

    raw = pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )

    if raw.empty:
        raise RuntimeError(
            "Statcast cache is empty"
        )

    raw["_game_date_dt"] = pd.to_datetime(
        raw["game_date"],
        errors="coerce",
    ).dt.normalize()

    raw["_gamePk"] = raw[
        "game_pk"
    ].map(
        EXP.normalize_gamepk
    )

    raw["_home_code"] = raw[
        "home_team"
    ].map(
        EXP.canonical_team
    )

    raw["_away_code"] = raw[
        "away_team"
    ].map(
        EXP.canonical_team
    )

    bad = (
        raw["_game_date_dt"].isna()
        | raw["_gamePk"].isna()
        | raw["_home_code"].isna()
        | raw["_away_code"].isna()
    )

    if bad.any():
        raise RuntimeError(
            "Statcast cache contains invalid game/date/team rows"
        )

    keys = [
        c
        for c in (
            "_gamePk",
            "at_bat_number",
            "pitch_number",
            "pitcher",
            "batter",
        )
        if c in raw.columns
    ]

    return raw.drop_duplicates(
        subset=keys,
        keep="last",
    ).reset_index(
        drop=True
    )


def build_tables(raw):
    statcast = EXP.prepare_statcast(
        raw
    )

    bp = statcast[
        statcast["_is_bullpen"]
    ].copy()

    if bp.empty:
        raise RuntimeError(
            "No bullpen pitches found"
        )

    team_workload = (
        bp.groupby(
            [
                "_game_date_dt",
                "_pitching_team",
            ],
            as_index=False,
        )
        .agg(
            bp_pitches=(
                "_pitcher",
                "size",
            ),
            bp_relievers_used=(
                "_pitcher",
                "nunique",
            ),
        )
        .rename(
            columns={
                "_pitching_team":
                    "team"
            }
        )
    )

    pitcher_daily = (
        bp.groupby(
            [
                "_game_date_dt",
                "_pitching_team",
                "_pitcher",
            ],
            as_index=False,
        )
        .size()
        .rename(
            columns={
                "_pitching_team":
                    "team",
                "_pitcher":
                    "pitcher",
                "size":
                    "pitches",
            }
        )
    )

    pa = bp[
        bp["_is_pa"]
    ].copy()

    pa["_woba_sum"] = (
        pa["_woba_value"]
        .fillna(0.0)
    )

    pa["_woba_n"] = (
        pa["_woba_value"]
        .notna()
        .astype(int)
    )

    performance = (
        pa.groupby(
            [
                "_game_date_dt",
                "_pitching_team",
            ],
            as_index=False,
        )
        .agg(
            bp_pa=(
                "_is_pa",
                "sum",
            ),
            bp_woba_sum=(
                "_woba_sum",
                "sum",
            ),
            bp_woba_n=(
                "_woba_n",
                "sum",
            ),
            bp_k=(
                "_is_k",
                "sum",
            ),
            bp_bb=(
                "_is_bb",
                "sum",
            ),
            bp_bbe=(
                "_is_bbe",
                "sum",
            ),
            bp_hard=(
                "_is_hard",
                "sum",
            ),
        )
        .rename(
            columns={
                "_pitching_team":
                    "team"
            }
        )
    )

    team_daily = team_workload.merge(
        performance,
        on=[
            "_game_date_dt",
            "team",
        ],
        how="left",
        validate="one_to_one",
    )

    return (
        team_daily,
        pitcher_daily,
    )


def perf(
    team_daily,
    team,
    target_date,
    days,
):
    start = (
        target_date
        - pd.Timedelta(
            days=days
        )
    )

    part = team_daily[
        (
            team_daily["team"]
            == team
        )
        & (
            team_daily["_game_date_dt"]
            >= start
        )
        & (
            team_daily["_game_date_dt"]
            < target_date
        )
    ]

    if part.empty:
        return {
            "pa":
                0.0,
            "woba":
                np.nan,
            "k":
                np.nan,
            "bb":
                np.nan,
            "hard":
                np.nan,
        }

    pa = float(
        part["bp_pa"]
        .fillna(0)
        .sum()
    )

    woba_n = float(
        part["bp_woba_n"]
        .fillna(0)
        .sum()
    )

    bbe = float(
        part["bp_bbe"]
        .fillna(0)
        .sum()
    )

    return {
        "pa":
            pa,

        "woba":
            EXP.safe_ratio(
                float(
                    part["bp_woba_sum"]
                    .fillna(0)
                    .sum()
                ),
                woba_n,
            ),

        "k":
            EXP.safe_ratio(
                float(
                    part["bp_k"]
                    .fillna(0)
                    .sum()
                ),
                pa,
            ),

        "bb":
            EXP.safe_ratio(
                float(
                    part["bp_bb"]
                    .fillna(0)
                    .sum()
                ),
                pa,
            ),

        "hard":
            EXP.safe_ratio(
                float(
                    part["bp_hard"]
                    .fillna(0)
                    .sum()
                ),
                bbe,
            ),
    }


def workload(
    team_daily,
    team,
    target_date,
    days,
):
    start = (
        target_date
        - pd.Timedelta(
            days=days
        )
    )

    part = team_daily[
        (
            team_daily["team"]
            == team
        )
        & (
            team_daily["_game_date_dt"]
            >= start
        )
        & (
            team_daily["_game_date_dt"]
            < target_date
        )
    ]

    if part.empty:
        return {
            "pitches":
                0.0,
            "relievers":
                0.0,
        }

    return {
        "pitches":
            float(
                part["bp_pitches"]
                .fillna(0)
                .sum()
            ),

        "relievers":
            float(
                part["bp_relievers_used"]
                .fillna(0)
                .sum()
            ),
    }


def exact_day(
    pitcher_daily,
    team,
    target_date,
    days_back,
):
    day = (
        target_date
        - pd.Timedelta(
            days=days_back
        )
    )

    return pitcher_daily[
        (
            pitcher_daily["team"]
            == team
        )
        & (
            pitcher_daily["_game_date_dt"]
            == day
        )
    ]


def back_to_back_count(
    pitcher_daily,
    team,
    target_date,
):
    d1 = exact_day(
        pitcher_daily,
        team,
        target_date,
        1,
    )

    d2 = exact_day(
        pitcher_daily,
        team,
        target_date,
        2,
    )

    return float(
        len(
            set(
                d1["pitcher"]
            )
            & set(
                d2["pitcher"]
            )
        )
    )


def key3(
    pitcher_daily,
    team,
    target_date,
):
    start = (
        target_date
        - pd.Timedelta(
            days=30
        )
    )

    prior = pitcher_daily[
        (
            pitcher_daily["team"]
            == team
        )
        & (
            pitcher_daily["_game_date_dt"]
            >= start
        )
        & (
            pitcher_daily["_game_date_dt"]
            < target_date
        )
    ]

    if prior.empty:
        return {
            "used1":
                0.0,
            "p1":
                0.0,
            "p2":
                0.0,
            "p3":
                0.0,
            "b2b":
                0.0,
            "max1":
                0.0,
        }

    leaders = (
        prior.groupby(
            "pitcher",
            as_index=False,
        )["pitches"]
        .sum()
        .sort_values(
            [
                "pitches",
                "pitcher",
            ],
            ascending=[
                False,
                True,
            ],
        )
        .head(3)
    )

    ids = set(
        leaders["pitcher"]
    )

    def window(days):
        start_date = (
            target_date
            - pd.Timedelta(
                days=days
            )
        )

        return pitcher_daily[
            (
                pitcher_daily["team"]
                == team
            )
            & (
                pitcher_daily["pitcher"]
                .isin(ids)
            )
            & (
                pitcher_daily["_game_date_dt"]
                >= start_date
            )
            & (
                pitcher_daily["_game_date_dt"]
                < target_date
            )
        ]

    one = window(1)
    two = window(2)
    three = window(3)

    d1 = exact_day(
        pitcher_daily,
        team,
        target_date,
        1,
    )

    d2 = exact_day(
        pitcher_daily,
        team,
        target_date,
        2,
    )

    ids1 = set(
        d1.loc[
            d1["pitcher"].isin(ids),
            "pitcher",
        ]
    )

    ids2 = set(
        d2.loc[
            d2["pitcher"].isin(ids),
            "pitcher",
        ]
    )

    return {
        "used1":
            float(
                one["pitcher"]
                .nunique()
            ),

        "p1":
            float(
                one["pitches"]
                .sum()
            ),

        "p2":
            float(
                two["pitches"]
                .sum()
            ),

        "p3":
            float(
                three["pitches"]
                .sum()
            ),

        "b2b":
            float(
                len(
                    ids1
                    & ids2
                )
            ),

        "max1":
            (
                float(
                    one["pitches"]
                    .max()
                )
                if not one.empty
                else 0.0
            ),
    }


def attach_features(
    frame,
    team_daily,
    pitcher_daily,
):
    out = frame.copy()

    values = {
        c: []
        for c in ALL_EXTRA
    }

    cache = {}

    for _, row in out.iterrows():
        date = pd.Timestamp(
            row["_game_date_dt"]
        ).normalize()

        for side in (
            "home",
            "away",
        ):
            team = row[
                f"_{side}_code"
            ]

            key = (
                date,
                team,
            )

            if key not in cache:
                cache[key] = {
                    "p14":
                        perf(
                            team_daily,
                            team,
                            date,
                            14,
                        ),

                    "p7":
                        perf(
                            team_daily,
                            team,
                            date,
                            7,
                        ),

                    "w1":
                        workload(
                            team_daily,
                            team,
                            date,
                            1,
                        ),

                    "w2":
                        workload(
                            team_daily,
                            team,
                            date,
                            2,
                        ),

                    "w3":
                        workload(
                            team_daily,
                            team,
                            date,
                            3,
                        ),

                    "b2b":
                        back_to_back_count(
                            pitcher_daily,
                            team,
                            date,
                        ),

                    "key3":
                        key3(
                            pitcher_daily,
                            team,
                            date,
                        ),
                }

            d = cache[key]

            p14 = d["p14"]
            p7 = d["p7"]

            w1 = d["w1"]
            w2 = d["w2"]
            w3 = d["w3"]

            k3 = d["key3"]

            mapping = {
                f"{side}_bp_pa_14d":
                    p14["pa"],

                f"{side}_bp_woba_allowed_14d":
                    p14["woba"],

                f"{side}_bp_k_rate_14d":
                    p14["k"],

                f"{side}_bp_bb_rate_14d":
                    p14["bb"],

                f"{side}_bp_hard_rate_14d":
                    p14["hard"],

                f"{side}_bp_pitches_3d":
                    w3["pitches"],

                f"{side}_bp_pa_7d":
                    p7["pa"],

                f"{side}_bp_woba_allowed_7d":
                    p7["woba"],

                f"{side}_bp_k_rate_7d":
                    p7["k"],

                f"{side}_bp_bb_rate_7d":
                    p7["bb"],

                f"{side}_bp_hard_rate_7d":
                    p7["hard"],

                f"{side}_bp_pitches_1d":
                    w1["pitches"],

                f"{side}_bp_pitches_2d":
                    w2["pitches"],

                f"{side}_bp_relievers_used_1d":
                    w1["relievers"],

                f"{side}_bp_relievers_used_2d":
                    w2["relievers"],

                f"{side}_bp_relievers_used_3d":
                    w3["relievers"],

                f"{side}_bp_back_to_back_relievers":
                    d["b2b"],

                f"{side}_bp_key3_used_1d":
                    k3["used1"],

                f"{side}_bp_key3_pitches_1d":
                    k3["p1"],

                f"{side}_bp_key3_pitches_2d":
                    k3["p2"],

                f"{side}_bp_key3_pitches_3d":
                    k3["p3"],

                f"{side}_bp_key3_back_to_back":
                    k3["b2b"],

                f"{side}_bp_key3_max_pitches_1d":
                    k3["max1"],
            }

            for col, value in mapping.items():
                values[
                    col
                ].append(
                    value
                )

    for col, vals in values.items():
        out[col] = pd.to_numeric(
            pd.Series(
                vals,
                index=out.index,
            ),
            errors="coerce",
        )

    return out


def select_params(
    development,
    variants,
):
    split = TRAIN.chronological_date_split(
        development
    )

    selected = {}

    for i, (
        name,
        features,
    ) in enumerate(
        variants.items(),
        1,
    ):
        print(
            f"TUNING {i}/{len(variants)} {name}"
        )

        hp, hs = TRAIN.select_hyperparameters(
            split["train"],
            split["validation"],
            features,
            "target_home_runs",
            f"{name}_home",
        )

        ap, a_s = TRAIN.select_hyperparameters(
            split["train"],
            split["validation"],
            features,
            "target_away_runs",
            f"{name}_away",
        )

        selected[name] = {
            "home":
                hp,
            "away":
                ap,
            "home_score":
                hs,
            "away_score":
                a_s,
        }

    return selected


def fit(
    prior,
    features,
    target,
    params,
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
        prior[
            features
        ],
        prior[
            target
        ],
    )

    return model


def predict_new(
    frame,
    dates,
    baseline_features,
    selected,
):
    rows = []

    variants = {
        name: [
            *baseline_features,
            *extras,
        ]
        for name, extras
        in NEW_VARIANTS.items()
    }

    for date_i, target_date in enumerate(
        dates,
        1,
    ):
        print(
            f"TEST DATE {date_i}/{len(dates)} "
            f"{target_date.date()}"
        )

        prior = frame[
            frame["_game_date_dt"]
            < target_date
        ]

        current = frame[
            frame["_game_date_dt"]
            == target_date
        ]

        if current.empty:
            continue

        for name, features in variants.items():
            hm = fit(
                prior,
                features,
                "target_home_runs",
                selected[
                    name
                ]["home"],
            )

            am = fit(
                prior,
                features,
                "target_away_runs",
                selected[
                    name
                ]["away"],
            )

            hr = np.asarray(
                hm.predict(
                    current[
                        features
                    ]
                ),
                dtype=float,
            )

            ar = np.asarray(
                am.predict(
                    current[
                        features
                    ]
                ),
                dtype=float,
            )

            if (
                np.any(
                    ~np.isfinite(
                        hr
                    )
                )
                or np.any(
                    hr < 0
                )
            ):
                raise RuntimeError(
                    f"{name} invalid home-run predictions"
                )

            if (
                np.any(
                    ~np.isfinite(
                        ar
                    )
                )
                or np.any(
                    ar < 0
                )
            ):
                raise RuntimeError(
                    f"{name} invalid away-run predictions"
                )

            for pos, (
                _,
                game,
            ) in enumerate(
                current.iterrows()
            ):
                ah = float(
                    game[
                        "target_home_runs"
                    ]
                )

                aa = float(
                    game[
                        "target_away_runs"
                    ]
                )

                if ah == aa:
                    continue

                hp, ap, _ = (
                    JUICE.moneyline_probabilities(
                        float(
                            hr[pos]
                        ),
                        float(
                            ar[pos]
                        ),
                    )
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
                            game[
                                "gamePk"
                            ],

                        "home_team":
                            game[
                                "home_team"
                            ],

                        "away_team":
                            game[
                                "away_team"
                            ],

                        "model_home_runs":
                            float(
                                hr[pos]
                            ),

                        "model_away_runs":
                            float(
                                ar[pos]
                            ),

                        "model_home_probability":
                            float(
                                hp
                            ),

                        "model_away_probability":
                            float(
                                ap
                            ),

                        "actual_home_runs":
                            ah,

                        "actual_away_runs":
                            aa,

                        "actual_home_win":
                            (
                                1.0
                                if ah > aa
                                else 0.0
                            ),
                    }
                )

    return pd.DataFrame(
        rows
    )


def score(part):
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

    p = work[
        "model_home_probability"
    ].to_numpy(
        float
    )

    y = work[
        "actual_home_win"
    ].to_numpy(
        float
    )

    corr = (
        float(
            np.corrcoef(
                p,
                y,
            )[0, 1]
        )
        if np.std(p)
        and np.std(y)
        else np.nan
    )

    error = float(
        np.mean(
            (
                p
                - y
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
        5,
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
        .sort_values(
            "bucket"
        )
        .reset_index(
            drop=True
        )
    )

    buckets[
        "bucket"
    ] += 1

    low = float(
        buckets.iloc[
            0
        ][
            "actual_home_win_rate"
        ]
    )

    high = float(
        buckets.iloc[
            -1
        ][
            "actual_home_win_rate"
        ]
    )

    rising = int(
        np.sum(
            np.diff(
                buckets[
                    "actual_home_win_rate"
                ].to_numpy(
                    float
                )
            )
            > 0
        )
    )

    gaps = np.abs(
        buckets[
            "average_model_probability"
        ].to_numpy(
            float
        )
        - buckets[
            "actual_home_win_rate"
        ].to_numpy(
            float
        )
    )

    gap = float(
        np.average(
            gaps,
            weights=buckets[
                "games"
            ].to_numpy(
                float
            ),
        )
    )

    return (
        {
            "games":
                len(work),

            "probability_win_correlation":
                corr,

            "lowest_group_actual_win_rate":
                low,

            "highest_group_actual_win_rate":
                high,

            "high_minus_low_win_rate":
                high - low,

            "rising_bucket_steps_out_of_4":
                rising,

            "probability_error":
                error,

            "average_bucket_probability_gap":
                gap,
        },
        buckets,
    )


def summarize(predictions):
    rows = []

    bucket_tables = {}

    for name in VARIANT_ORDER:
        part = predictions[
            predictions[
                "variant"
            ]
            == name
        ]

        if part.empty:
            raise RuntimeError(
                f"Missing variant: {name}"
            )

        metrics, buckets = score(
            part
        )

        rows.append(
            {
                "variant":
                    name,
                **metrics,
            }
        )

        bucket_tables[
            name
        ] = buckets

    summary = pd.DataFrame(
        rows
    )

    current = summary[
        summary[
            "variant"
        ]
        == "bullpen_current"
    ].iloc[0]

    summary[
        "correlation_change_vs_current"
    ] = (
        summary[
            "probability_win_correlation"
        ]
        - current[
            "probability_win_correlation"
        ]
    )

    summary[
        "win_spread_change_vs_current"
    ] = (
        summary[
            "high_minus_low_win_rate"
        ]
        - current[
            "high_minus_low_win_rate"
        ]
    )

    summary[
        "probability_error_change_vs_current"
    ] = (
        summary[
            "probability_error"
        ]
        - current[
            "probability_error"
        ]
    )

    summary[
        "bucket_gap_change_vs_current"
    ] = (
        summary[
            "average_bucket_probability_gap"
        ]
        - current[
            "average_bucket_probability_gap"
        ]
    )

    return (
        summary,
        bucket_tables,
    )


def coverage(frame):
    groups = {
        "current":
            CURRENT,

        "plus_7d":
            PLUS_7D,

        "plus_workload":
            PLUS_WORKLOAD,

        "plus_key_reliever":
            PLUS_KEY,
    }

    rows = []

    for group, cols in groups.items():
        for col in cols:
            rows.append(
                {
                    "group":
                        group,

                    "feature":
                        col,

                    "coverage_pct":
                        float(
                            frame[
                                col
                            ]
                            .notna()
                            .mean()
                            * 100
                        ),
                }
            )

    return pd.DataFrame(
        rows
    )


def pct(v):
    if pd.isna(v):
        return ""

    return (
        f"{float(v) * 100:.2f}%"
    )


def num(v):
    if pd.isna(v):
        return ""

    return (
        f"{float(v):.4f}"
    )


def report_html(
    summary,
    buckets,
    feature_coverage,
    dates,
):
    shown = summary.copy()

    for col in [
        "lowest_group_actual_win_rate",
        "highest_group_actual_win_rate",
        "high_minus_low_win_rate",
        "win_spread_change_vs_current",
        "average_bucket_probability_gap",
        "bucket_gap_change_vs_current",
    ]:
        shown[
            col
        ] = shown[
            col
        ].map(
            pct
        )

    for col in [
        "probability_win_correlation",
        "correlation_change_vs_current",
        "probability_error",
        "probability_error_change_vs_current",
    ]:
        shown[
            col
        ] = shown[
            col
        ].map(
            num
        )

    bucket_html = []

    for name, table in buckets.items():
        t = table.copy()

        t[
            "average_model_probability"
        ] = t[
            "average_model_probability"
        ].map(
            pct
        )

        t[
            "actual_home_win_rate"
        ] = t[
            "actual_home_win_rate"
        ].map(
            pct
        )

        bucket_html.append(
            f"<h3>{html.escape(name)}</h3>"
            + t.to_html(
                index=False,
                border=0,
            )
        )

    cov = feature_coverage.copy()

    cov[
        "coverage_pct"
    ] = cov[
        "coverage_pct"
    ].map(
        lambda x:
            f"{x:.1f}%"
    )

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Moneyline Bullpen Detail Test</title>
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

<h1>MLB Moneyline Bullpen Detail Test</h1>

<p>
Comparison is against the current bullpen version
from the first feature test.
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

{''.join(bucket_html)}

<h2>Feature Availability</h2>

{cov.to_html(index=False, border=0)}

</body>
</html>
"""


def main():
    print(
        "Loading prior results..."
    )

    reference, dates = load_reference()

    print(
        "Loading training data..."
    )

    frame, baseline_features = (
        load_training()
    )

    print(
        "Loading existing Statcast cache..."
    )

    raw = load_cache()

    print(
        f"Statcast rows: {len(raw):,}"
    )

    print(
        "Building deeper bullpen features..."
    )

    (
        team_daily,
        pitcher_daily,
    ) = build_tables(
        raw
    )

    frame = attach_features(
        frame,
        team_daily,
        pitcher_daily,
    )

    for col in ALL_EXTRA:
        frame[
            col
        ] = pd.to_numeric(
            frame[
                col
            ],
            errors="coerce",
        )

        bad = (
            frame[
                col
            ].notna()
            & ~np.isfinite(
                frame[
                    col
                ]
            )
        )

        if bad.any():
            raise RuntimeError(
                f"Invalid values in {col}"
            )

    eval_start = dates[0]

    development = frame[
        frame[
            "_game_date_dt"
        ]
        < eval_start
    ].copy()

    training_variants = {
        name: [
            *baseline_features,
            *extras,
        ]
        for name, extras
        in NEW_VARIANTS.items()
    }

    print(
        "Selecting settings for new bullpen variants..."
    )

    selected = select_params(
        development,
        training_variants,
    )

    print(
        "Running walk-forward moneyline test..."
    )

    new_predictions = predict_new(
        frame,
        dates,
        baseline_features,
        selected,
    )

    ref_cols = [
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
        ref_cols
    ].copy()

    new_predictions = new_predictions[
        ref_cols
    ].copy()

    predictions = pd.concat(
        [
            reference,
            new_predictions,
        ],
        ignore_index=True,
        sort=False,
    )

    summary, buckets = summarize(
        predictions
    )

    feature_coverage = coverage(
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
        report_html(
            summary,
            buckets,
            feature_coverage,
            dates,
        ),
        encoding="utf-8",
    )

    print("")
    print(
        "Moneyline bullpen detail test complete."
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

        raise SystemExit(
            1
        )