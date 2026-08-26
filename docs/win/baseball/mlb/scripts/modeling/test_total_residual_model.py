#!/usr/bin/env python3
"""Leakage-safe experiment: correct current MLB total projection with a residual model.

Baseline:
    baseline_total = home_model_prediction + away_model_prediction

Residual target:
    actual_total - baseline_total

Experimental total:
    adjusted_total = baseline_total + predicted_residual

No production files or models are modified.
"""

from __future__ import annotations

import importlib.util
import warnings
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_poisson_deviance,
)

warnings.filterwarnings(
    "ignore",
    message=r".*sklearn\.utils\.parallel\.delayed.*",
    category=UserWarning,
)


def repo_root() -> Path:
    for start in (Path.cwd().resolve(), Path(__file__).resolve().parent):
        for p in (start, *start.parents):
            if (p / "docs/win/baseball/mlb").exists():
                return p
    raise RuntimeError("Could not locate repository root")


ROOT = repo_root()
BASE = ROOT / "docs/win/baseball/mlb"

TOTAL_TEST_SCRIPT = BASE / "scripts/modeling/test_total_model.py"

SUMMARY_FILE = ROOT / "mlb_total_residual_model_summary.csv"
BUCKET_FILE = ROOT / "mlb_total_residual_model_buckets.csv"
PREDICTIONS_FILE = ROOT / "mlb_total_residual_model_predictions.csv"
REPORT_FILE = ROOT / "mlb_total_residual_model_report.html"

EPS = 1e-12
MIN_TOTAL = 0.05


def load_module(name: str, path: Path):
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {path}")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TM = load_module("total_model_base", TOTAL_TEST_SCRIPT)

TRAIN = TM.TRAIN
JUICE = TM.JUICE
BT = TM.BT

TRAIN._log = lambda *args, **kwargs: None


def fit_poisson(frame, features, target, params):
    model = HistGradientBoostingRegressor(
        loss="poisson",
        learning_rate=params["learning_rate"],
        max_leaf_nodes=params["max_leaf_nodes"],
        min_samples_leaf=params["min_samples_leaf"],
        l2_regularization=params["l2_regularization"],
        random_state=TRAIN.RANDOM_STATE,
    )
    model.fit(frame[features], frame[target])
    return model


def tune_baseline_models(split, features):
    print("Tuning home baseline once...")
    home_params, home_score = TRAIN.select_hyperparameters(
        split["train"],
        split["validation"],
        features,
        "target_home_runs",
        "home_runs",
    )
    print(
        f"home_runs: validation_poisson={home_score:.6f} "
        f"params={home_params}"
    )

    print("Tuning away baseline once...")
    away_params, away_score = TRAIN.select_hyperparameters(
        split["train"],
        split["validation"],
        features,
        "target_away_runs",
        "away_runs",
    )
    print(
        f"away_runs: validation_poisson={away_score:.6f} "
        f"params={away_params}"
    )

    # Seed models do not train on validation outcomes.
    seed_home = fit_poisson(
        split["train"],
        features,
        "target_home_runs",
        home_params,
    )
    seed_away = fit_poisson(
        split["train"],
        features,
        "target_away_runs",
        away_params,
    )

    # Final baseline models may use train + validation because test remains untouched.
    final_home = TRAIN.fit_final_model(
        split["train"],
        split["validation"],
        features,
        "target_home_runs",
        home_params,
    )
    final_away = TRAIN.fit_final_model(
        split["train"],
        split["validation"],
        features,
        "target_away_runs",
        away_params,
    )

    return seed_home, seed_away, final_home, final_away


def add_baseline_predictions(frame, home_model, away_model, features):
    out = frame.copy()

    out["_baseline_home"] = home_model.predict(out[features])
    out["_baseline_away"] = away_model.predict(out[features])
    out["_baseline_total"] = out["_baseline_home"] + out["_baseline_away"]

    for col in (
        "_baseline_home",
        "_baseline_away",
        "_baseline_total",
    ):
        values = pd.to_numeric(out[col], errors="coerce")
        bad = values.isna() | ~np.isfinite(values) | (values < 0)

        if bad.any():
            raise RuntimeError(f"Invalid baseline predictions in {col}")

    return out


def numeric(frame, col):
    if col not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype=float)

    return pd.to_numeric(frame[col], errors="coerce")


def venue_levels_from(frame):
    venue = pd.to_numeric(frame["venue_id"], errors="coerce")

    return sorted(
        {
            str(int(v))
            for v in venue.dropna().unique()
        }
    )


def residual_features(frame, venue_levels):
    """Build compact total-specific feature structure from existing safe inputs."""

    x = pd.DataFrame(index=frame.index)

    x["baseline_total"] = numeric(frame, "_baseline_total")
    x["baseline_home"] = numeric(frame, "_baseline_home")
    x["baseline_away"] = numeric(frame, "_baseline_away")

    x["baseline_margin_abs"] = (
        x["baseline_home"] - x["baseline_away"]
    ).abs()

    x["dratings_total"] = numeric(
        frame,
        "dratings_total_projected_runs",
    )
    x["dratings_home"] = numeric(
        frame,
        "dratings_home_projected_runs",
    )
    x["dratings_away"] = numeric(
        frame,
        "dratings_away_projected_runs",
    )

    x["dratings_margin_abs"] = (
        x["dratings_home"] - x["dratings_away"]
    ).abs()

    x["baseline_minus_dratings"] = (
        x["baseline_total"] - x["dratings_total"]
    )

    pair_sums = {
        "sp_pitch_quality_sum": (
            "home_sp_pitch_quality_plus",
            "away_sp_pitch_quality_plus",
        ),
        "sp_command_sum": (
            "home_sp_command_plus",
            "away_sp_command_plus",
        ),
        "sp_xera_sum": (
            "home_sp_xera",
            "away_sp_xera",
        ),
        "sp_xera_30d_sum": (
            "home_sp_xera_30d",
            "away_sp_xera_30d",
        ),
        "sp_xwoba_sum": (
            "home_sp_xwoba",
            "away_sp_xwoba",
        ),
        "sp_xwoba_30d_sum": (
            "home_sp_xwoba_30d",
            "away_sp_xwoba_30d",
        ),
        "sp_pitches_sum": (
            "home_sp_pitches",
            "away_sp_pitches",
        ),
        "sp_pitches_30d_sum": (
            "home_sp_pitches_30d",
            "away_sp_pitches_30d",
        ),
        "bp_pa_14d_sum": (
            "home_bp_pa_14d",
            "away_bp_pa_14d",
        ),
        "bp_woba_14d_sum": (
            "home_bp_woba_allowed_14d",
            "away_bp_woba_allowed_14d",
        ),
        "bp_k_14d_sum": (
            "home_bp_k_rate_14d",
            "away_bp_k_rate_14d",
        ),
        "bp_bb_14d_sum": (
            "home_bp_bb_rate_14d",
            "away_bp_bb_rate_14d",
        ),
        "bp_hard_14d_sum": (
            "home_bp_hard_rate_14d",
            "away_bp_hard_rate_14d",
        ),
        "bp_pitches_3d_sum": (
            "home_bp_pitches_3d",
            "away_bp_pitches_3d",
        ),
        "bp_pa_7d_sum": (
            "home_bp_pa_7d",
            "away_bp_pa_7d",
        ),
        "bp_woba_7d_sum": (
            "home_bp_woba_allowed_7d",
            "away_bp_woba_allowed_7d",
        ),
        "bp_k_7d_sum": (
            "home_bp_k_rate_7d",
            "away_bp_k_rate_7d",
        ),
        "bp_bb_7d_sum": (
            "home_bp_bb_rate_7d",
            "away_bp_bb_rate_7d",
        ),
        "bp_hard_7d_sum": (
            "home_bp_hard_rate_7d",
            "away_bp_hard_rate_7d",
        ),
    }

    for output, (home, away) in pair_sums.items():
        x[output] = numeric(frame, home) + numeric(frame, away)

    x["sp_velo_delta_abs_sum"] = (
        numeric(frame, "home_sp_velo_delta_30d").abs()
        + numeric(frame, "away_sp_velo_delta_30d").abs()
    )

    # Only use weather if it already survived the production training builder's
    # pregame provenance checks. Do not read raw historical weather files here.
    for col in TRAIN.OPTIONAL_NUMERIC_FEATURE_COLUMNS:
        if col in frame.columns:
            x[col] = numeric(frame, col)

    day_night = (
        frame["day_night"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    x["is_night"] = (day_night == "night").astype(float)

    venue = (
        pd.to_numeric(frame["venue_id"], errors="coerce")
        .round()
        .astype("Int64")
        .astype("string")
    )

    for level in venue_levels:
        x[f"venue_{level}"] = (venue == level).astype(float)

    return x


def split_residual_training(frame):
    dates = np.array(
        sorted(
            frame["_game_date_dt"]
            .drop_duplicates()
            .tolist()
        )
    )

    if len(dates) < 2:
        raise RuntimeError(
            "Residual training requires at least two dates"
        )

    cut = int(np.floor(len(dates) * 0.70))
    cut = max(1, min(cut, len(dates) - 1))

    train_dates = set(dates[:cut])
    validation_dates = set(dates[cut:])

    train = frame[
        frame["_game_date_dt"].isin(train_dates)
    ].copy()

    validation = frame[
        frame["_game_date_dt"].isin(validation_dates)
    ].copy()

    if train.empty or validation.empty:
        raise RuntimeError(
            "Residual chronological split produced an empty partition"
        )

    return train, validation


def tune_residual_model(train, validation, feature_columns):
    best_params = None
    best_mae = np.inf
    count = 0

    for params in TRAIN.hyperparameter_candidates():
        count += 1

        model = HistGradientBoostingRegressor(
            loss="squared_error",
            learning_rate=params["learning_rate"],
            max_leaf_nodes=params["max_leaf_nodes"],
            min_samples_leaf=params["min_samples_leaf"],
            l2_regularization=params["l2_regularization"],
            random_state=TRAIN.RANDOM_STATE,
        )

        model.fit(
            train[feature_columns],
            train["_residual_target"],
        )

        pred = model.predict(
            validation[feature_columns]
        )

        mae = float(
            mean_absolute_error(
                validation["_residual_target"],
                pred,
            )
        )

        if mae < best_mae:
            best_mae = mae
            best_params = dict(params)

    if count != 81:
        raise RuntimeError(
            f"Expected 81 residual candidates; evaluated {count}"
        )

    if best_params is None:
        raise RuntimeError(
            "Residual hyperparameter selection failed"
        )

    return best_params, best_mae


def fit_final_residual_model(frame, feature_columns, params):
    model = HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=params["learning_rate"],
        max_leaf_nodes=params["max_leaf_nodes"],
        min_samples_leaf=params["min_samples_leaf"],
        l2_regularization=params["l2_regularization"],
        random_state=TRAIN.RANDOM_STATE,
    )

    model.fit(
        frame[feature_columns],
        frame["_residual_target"],
    )

    return model


def prepare_residual_model(
    validation_frame,
    seed_home,
    seed_away,
    baseline_features,
    venue_levels,
):
    validation = add_baseline_predictions(
        validation_frame,
        seed_home,
        seed_away,
        baseline_features,
    )

    validation["_residual_target"] = (
        validation["target_home_runs"]
        + validation["target_away_runs"]
        - validation["_baseline_total"]
    )

    feature_frame = residual_features(
        validation,
        venue_levels,
    )

    feature_columns = list(feature_frame.columns)

    residual_frame = pd.concat(
        [
            validation[
                [
                    "_game_date_dt",
                    "game_id",
                    "_residual_target",
                ]
            ].reset_index(drop=True),
            feature_frame.reset_index(drop=True),
        ],
        axis=1,
    )

    residual_train, residual_validation = (
        split_residual_training(
            residual_frame
        )
    )

    print(
        "Residual data: "
        f"train={len(residual_train):,}, "
        f"validation={len(residual_validation):,}, "
        f"features={len(feature_columns)}"
    )

    print("Tuning residual model once...")

    params, validation_mae = tune_residual_model(
        residual_train,
        residual_validation,
        feature_columns,
    )

    print(
        f"residual_model: validation_mae={validation_mae:.6f} "
        f"params={params}"
    )

    model = fit_final_residual_model(
        residual_frame,
        feature_columns,
        params,
    )

    mean_residual = float(
        residual_frame["_residual_target"].mean()
    )

    print(
        f"Historical mean residual correction: "
        f"{mean_residual:+.6f} runs"
    )

    return model, feature_columns, mean_residual


def sportsbook_test_rows(test):
    return TM.sportsbook_test_rows(test)


def grade_total(side, line, actual):
    margin = actual - line

    if abs(margin) <= EPS:
        return "Push"

    if side == "over":
        return "Win" if margin > 0 else "Loss"

    if side == "under":
        return "Win" if margin < 0 else "Loss"

    raise ValueError(f"Unknown side: {side}")


def build_predictions(games):
    rows = []

    for _, r in games.iterrows():
        line = float(r["total"])

        actual = float(
            r["target_home_runs"]
            + r["target_away_runs"]
        )

        baseline_home = float(
            r["_baseline_home"]
        )
        baseline_away = float(
            r["_baseline_away"]
        )
        baseline_total = float(
            r["_baseline_total"]
        )
        bias_total = float(
            r["_bias_total"]
        )
        residual_total = float(
            r["_residual_total"]
        )

        variants = {
            "current_sum": (
                baseline_total,
                JUICE.totals_probabilities(
                    baseline_home,
                    baseline_away,
                    line,
                ),
            ),
            "bias_corrected": (
                bias_total,
                JUICE.totals_probabilities(
                    bias_total,
                    0.0,
                    line,
                ),
            ),
            "residual_model": (
                residual_total,
                JUICE.totals_probabilities(
                    residual_total,
                    0.0,
                    line,
                ),
            ),
        }

        for variant, (projected_total, probs) in variants.items():
            p_over, p_under, p_push = probs

            for side, p_win, p_loss in (
                ("over", p_over, p_under),
                ("under", p_under, p_over),
            ):
                result = grade_total(
                    side,
                    line,
                    actual,
                )

                resolved_mass = (
                    p_win + p_loss
                )

                if (
                    not np.isfinite(resolved_mass)
                    or resolved_mass <= 0
                ):
                    raise RuntimeError(
                        "Invalid totals probability mass"
                    )

                rows.append(
                    {
                        "variant": variant,
                        "game_date": pd.Timestamp(
                            r["_game_date_dt"]
                        ).strftime("%Y-%m-%d"),
                        "game_id": str(r["game_id"]),
                        "side": side,
                        "total_line": line,
                        "projected_total": projected_total,
                        "actual_total": actual,
                        "correction": (
                            projected_total
                            - baseline_total
                        ),
                        "model_prob": float(p_win),
                        "resolved_prob": float(
                            p_win / resolved_mass
                        ),
                        "p_loss": float(p_loss),
                        "p_push": float(p_push),
                        "result": result,
                        "win_binary": (
                            1.0
                            if result == "Win"
                            else (
                                0.0
                                if result == "Loss"
                                else np.nan
                            )
                        ),
                    }
                )

    return pd.DataFrame(rows)


def corr(x, y):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)

    mask = (
        np.isfinite(x)
        & np.isfinite(y)
    )

    if mask.sum() < 2:
        return np.nan

    x = x[mask]
    y = y[mask]

    if (
        np.std(x) <= 0
        or np.std(y) <= 0
    ):
        return np.nan

    return float(
        np.corrcoef(x, y)[0, 1]
    )


def build_buckets(resolved):
    rows = []

    for variant, part in resolved.groupby(
        "variant",
        sort=False,
    ):
        part = part.copy()

        part["bucket"] = pd.qcut(
            part["model_prob"],
            q=5,
            labels=False,
            duplicates="drop",
        )

        part = part.dropna(
            subset=["bucket"]
        )

        part["bucket"] = (
            part["bucket"].astype(int)
            + 1
        )

        for bucket, group in part.groupby(
            "bucket",
            sort=True,
        ):
            rows.append(
                {
                    "variant": variant,
                    "bucket": int(bucket),
                    "n": int(len(group)),
                    "prob_min": float(
                        group["model_prob"].min()
                    ),
                    "prob_mean": float(
                        group["model_prob"].mean()
                    ),
                    "prob_max": float(
                        group["model_prob"].max()
                    ),
                    "actual_win_rate": float(
                        group["win_binary"].mean()
                    ),
                }
            )

    return pd.DataFrame(rows)


def build_summary(predictions, buckets):
    resolved = predictions.dropna(
        subset=["win_binary"]
    ).copy()

    game_rows = predictions[
        predictions["side"] == "over"
    ].copy()

    rows = []

    for variant in (
        "current_sum",
        "bias_corrected",
        "residual_model",
    ):
        part = resolved[
            resolved["variant"]
            == variant
        ].copy()

        games = game_rows[
            game_rows["variant"]
            == variant
        ].copy()

        bucket_part = (
            buckets[
                buckets["variant"]
                == variant
            ]
            .sort_values("bucket")
        )

        if (
            part.empty
            or games.empty
            or bucket_part.empty
        ):
            raise RuntimeError(
                f"Missing evaluation data for {variant}"
            )

        actual_total = games[
            "actual_total"
        ].to_numpy(dtype=float)

        projected_total = games[
            "projected_total"
        ].to_numpy(dtype=float)

        y = part[
            "win_binary"
        ].to_numpy(dtype=float)

        p = np.clip(
            part["resolved_prob"].to_numpy(
                dtype=float
            ),
            EPS,
            1.0 - EPS,
        )

        win_rates = bucket_part[
            "actual_win_rate"
        ].to_numpy(dtype=float)

        rows.append(
            {
                "variant": variant,
                "games": int(len(games)),
                "resolved_candidates": int(
                    len(part)
                ),
                "projection_mae": float(
                    mean_absolute_error(
                        actual_total,
                        projected_total,
                    )
                ),
                "projection_poisson_deviance": float(
                    mean_poisson_deviance(
                        actual_total,
                        np.maximum(
                            projected_total,
                            EPS,
                        ),
                    )
                ),
                "projection_actual_corr": corr(
                    projected_total,
                    actual_total,
                ),
                "mean_projected_total": float(
                    projected_total.mean()
                ),
                "mean_actual_total": float(
                    actual_total.mean()
                ),
                "model_probability_win_corr": corr(
                    part["model_prob"],
                    part["win_binary"],
                ),
                "resolved_probability_win_corr": corr(
                    part["resolved_prob"],
                    part["win_binary"],
                ),
                "brier_score_resolved": float(
                    brier_score_loss(y, p)
                ),
                "log_loss_resolved": float(
                    log_loss(
                        y,
                        p,
                        labels=[0.0, 1.0],
                    )
                ),
                "low_probability_bucket_win_rate": float(
                    win_rates[0]
                ),
                "high_probability_bucket_win_rate": float(
                    win_rates[-1]
                ),
                "high_minus_low_win_rate": float(
                    win_rates[-1]
                    - win_rates[0]
                ),
                "rising_bucket_steps_out_of_4": int(
                    np.sum(
                        np.diff(win_rates)
                        > 0
                    )
                ),
                "mean_correction": float(
                    games["correction"].mean()
                ),
                "correction_std": float(
                    games["correction"].std(
                        ddof=0
                    )
                ),
            }
        )

    return pd.DataFrame(rows)


def write_report(summary, buckets, predictions):
    REPORT_FILE.write_text(
        f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Total Residual Model Test</title>
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
<h1>MLB Total Residual Model Test</h1>
<p>Generated: {datetime.now(UTC).isoformat()}</p>
<p>current_sum = home + away baseline.</p>
<p>bias_corrected = baseline + historical mean residual.</p>
<p>residual_model = baseline + predicted residual.</p>
<h2>Summary</h2>
{summary.to_html(index=False, border=0)}
<h2>Probability buckets</h2>
{buckets.to_html(index=False, border=0)}
<h2>Predictions</h2>
{predictions.to_html(index=False, border=0)}
</body>
</html>
""",
        encoding="utf-8",
    )


def main():
    training, baseline_features = (
        TM.load_training()
    )

    training[
        "target_total_runs"
    ] = (
        training["target_home_runs"]
        + training["target_away_runs"]
    )

    split = TRAIN.chronological_date_split(
        training
    )

    print(
        "Main split: "
        f"train={len(split['train']):,}, "
        f"validation={len(split['validation']):,}, "
        f"test={len(split['test']):,}"
    )

    venue_levels = venue_levels_from(
        pd.concat(
            [
                split["train"],
                split["validation"],
            ],
            ignore_index=True,
        )
    )

    (
        seed_home,
        seed_away,
        final_home,
        final_away,
    ) = tune_baseline_models(
        split,
        baseline_features,
    )

    (
        residual_model,
        residual_feature_columns,
        bias_correction,
    ) = prepare_residual_model(
        split["validation"],
        seed_home,
        seed_away,
        baseline_features,
        venue_levels,
    )

    test = add_baseline_predictions(
        split["test"],
        final_home,
        final_away,
        baseline_features,
    )

    test_features = residual_features(
        test,
        venue_levels,
    )

    missing = [
        col
        for col in residual_feature_columns
        if col not in test_features.columns
    ]

    if missing:
        raise RuntimeError(
            f"Missing residual test features: {missing}"
        )

    test["_predicted_residual"] = (
        residual_model.predict(
            test_features[
                residual_feature_columns
            ]
        )
    )

    if (
        ~np.isfinite(
            test["_predicted_residual"]
        )
    ).any():
        raise RuntimeError(
            "Residual model produced non-finite predictions"
        )

    test["_bias_total"] = np.maximum(
        test["_baseline_total"]
        + bias_correction,
        MIN_TOTAL,
    )

    test["_residual_total"] = np.maximum(
        test["_baseline_total"]
        + test["_predicted_residual"],
        MIN_TOTAL,
    )

    games = sportsbook_test_rows(
        test
    )

    predictions = build_predictions(
        games
    )

    resolved = predictions.dropna(
        subset=["win_binary"]
    ).copy()

    if resolved.empty:
        raise RuntimeError(
            "No resolved total candidates"
        )

    buckets = build_buckets(
        resolved
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

    current = summary[
        summary["variant"]
        == "current_sum"
    ].iloc[0]

    residual = summary[
        summary["variant"]
        == "residual_model"
    ].iloc[0]

    checks = {
        "lower_MAE": (
            residual["projection_mae"]
            < current["projection_mae"]
        ),
        "higher_total_correlation": (
            residual["projection_actual_corr"]
            > current["projection_actual_corr"]
        ),
        "higher_probability_win_correlation": (
            residual[
                "resolved_probability_win_corr"
            ]
            > current[
                "resolved_probability_win_corr"
            ]
        ),
        "lower_Brier": (
            residual["brier_score_resolved"]
            < current["brier_score_resolved"]
        ),
        "better_high_minus_low": (
            residual["high_minus_low_win_rate"]
            > current["high_minus_low_win_rate"]
        ),
    }

    wins = sum(
        bool(v)
        for v in checks.values()
    )

    print(
        "\nTOTAL RESIDUAL MODEL TEST COMPLETE"
    )

    print(
        summary.to_string(
            index=False
        )
    )

    print(
        "\nRESIDUAL VS CURRENT:"
    )

    for name, passed in checks.items():
        print(
            f"  {name}: "
            f"{'BETTER' if passed else 'WORSE'}"
        )

    print(
        f"\nResidual model improved "
        f"{wins}/5 primary metrics."
    )

    print(
        f"\nSUMMARY: {SUMMARY_FILE}"
    )
    print(
        f"BUCKETS: {BUCKET_FILE}"
    )
    print(
        f"PREDICTIONS: {PREDICTIONS_FILE}"
    )
    print(
        f"REPORT: {REPORT_FILE}"
    )


if __name__ == "__main__":
    main()