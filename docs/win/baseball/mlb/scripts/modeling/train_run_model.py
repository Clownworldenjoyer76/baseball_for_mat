#!/usr/bin/env python3
"""Train separate MLB home-run and away-run projection candidate models.

Input:
    docs/win/baseball/mlb/modeling/data/mlb_run_training_set.csv

Candidate outputs:
    docs/win/baseball/mlb/models/run_projection/candidates/home_runs_model.joblib
    docs/win/baseball/mlb/models/run_projection/candidates/away_runs_model.joblib
    docs/win/baseball/mlb/models/run_projection/candidates/home_runs_model_metadata.json
    docs/win/baseball/mlb/models/run_projection/candidates/away_runs_model_metadata.json

Production artifacts are never overwritten by this script. Promotion is handled only
after evaluate_run_model.py compares BOTH candidate models with the DRatings baseline.

The split is chronological by unique game dates:
    first 70% -> training
    next 15%  -> validation
    final 15% -> untouched test

Hyperparameters are selected using validation mean Poisson deviance only.
The exact ordered feature list is persisted in candidate metadata and must be enforced
by production prediction/evaluation code.
"""

from __future__ import annotations

import argparse
import itertools
import json
import traceback
from datetime import UTC, datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_poisson_deviance


BASE_DIR = Path("docs/win/baseball/mlb")
DEFAULT_INPUT = BASE_DIR / "modeling/data/mlb_run_training_set.csv"

MODEL_DIR = BASE_DIR / "models/run_projection"
CANDIDATE_DIR = MODEL_DIR / "candidates"
ERROR_DIR = BASE_DIR / "errors/modeling"
LOG_FILE = ERROR_DIR / "train_run_model.txt"

MODEL_DIR.mkdir(parents=True, exist_ok=True)
CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

HOME_MODEL_FILE = CANDIDATE_DIR / "home_runs_model.joblib"
AWAY_MODEL_FILE = CANDIDATE_DIR / "away_runs_model.joblib"
HOME_METADATA_FILE = CANDIDATE_DIR / "home_runs_model_metadata.json"
AWAY_METADATA_FILE = CANDIDATE_DIR / "away_runs_model_metadata.json"

AUDIT_COLUMNS = [
    "game_date",
    "game_id",
    "gamePk",
    "home_team",
    "away_team",
    "sdv_as_of_date",
]

TARGET_COLUMNS = [
    "target_home_runs",
    "target_away_runs",
]

CORE_FEATURE_COLUMNS = [
    "dratings_home_prob",
    "dratings_away_prob",
    "dratings_home_projected_runs",
    "dratings_away_projected_runs",
    "dratings_total_projected_runs",
    "home_sp_pitch_quality_plus",
    "away_sp_pitch_quality_plus",
    "home_sp_command_plus",
    "away_sp_command_plus",
    "home_sp_xera",
    "away_sp_xera",
    "home_sp_xera_30d",
    "away_sp_xera_30d",
    "home_sp_xwoba",
    "away_sp_xwoba",
    "home_sp_xwoba_30d",
    "away_sp_xwoba_30d",
    "home_sp_avg_velo",
    "away_sp_avg_velo",
    "home_sp_avg_velo_30d",
    "away_sp_avg_velo_30d",
    "home_sp_velo_delta_30d",
    "away_sp_velo_delta_30d",
    "home_sp_pitches",
    "away_sp_pitches",
    "home_sp_games",
    "away_sp_games",
    "home_sp_pitches_30d",
    "away_sp_pitches_30d",
    "home_sp_games_30d",
    "away_sp_games_30d",
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

OPTIONAL_NUMERIC_FEATURE_COLUMNS = [
    "temp_f",
    "wind_mph",
    "wind_blowing_out",
    "humidity",
    "air_pressure_at_sea_level",
    "dew_point_f",
    "weather_applicable",
]

HYPERPARAMETER_GRID = {
    "learning_rate": [0.03, 0.05, 0.10],
    "max_leaf_nodes": [7, 15, 31],
    "min_samples_leaf": [10, 20, 40],
    "l2_regularization": [0, 1, 5],
}

RANDOM_STATE = 42
POISSON_EPSILON = 1e-12


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _log(message: str, level: str = "INFO") -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level:<5} | {message.rstrip()}\n")


def fail(message: str) -> None:
    _log(message, "ERROR")
    raise RuntimeError(message)


def duplicate_columns(columns) -> list[str]:
    seen: set[str] = set()
    dupes: list[str] = []

    for col in columns:
        if col in seen and col not in dupes:
            dupes.append(col)
        seen.add(col)

    return dupes


def load_training_set(path: Path) -> pd.DataFrame:
    if not path.exists():
        fail(f"Training set not found: {path}")

    df = pd.read_csv(path, encoding="utf-8-sig")

    if df.empty:
        fail(f"Training set is empty: {path}")

    dupes = duplicate_columns(list(df.columns))
    if dupes:
        fail(f"Training set contains duplicate columns: {dupes}")

    required = AUDIT_COLUMNS + TARGET_COLUMNS + CORE_FEATURE_COLUMNS
    missing = [col for col in required if col not in df.columns]

    if missing:
        fail(f"Training set missing required columns: {missing}")

    return df


def determine_feature_columns(df: pd.DataFrame) -> list[str]:
    """Build the exact ordered production feature contract."""
    features = list(CORE_FEATURE_COLUMNS)

    for col in OPTIONAL_NUMERIC_FEATURE_COLUMNS:
        if col in df.columns:
            features.append(col)

    if len(features) != len(set(features)):
        fail("Duplicate feature names detected")

    return features


def coerce_and_validate_training_data(
    df: pd.DataFrame,
    feature_columns: list[str],
) -> pd.DataFrame:
    df = df.copy()

    df["_game_date_dt"] = pd.to_datetime(
        df["game_date"].astype("string").str.replace("_", "-", regex=False),
        errors="coerce",
    ).dt.normalize()

    bad_dates = df["_game_date_dt"].isna()
    if bad_dates.any():
        sample = df.loc[
            bad_dates,
            ["game_id", "game_date"],
        ].head(10).to_dict("records")
        fail(
            f"Invalid game_date values; bad_rows={int(bad_dates.sum())}; "
            f"sample={sample}"
        )

    for col in feature_columns + TARGET_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for target in TARGET_COLUMNS:
        values = df[target]
        bad = values.isna() | ~np.isfinite(values) | (values < 0)

        if bad.any():
            sample = df.loc[
                bad,
                ["game_id", "game_date", target],
            ].head(10).to_dict("records")
            fail(
                f"Invalid target values in {target}; "
                f"bad_rows={int(bad.sum())}; sample={sample}"
            )

    for col in feature_columns:
        values = df[col]
        bad = values.notna() & ~np.isfinite(values)

        if bad.any():
            sample = df.loc[
                bad,
                ["game_id", "game_date", col],
            ].head(10).to_dict("records")
            fail(
                f"Non-finite feature values in {col}; "
                f"bad_rows={int(bad.sum())}; sample={sample}"
            )

    return df


def chronological_date_split(
    df: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    unique_dates = np.array(
        sorted(df["_game_date_dt"].drop_duplicates().tolist())
    )

    n_dates = len(unique_dates)

    if n_dates < 3:
        fail(
            "At least 3 unique game dates are required for "
            f"train/validation/test splits; found {n_dates}"
        )

    train_end = int(np.floor(n_dates * 0.70))
    validation_end = int(np.floor(n_dates * 0.85))

    train_end = max(1, min(train_end, n_dates - 2))
    validation_end = max(
        train_end + 1,
        min(validation_end, n_dates - 1),
    )

    train_dates = set(unique_dates[:train_end])
    validation_dates = set(unique_dates[train_end:validation_end])
    test_dates = set(unique_dates[validation_end:])

    if (
        train_dates & validation_dates
        or train_dates & test_dates
        or validation_dates & test_dates
    ):
        fail("The same game date appears in more than one split")

    train = df[df["_game_date_dt"].isin(train_dates)].copy()
    validation = df[
        df["_game_date_dt"].isin(validation_dates)
    ].copy()
    test = df[df["_game_date_dt"].isin(test_dates)].copy()

    if train.empty or validation.empty or test.empty:
        fail(
            "Chronological split produced an empty partition: "
            f"train={len(train)} validation={len(validation)} "
            f"test={len(test)}"
        )

    return {
        "train": train,
        "validation": validation,
        "test": test,
    }


def hyperparameter_candidates():
    keys = [
        "learning_rate",
        "max_leaf_nodes",
        "min_samples_leaf",
        "l2_regularization",
    ]

    for values in itertools.product(
        *(HYPERPARAMETER_GRID[key] for key in keys)
    ):
        yield dict(zip(keys, values))


def safe_mean_poisson_deviance(
    y_true,
    y_pred,
) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    if np.any(~np.isfinite(y_true)) or np.any(y_true < 0):
        fail("Invalid actual-run values supplied to Poisson metric")

    if np.any(~np.isfinite(y_pred)):
        fail("Non-finite predictions supplied to Poisson metric")

    safe_pred = np.maximum(y_pred, POISSON_EPSILON)

    return float(
        mean_poisson_deviance(
            y_true,
            safe_pred,
        )
    )


def evaluate_predictions(
    y_true,
    y_pred,
) -> dict:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    if np.any(~np.isfinite(y_pred)):
        fail("Evaluation received non-finite predictions")

    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "mean_poisson_deviance": safe_mean_poisson_deviance(
            y_true,
            y_pred,
        ),
        "mean_predicted_runs": float(np.mean(y_pred)),
        "mean_actual_runs": float(np.mean(y_true)),
    }


def select_hyperparameters(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
    label: str,
) -> tuple[dict, float]:
    X_train = train[feature_columns]
    y_train = train[target_column]

    X_validation = validation[feature_columns]
    y_validation = validation[target_column]

    best_params = None
    best_score = np.inf
    candidate_count = 0

    for params in hyperparameter_candidates():
        candidate_count += 1

        model = HistGradientBoostingRegressor(
            loss="poisson",
            learning_rate=params["learning_rate"],
            max_leaf_nodes=params["max_leaf_nodes"],
            min_samples_leaf=params["min_samples_leaf"],
            l2_regularization=params["l2_regularization"],
            random_state=RANDOM_STATE,
        )

        model.fit(
            X_train,
            y_train,
        )

        validation_predictions = model.predict(
            X_validation
        )

        validation_score = safe_mean_poisson_deviance(
            y_validation,
            validation_predictions,
        )

        if validation_score < best_score:
            best_score = validation_score
            best_params = dict(params)

    if candidate_count != 81:
        fail(
            f"{label} hyperparameter grid expected 81 candidates; "
            f"evaluated {candidate_count}"
        )

    if best_params is None or not np.isfinite(best_score):
        fail(f"{label} failed to select valid hyperparameters")

    _log(
        f"{label} selected_hyperparameters={best_params} "
        f"validation_mean_poisson_deviance={best_score:.12f}"
    )

    return best_params, float(best_score)


def fit_final_model(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
    selected_hyperparameters: dict,
) -> HistGradientBoostingRegressor:
    fit_frame = pd.concat(
        [train, validation],
        ignore_index=True,
        sort=False,
    )

    model = HistGradientBoostingRegressor(
        loss="poisson",
        learning_rate=selected_hyperparameters["learning_rate"],
        max_leaf_nodes=selected_hyperparameters["max_leaf_nodes"],
        min_samples_leaf=selected_hyperparameters["min_samples_leaf"],
        l2_regularization=selected_hyperparameters["l2_regularization"],
        random_state=RANDOM_STATE,
    )

    model.fit(
        fit_frame[feature_columns],
        fit_frame[target_column],
    )

    return model


def split_date_range(
    frame: pd.DataFrame,
) -> tuple[str, str]:
    start = frame["_game_date_dt"].min()
    end = frame["_game_date_dt"].max()

    return (
        pd.Timestamp(start).strftime("%Y-%m-%d"),
        pd.Timestamp(end).strftime("%Y-%m-%d"),
    )


def train_one_side(
    *,
    side: str,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[HistGradientBoostingRegressor, dict]:
    if side == "home":
        target_column = "target_home_runs"
        baseline_column = "dratings_home_projected_runs"
    elif side == "away":
        target_column = "target_away_runs"
        baseline_column = "dratings_away_projected_runs"
    else:
        fail(f"Unknown model side: {side}")

    selected_hyperparameters, validation_score = (
        select_hyperparameters(
            train,
            validation,
            feature_columns,
            target_column,
            f"{side}_runs",
        )
    )

    model = fit_final_model(
        train,
        validation,
        feature_columns,
        target_column,
        selected_hyperparameters,
    )

    test_features = test[feature_columns]
    test_actual = test[target_column]

    model_predictions = model.predict(
        test_features
    )

    baseline_predictions = pd.to_numeric(
        test[baseline_column],
        errors="coerce",
    )

    baseline_bad = (
        baseline_predictions.isna()
        | ~np.isfinite(baseline_predictions)
        | (baseline_predictions < 0)
    )

    if baseline_bad.any():
        sample = test.loc[
            baseline_bad,
            ["game_id", "game_date", baseline_column],
        ].head(10).to_dict("records")
        fail(
            f"{side} DRatings baseline contains invalid values; "
            f"bad_rows={int(baseline_bad.sum())}; sample={sample}"
        )

    model_metrics = evaluate_predictions(
        test_actual,
        model_predictions,
    )

    baseline_metrics = evaluate_predictions(
        test_actual,
        baseline_predictions,
    )

    training_start_date, training_end_date = split_date_range(
        train
    )
    validation_start_date, validation_end_date = split_date_range(
        validation
    )
    test_start_date, test_end_date = split_date_range(
        test
    )

    metadata = {
        "artifact_stage": "candidate",
        "training_start_date": training_start_date,
        "training_end_date": training_end_date,
        "validation_start_date": validation_start_date,
        "validation_end_date": validation_end_date,
        "test_start_date": test_start_date,
        "test_end_date": test_end_date,
        "feature_columns": list(feature_columns),
        "selected_hyperparameters": selected_hyperparameters,
        "training_row_count": int(len(train)),
        "validation_row_count": int(len(validation)),
        "test_row_count": int(len(test)),
        "baseline_metrics": baseline_metrics,
        "model_metrics": model_metrics,
        "created_at": _now(),
        "validation_mean_poisson_deviance": validation_score,
        "target_column": target_column,
        "baseline_column": baseline_column,
        "model_class": "HistGradientBoostingRegressor",
        "loss": "poisson",
        "random_state": RANDOM_STATE,
        "final_fit_row_count": int(len(train) + len(validation)),
        "final_fit_rule": (
            "refit_on_training_plus_validation_after_validation_selection"
        ),
    }

    return model, metadata


def write_metadata(
    path: Path,
    metadata: dict,
) -> None:
    path.write_text(
        json.dumps(
            metadata,
            indent=2,
            sort_keys=False,
        )
        + "\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=(
            "Training-set CSV path "
            f"(default: {DEFAULT_INPUT})"
        ),
    )

    parser.add_argument(
        "--candidate-dir",
        type=Path,
        default=CANDIDATE_DIR,
        help=(
            "Directory for candidate run-model artifacts "
            f"(default: {CANDIDATE_DIR})"
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    with LOG_FILE.open("w", encoding="utf-8") as f:
        f.write(f"=== train_run_model RUN {_now()} ===\n")

    try:
        args.candidate_dir.mkdir(parents=True, exist_ok=True)

        home_model_file = args.candidate_dir / "home_runs_model.joblib"
        away_model_file = args.candidate_dir / "away_runs_model.joblib"
        home_metadata_file = args.candidate_dir / "home_runs_model_metadata.json"
        away_metadata_file = args.candidate_dir / "away_runs_model_metadata.json"

        training_df = load_training_set(
            args.input
        )

        feature_columns = determine_feature_columns(
            training_df
        )

        training_df = coerce_and_validate_training_data(
            training_df,
            feature_columns,
        )

        splits = chronological_date_split(
            training_df
        )

        train = splits["train"]
        validation = splits["validation"]
        test = splits["test"]

        _log(
            "chronological_split "
            f"train_rows={len(train)} "
            f"validation_rows={len(validation)} "
            f"test_rows={len(test)} "
            f"train_dates={train['_game_date_dt'].nunique()} "
            f"validation_dates={validation['_game_date_dt'].nunique()} "
            f"test_dates={test['_game_date_dt'].nunique()}"
        )

        _log(
            f"feature_count={len(feature_columns)} "
            f"feature_columns={feature_columns}"
        )

        home_model, home_metadata = train_one_side(
            side="home",
            train=train,
            validation=validation,
            test=test,
            feature_columns=feature_columns,
        )

        away_model, away_metadata = train_one_side(
            side="away",
            train=train,
            validation=validation,
            test=test,
            feature_columns=feature_columns,
        )

        joblib.dump(
            home_model,
            home_model_file,
        )
        joblib.dump(
            away_model,
            away_model_file,
        )

        write_metadata(
            home_metadata_file,
            home_metadata,
        )
        write_metadata(
            away_metadata_file,
            away_metadata,
        )

        _log(f"WROTE CANDIDATE {home_model_file}")
        _log(f"WROTE CANDIDATE {away_model_file}")
        _log(f"WROTE CANDIDATE {home_metadata_file}")
        _log(f"WROTE CANDIDATE {away_metadata_file}")
        _log("PRODUCTION ARTIFACTS NOT MODIFIED BY TRAINING")

        _log(
            "home_candidate_test_report "
            f"baseline_metrics={home_metadata['baseline_metrics']} "
            f"model_metrics={home_metadata['model_metrics']}"
        )

        _log(
            "away_candidate_test_report "
            f"baseline_metrics={away_metadata['baseline_metrics']} "
            f"model_metrics={away_metadata['model_metrics']}"
        )

        print("train_run_model complete. candidate artifacts written only.")

        print(
            "home candidate test | "
            f"model_mae={home_metadata['model_metrics']['mae']:.6f} "
            f"baseline_mae={home_metadata['baseline_metrics']['mae']:.6f} "
            f"model_poisson="
            f"{home_metadata['model_metrics']['mean_poisson_deviance']:.6f} "
            f"baseline_poisson="
            f"{home_metadata['baseline_metrics']['mean_poisson_deviance']:.6f}"
        )

        print(
            "away candidate test | "
            f"model_mae={away_metadata['model_metrics']['mae']:.6f} "
            f"baseline_mae={away_metadata['baseline_metrics']['mae']:.6f} "
            f"model_poisson="
            f"{away_metadata['model_metrics']['mean_poisson_deviance']:.6f} "
            f"baseline_poisson="
            f"{away_metadata['baseline_metrics']['mean_poisson_deviance']:.6f}"
        )

    except Exception as exc:
        _log(
            f"FATAL: {exc}\n{traceback.format_exc()}",
            "ERROR",
        )
        print(f"train_run_model failed: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
