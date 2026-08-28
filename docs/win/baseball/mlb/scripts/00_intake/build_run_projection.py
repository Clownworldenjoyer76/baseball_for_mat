#!/usr/bin/env python3
"""Build production MLB run projections from trained home/away run models.

Inputs per date:
    docs/win/baseball/mlb/00_intake/predictions/pred_with_game_id/{date}_MLB.csv
    docs/win/baseball/mlb/00_intake/games/{date}_games.csv
    docs/win/baseball/mlb/00_intake/sportsdataverse/{date}_sportsdataverse.csv
    docs/win/baseball/mlb/00_intake/mlb_raw/{date}_game_context.csv

Models loaded once per run:
    docs/win/baseball/mlb/models/run_projection/home_runs_model.joblib
    docs/win/baseball/mlb/models/run_projection/away_runs_model.joblib
    docs/win/baseball/mlb/models/run_projection/home_runs_model_metadata.json
    docs/win/baseball/mlb/models/run_projection/away_runs_model_metadata.json

Output:
    docs/win/baseball/mlb/00_intake/predictions/model_projection/{date}_MLB.csv

When no dates are supplied, every *_MLB.csv file in pred_with_game_id is rebuilt.

The exact ordered feature list comes from model metadata. This script does not
silently infer, add, drop, reorder, fill, or clip model features/predictions.
"""

from __future__ import annotations

import argparse
import json
import traceback
from datetime import UTC, datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


BASE_DIR = Path("docs/win/baseball/mlb")

PRED_DIR = BASE_DIR / "00_intake/predictions/pred_with_game_id"
GAMES_DIR = BASE_DIR / "00_intake/games"
SDV_DIR = BASE_DIR / "00_intake/sportsdataverse"
CONTEXT_DIR = BASE_DIR / "00_intake/mlb_raw"

MODEL_DIR = BASE_DIR / "models/run_projection"
HOME_MODEL_FILE = MODEL_DIR / "home_runs_model.joblib"
AWAY_MODEL_FILE = MODEL_DIR / "away_runs_model.joblib"
HOME_METADATA_FILE = MODEL_DIR / "home_runs_model_metadata.json"
AWAY_METADATA_FILE = MODEL_DIR / "away_runs_model_metadata.json"

OUTPUT_DIR = BASE_DIR / "00_intake/predictions/model_projection"
ERROR_DIR = BASE_DIR / "errors/00_intake"
LOG_FILE = ERROR_DIR / "build_run_projection.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)


PRED_REQUIRED = [
    "game_id",
    "game_date",
    "home_team",
    "away_team",
    "home_prob",
    "away_prob",
    "home_projected_runs",
    "away_projected_runs",
    "total_projected_runs",
]

GAMES_REQUIRED = [
    "game_id",
    "gamePk",
    "game_date",
    "home_team",
    "away_team",
]

SDV_REQUIRED = [
    "gamePk",
    "game_id",
    "sdv_as_of_date",
    "sdv_status",
    "sdv_home_sp_found",
    "sdv_away_sp_found",
    "sdv_home_bp_pa_14d",
    "sdv_away_bp_pa_14d",
    "sdv_home_bp_woba_allowed_14d",
    "sdv_away_bp_woba_allowed_14d",
    "sdv_home_bp_k_rate_14d",
    "sdv_away_bp_k_rate_14d",
    "sdv_home_bp_bb_rate_14d",
    "sdv_away_bp_bb_rate_14d",
    "sdv_home_bp_hard_rate_14d",
    "sdv_away_bp_hard_rate_14d",
    "sdv_home_bp_pitches_3d",
    "sdv_away_bp_pitches_3d",
    "sdv_home_bp_pa_7d",
    "sdv_away_bp_pa_7d",
    "sdv_home_bp_woba_allowed_7d",
    "sdv_away_bp_woba_allowed_7d",
    "sdv_home_bp_k_rate_7d",
    "sdv_away_bp_k_rate_7d",
    "sdv_home_bp_bb_rate_7d",
    "sdv_away_bp_bb_rate_7d",
    "sdv_home_bp_hard_rate_7d",
    "sdv_away_bp_hard_rate_7d",
]


SDV_FEATURE_MAP = {
    "sdv_home_sp_stuff_plus": "home_sp_pitch_quality_plus",
    "sdv_away_sp_stuff_plus": "away_sp_pitch_quality_plus",
    "sdv_home_sp_command_plus": "home_sp_command_plus",
    "sdv_away_sp_command_plus": "away_sp_command_plus",
    "sdv_home_sp_xera": "home_sp_xera",
    "sdv_away_sp_xera": "away_sp_xera",
    "sdv_home_sp_xera_30d": "home_sp_xera_30d",
    "sdv_away_sp_xera_30d": "away_sp_xera_30d",
    "sdv_home_sp_xwoba": "home_sp_xwoba",
    "sdv_away_sp_xwoba": "away_sp_xwoba",
    "sdv_home_sp_xwoba_30d": "home_sp_xwoba_30d",
    "sdv_away_sp_xwoba_30d": "away_sp_xwoba_30d",
    "sdv_home_sp_avg_velo": "home_sp_avg_velo",
    "sdv_away_sp_avg_velo": "away_sp_avg_velo",
    "sdv_home_sp_avg_velo_30d": "home_sp_avg_velo_30d",
    "sdv_away_sp_avg_velo_30d": "away_sp_avg_velo_30d",
    "sdv_home_sp_velo_delta_30d": "home_sp_velo_delta_30d",
    "sdv_away_sp_velo_delta_30d": "away_sp_velo_delta_30d",
    "sdv_home_sp_pitches": "home_sp_pitches",
    "sdv_away_sp_pitches": "away_sp_pitches",
    "sdv_home_sp_games": "home_sp_games",
    "sdv_away_sp_games": "away_sp_games",
    "sdv_home_sp_pitches_30d": "home_sp_pitches_30d",
    "sdv_away_sp_pitches_30d": "away_sp_pitches_30d",
    "sdv_home_sp_games_30d": "home_sp_games_30d",
    "sdv_away_sp_games_30d": "away_sp_games_30d",
    "sdv_home_bp_pa_14d": "home_bp_pa_14d",
    "sdv_away_bp_pa_14d": "away_bp_pa_14d",
    "sdv_home_bp_woba_allowed_14d": "home_bp_woba_allowed_14d",
    "sdv_away_bp_woba_allowed_14d": "away_bp_woba_allowed_14d",
    "sdv_home_bp_k_rate_14d": "home_bp_k_rate_14d",
    "sdv_away_bp_k_rate_14d": "away_bp_k_rate_14d",
    "sdv_home_bp_bb_rate_14d": "home_bp_bb_rate_14d",
    "sdv_away_bp_bb_rate_14d": "away_bp_bb_rate_14d",
    "sdv_home_bp_hard_rate_14d": "home_bp_hard_rate_14d",
    "sdv_away_bp_hard_rate_14d": "away_bp_hard_rate_14d",
    "sdv_home_bp_pitches_3d": "home_bp_pitches_3d",
    "sdv_away_bp_pitches_3d": "away_bp_pitches_3d",
    "sdv_home_bp_pa_7d": "home_bp_pa_7d",
    "sdv_away_bp_pa_7d": "away_bp_pa_7d",
    "sdv_home_bp_woba_allowed_7d": "home_bp_woba_allowed_7d",
    "sdv_away_bp_woba_allowed_7d": "away_bp_woba_allowed_7d",
    "sdv_home_bp_k_rate_7d": "home_bp_k_rate_7d",
    "sdv_away_bp_k_rate_7d": "away_bp_k_rate_7d",
    "sdv_home_bp_bb_rate_7d": "home_bp_bb_rate_7d",
    "sdv_away_bp_bb_rate_7d": "away_bp_bb_rate_7d",
    "sdv_home_bp_hard_rate_7d": "home_bp_hard_rate_7d",
    "sdv_away_bp_hard_rate_7d": "away_bp_hard_rate_7d",
}


DRATINGS_RENAME = {
    "home_prob": "dratings_home_prob",
    "away_prob": "dratings_away_prob",
    "home_projected_runs": "dratings_home_projected_runs",
    "away_projected_runs": "dratings_away_projected_runs",
    "total_projected_runs": "dratings_total_projected_runs",
}


SAFE_CONTEXT_FEATURES = {
    "temp_f",
    "wind_mph",
    "wind_blowing_out",
    "humidity",
    "air_pressure_at_sea_level",
    "dew_point_f",
    "weather_applicable",
    "venue_id",
    "day_night",
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _log(message: str, level: str = "INFO") -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level:<5} | {message.rstrip()}\n")


def fail(message: str) -> None:
    _log(message, "ERROR")
    raise RuntimeError(message)


def duplicate_columns(columns) -> list[str]:
    seen = set()
    dupes = []

    for col in columns:
        if col in seen and col not in dupes:
            dupes.append(col)
        seen.add(col)

    return dupes


def read_csv_checked(
    path: Path,
    required: list[str],
    label: str,
) -> pd.DataFrame:
    if not path.exists():
        fail(f"{label} missing required file: {path}")

    df = pd.read_csv(
        path,
        dtype=str,
        encoding="utf-8-sig",
    )

    dupes = duplicate_columns(list(df.columns))

    if dupes:
        fail(f"{label} duplicate columns: {dupes}")

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        fail(f"{label} missing required columns: {missing}")

    return df


def normalize_game_id(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def normalize_gamepk(series: pd.Series) -> pd.Series:
    raw = series.astype("string").str.strip()
    numeric = pd.to_numeric(raw, errors="coerce")
    out = raw.copy()

    valid = numeric.notna()

    out.loc[valid] = (
        numeric.loc[valid]
        .round()
        .astype("Int64")
        .astype("string")
    )

    return out


def assert_unique_nonblank_key(
    df: pd.DataFrame,
    key: str,
    label: str,
) -> None:
    if key not in df.columns:
        return

    values = df[key].astype("string").str.strip()

    nonblank = (
        values.notna()
        & (values != "")
    )

    duplicated = (
        nonblank
        & values.duplicated(keep=False)
    )

    if duplicated.any():
        sample = (
            values.loc[duplicated]
            .head(10)
            .tolist()
        )

        fail(
            f"{label} duplicate {key}; "
            f"duplicate_rows={int(duplicated.sum())}; "
            f"sample={sample}"
        )


def prepare_keys(
    df: pd.DataFrame,
    label: str,
) -> pd.DataFrame:
    df = df.copy()

    if "game_id" in df.columns:
        df["game_id"] = normalize_game_id(
            df["game_id"]
        )

        assert_unique_nonblank_key(
            df,
            "game_id",
            label,
        )

    if "gamePk" in df.columns:
        df["gamePk"] = normalize_gamepk(
            df["gamePk"]
        )

        assert_unique_nonblank_key(
            df,
            "gamePk",
            label,
        )

    return df


def load_metadata(
    path: Path,
    label: str,
) -> dict:
    if not path.exists():
        fail(f"{label} metadata missing: {path}")

    with path.open(
        "r",
        encoding="utf-8",
    ) as f:
        metadata = json.load(f)

    feature_columns = metadata.get(
        "feature_columns"
    )

    if (
        not isinstance(feature_columns, list)
        or not feature_columns
        or any(
            not isinstance(col, str)
            or not col
            for col in feature_columns
        )
    ):
        fail(
            f"{label} metadata has invalid "
            "feature_columns"
        )

    if len(feature_columns) != len(
        set(feature_columns)
    ):
        fail(
            f"{label} metadata feature_columns "
            "contain duplicates"
        )

    return metadata


def load_model(
    path: Path,
    label: str,
):
    if not path.exists():
        fail(f"{label} model missing: {path}")

    return joblib.load(path)


def model_version(
    metadata: dict,
    label: str,
) -> str:
    created_at = str(
        metadata.get("created_at") or ""
    ).strip()

    if created_at:
        return f"{label}:{created_at}"

    return f"{label}:unknown"


def assert_metadata_feature_contract(
    home_metadata: dict,
    away_metadata: dict,
) -> list[str]:
    home_features = list(
        home_metadata["feature_columns"]
    )

    away_features = list(
        away_metadata["feature_columns"]
    )

    if home_features != away_features:
        fail(
            "Home and away model metadata "
            "feature order differs; "
            f"home={home_features} "
            f"away={away_features}"
        )

    return home_features


def assert_model_feature_order(
    model,
    metadata_features: list[str],
    label: str,
) -> None:
    model_names = getattr(
        model,
        "feature_names_in_",
        None,
    )

    if model_names is None:
        fail(
            f"{label} model does not expose "
            "feature_names_in_; cannot verify "
            "exact feature order against metadata"
        )

    model_features = [
        str(x)
        for x in model_names.tolist()
    ]

    if model_features != metadata_features:
        fail(
            f"{label} model feature order "
            "differs from metadata; "
            f"model={model_features} "
            f"metadata={metadata_features}"
        )


def assert_secondary_game_id_match(
    df: pd.DataFrame,
    left_col: str,
    right_col: str,
    label: str,
) -> None:
    left = (
        df[left_col]
        .astype("string")
        .str.strip()
    )

    right = (
        df[right_col]
        .astype("string")
        .str.strip()
    )

    comparable = (
        left.notna()
        & right.notna()
        & (left != "")
        & (right != "")
    )

    mismatch = (
        comparable
        & (left != right)
    )

    if mismatch.any():
        sample = (
            df.loc[
                mismatch,
                [
                    left_col,
                    right_col,
                    "gamePk",
                ],
            ]
            .head(10)
            .to_dict("records")
        )

        fail(
            f"{label} game_id mismatch; "
            f"bad_rows={int(mismatch.sum())}; "
            f"sample={sample}"
        )


def build_feature_frame(
    pred: pd.DataFrame,
    games: pd.DataFrame,
    sdv: pd.DataFrame,
    context: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pred = pred.copy()

    pred["game_id"] = normalize_game_id(
        pred["game_id"]
    )

    blank_game_id = (
        pred["game_id"].isna()
        | (pred["game_id"] == "")
    )

    if blank_game_id.any():
        sample = (
            pred.loc[
                blank_game_id,
                [
                    "game_id",
                    "game_date",
                    "home_team",
                    "away_team",
                ],
            ]
            .head(10)
            .to_dict("records")
        )

        fail(
            "Blank prediction game_id; "
            f"bad_rows={int(blank_game_id.sum())}; "
            f"sample={sample}"
        )

    pred = prepare_keys(
        pred,
        "predictions",
    )

    games = prepare_keys(
        games,
        "games",
    )

    sdv = prepare_keys(
        sdv,
        "sportsdataverse",
    )

    context = prepare_keys(
        context,
        "game_context",
    )

    base = pred.rename(
        columns=DRATINGS_RENAME
    ).copy()

    for (
        source_col,
        dratings_col,
    ) in DRATINGS_RENAME.items():
        base[source_col] = pred[source_col]

    games_keep = [
        col
        for col in [
            "game_id",
            "gamePk",
            "game_date",
            "home_team",
            "away_team",
            "venue_id",
            "day_night",
        ]
        if col in games.columns
    ]

    joined = base.merge(
        games[games_keep],
        on="game_id",
        how="left",
        suffixes=("", "_games"),
        validate="one_to_one",
    )

    if joined["gamePk"].isna().any():
        sample = (
            joined.loc[
                joined["gamePk"].isna(),
                [
                    "game_id",
                    "game_date",
                    "home_team",
                    "away_team",
                ],
            ]
            .head(10)
            .to_dict("records")
        )

        fail(
            "Prediction rows missing "
            "games.gamePk; "
            f"bad_rows={int(joined['gamePk'].isna().sum())}; "
            f"sample={sample}"
        )

    sdv_keep = [
        col
        for col in (
            [
                "gamePk",
                "game_id",
                "sdv_as_of_date",
                "sdv_status",
                "sdv_home_sp_found",
                "sdv_away_sp_found",
            ]
            + list(
                SDV_FEATURE_MAP.keys()
            )
        )
        if col in sdv.columns
    ]

    joined = joined.merge(
        sdv[sdv_keep].rename(
            columns={
                "game_id": "game_id_sdv"
            }
        ),
        on="gamePk",
        how="left",
        validate="one_to_one",
    )

    assert_secondary_game_id_match(
        joined,
        "game_id",
        "game_id_sdv",
        "games->sportsdataverse",
    )

    joined = joined.rename(
        columns=SDV_FEATURE_MAP
    )

    context_features_needed = [
        col
        for col in feature_columns
        if (
            col in SAFE_CONTEXT_FEATURES
            and col in context.columns
        )
    ]

    if context_features_needed:
        context_keep = (
            ["gamePk"]
            + context_features_needed
        )

        joined = joined.merge(
            context[context_keep],
            on="gamePk",
            how="left",
            validate="one_to_one",
            suffixes=("", "_context"),
        )

    missing_feature_columns = [
        col
        for col in feature_columns
        if col not in joined.columns
    ]

    if missing_feature_columns:
        fail(
            "Required model feature columns "
            "unavailable for production "
            "projection: "
            f"{missing_feature_columns}"
        )

    X = joined.loc[
        :,
        feature_columns,
    ].copy()

    if list(X.columns) != feature_columns:
        fail(
            "Constructed model feature order "
            "differs from metadata; "
            f"constructed={list(X.columns)} "
            f"metadata={feature_columns}"
        )

    for col in X.columns:
        X[col] = pd.to_numeric(
            X[col],
            errors="coerce",
        )

        bad = (
            X[col].notna()
            & ~np.isfinite(X[col])
        )

        if bad.any():
            sample = (
                joined.loc[
                    bad,
                    [
                        "game_id",
                        "gamePk",
                        col,
                    ],
                ]
                .head(10)
                .to_dict("records")
            )

            fail(
                "Non-finite non-missing "
                f"feature values in {col}; "
                f"bad_rows={int(bad.sum())}; "
                f"sample={sample}"
            )

    return joined, X


def build_feature_status(
    joined: pd.DataFrame,
    X: pd.DataFrame,
) -> pd.Series:
    statuses = []

    home_found = pd.to_numeric(
        joined.get(
            "sdv_home_sp_found"
        ),
        errors="coerce",
    )

    away_found = pd.to_numeric(
        joined.get(
            "sdv_away_sp_found"
        ),
        errors="coerce",
    )

    for idx in joined.index:
        missing_features = [
            col
            for col in X.columns
            if pd.isna(
                X.loc[idx, col]
            )
        ]

        home_ok = (
            idx in home_found.index
            and pd.notna(
                home_found.loc[idx]
            )
            and float(
                home_found.loc[idx]
            ) == 1.0
        )

        away_ok = (
            idx in away_found.index
            and pd.notna(
                away_found.loc[idx]
            )
            and float(
                away_found.loc[idx]
            ) == 1.0
        )

        if home_ok and away_ok:
            starter_status = (
                "both_starters_sdv_found"
            )
        elif home_ok and not away_ok:
            starter_status = (
                "away_starter_sdv_missing"
            )
        elif away_ok and not home_ok:
            starter_status = (
                "home_starter_sdv_missing"
            )
        else:
            starter_status = (
                "both_starters_sdv_missing"
            )

        if missing_features:
            status = (
                f"{starter_status};"
                "missing_features="
                + ",".join(
                    missing_features
                )
            )
        else:
            status = (
                f"{starter_status};"
                "missing_features=none"
            )

        statuses.append(status)

    return pd.Series(
        statuses,
        index=joined.index,
        dtype="string",
    )


def validate_predictions(
    values,
    label: str,
) -> np.ndarray:
    values = np.asarray(
        values,
        dtype=float,
    )

    if np.any(
        ~np.isfinite(values)
    ):
        bad_idx = (
            np.where(
                ~np.isfinite(values)
            )[0][:10]
            .tolist()
        )

        fail(
            f"{label} produced non-finite "
            "run predictions; "
            f"sample_indices={bad_idx}"
        )

    if np.any(values < 0):
        bad_idx = (
            np.where(
                values < 0
            )[0][:10]
            .tolist()
        )

        bad_values = (
            values[
                values < 0
            ][:10]
            .tolist()
        )

        fail(
            f"{label} produced negative "
            "run predictions; "
            f"sample_indices={bad_idx}; "
            f"sample_values={bad_values}"
        )

    return values


def process_date(
    date_str: str,
    home_model,
    away_model,
    home_metadata: dict,
    away_metadata: dict,
    feature_columns: list[str],
) -> Path:
    pred_path = (
        PRED_DIR
        / f"{date_str}_MLB.csv"
    )

    games_path = (
        GAMES_DIR
        / f"{date_str}_games.csv"
    )

    sdv_path = (
        SDV_DIR
        / f"{date_str}_sportsdataverse.csv"
    )

    context_path = (
        CONTEXT_DIR
        / f"{date_str}_game_context.csv"
    )

    output_path = (
        OUTPUT_DIR
        / f"{date_str}_MLB.csv"
    )

    pred = read_csv_checked(
        pred_path,
        PRED_REQUIRED,
        f"predictions {date_str}",
    )

    games = read_csv_checked(
        games_path,
        GAMES_REQUIRED,
        f"games {date_str}",
    )

    sdv = read_csv_checked(
        sdv_path,
        SDV_REQUIRED,
        f"sportsdataverse {date_str}",
    )

    context = read_csv_checked(
        context_path,
        ["gamePk"],
        f"game_context {date_str}",
    )

    joined, X = build_feature_frame(
        pred,
        games,
        sdv,
        context,
        feature_columns,
    )

    home_runs = validate_predictions(
        home_model.predict(X),
        "home_runs_model",
    )

    away_runs = validate_predictions(
        away_model.predict(X),
        "away_runs_model",
    )

    result = joined.copy()

    result["model_home_runs"] = (
        home_runs
    )

    result["model_away_runs"] = (
        away_runs
    )

    result["model_total_runs"] = (
        result["model_home_runs"]
        + result["model_away_runs"]
    )

    result["run_model_version"] = (
        model_version(
            home_metadata,
            "home",
        )
        + "|"
        + model_version(
            away_metadata,
            "away",
        )
    )

    result[
        "run_model_feature_status"
    ] = build_feature_status(
        result,
        X,
    )

    for col in [
        "dratings_home_prob",
        "dratings_away_prob",
        "dratings_home_projected_runs",
        "dratings_away_projected_runs",
        "dratings_total_projected_runs",
    ]:
        if col not in result.columns:
            fail(
                "Missing required preserved "
                f"DRatings column: {col}"
            )

    if (
        output_path.resolve()
        == pred_path.resolve()
    ):
        fail(
            "Refusing to overwrite "
            "pred_with_game_id source file"
        )

    result.to_csv(
        output_path,
        index=False,
    )

    _log(
        f"WROTE {output_path} "
        f"rows={len(result)} "
        f"features={len(feature_columns)}"
    )

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "dates",
        nargs="*",
        help=(
            "Optional date(s) as "
            "YYYY_MM_DD or YYYY-MM-DD. "
            "If omitted, rebuild every "
            "prediction date."
        ),
    )

    return parser.parse_args()


def normalize_date_arg(
    value: str,
) -> str:
    return (
        str(value)
        .strip()
        .replace("-", "_")
    )


def discover_all_dates() -> list[str]:
    files = sorted(
        PRED_DIR.glob("*_MLB.csv")
    )

    if not files:
        fail(
            "No prediction files found "
            f"in {PRED_DIR}"
        )

    dates = [
        path.stem[:-4]
        for path in files
    ]

    return dates


def load_runtime_models():
    _log(
        "Loading model metadata once "
        "for entire run"
    )

    home_metadata = load_metadata(
        HOME_METADATA_FILE,
        "home_runs",
    )

    away_metadata = load_metadata(
        AWAY_METADATA_FILE,
        "away_runs",
    )

    feature_columns = (
        assert_metadata_feature_contract(
            home_metadata,
            away_metadata,
        )
    )

    _log(
        "Loading trained models once "
        "for entire run"
    )

    home_model = load_model(
        HOME_MODEL_FILE,
        "home_runs",
    )

    away_model = load_model(
        AWAY_MODEL_FILE,
        "away_runs",
    )

    assert_model_feature_order(
        home_model,
        feature_columns,
        "home_runs",
    )

    assert_model_feature_order(
        away_model,
        feature_columns,
        "away_runs",
    )

    _log(
        "Model loading complete; "
        f"features={len(feature_columns)}"
    )

    return (
        home_model,
        away_model,
        home_metadata,
        away_metadata,
        feature_columns,
    )


def main() -> None:
    args = parse_args()

    with LOG_FILE.open(
        "w",
        encoding="utf-8",
    ) as f:
        f.write(
            "=== build_run_projection "
            f"RUN {_now()} ===\n"
        )

    try:
        dates = [
            normalize_date_arg(value)
            for value in args.dates
            if str(value).strip()
        ]

        if not dates:
            dates = discover_all_dates()

        _log(
            "Dates to rebuild: "
            f"{len(dates)} "
            f"first={dates[0]} "
            f"last={dates[-1]}"
        )

        (
            home_model,
            away_model,
            home_metadata,
            away_metadata,
            feature_columns,
        ) = load_runtime_models()

        for date_str in dates:
            output_path = process_date(
                date_str=date_str,
                home_model=home_model,
                away_model=away_model,
                home_metadata=home_metadata,
                away_metadata=away_metadata,
                feature_columns=feature_columns,
            )

            print(
                f"WROTE {output_path}"
            )

        _log(
            "SUCCESS "
            f"dates_processed={len(dates)}"
        )

    except Exception as exc:
        _log(
            f"FATAL: {exc}\n"
            f"{traceback.format_exc()}",
            "ERROR",
        )

        print(
            "build_run_projection failed: "
            f"{exc}"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    main()
