#!/usr/bin/env python3
"""
Test whether additional pregame data improves MLB moneyline probabilities.

EXPERIMENT ONLY.
This script does not overwrite production models or pipeline outputs.

Tests:
    baseline
    starter_extra
    bullpen
    team_hitting
    team_form
    all_extra

Outputs:
    <repo>/mlb_moneyline_feature_test_report.html
    <repo>/mlb_moneyline_feature_test_summary.csv
    <repo>/mlb_moneyline_feature_test_predictions.csv
"""

from __future__ import annotations

import html
import importlib.util
import math
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor


# ============================================================
# PATHS
# ============================================================


def repo_root() -> Path:
    for start in (Path.cwd().resolve(), Path(__file__).resolve().parent):
        for candidate in (start, *start.parents):
            if (candidate / "docs/win/baseball/mlb").exists():
                return candidate

    raise RuntimeError("Could not locate repository root")


ROOT = repo_root()
BASE = ROOT / "docs/win/baseball/mlb"

TRAINING_FILE = BASE / "modeling/data/mlb_run_training_set.csv"
SDV_DIR = BASE / "00_intake/sportsdataverse"

EXPERIMENT_DIR = (
    BASE / "modeling/experiments/moneyline_feature_expansion"
)

STATCAST_CACHE_DIR = EXPERIMENT_DIR / "statcast_cache"

REPORT_FILE = ROOT / "mlb_moneyline_feature_test_report.html"
SUMMARY_FILE = ROOT / "mlb_moneyline_feature_test_summary.csv"
PREDICTIONS_FILE = ROOT / "mlb_moneyline_feature_test_predictions.csv"

EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
STATCAST_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOAD CURRENT PIPELINE MODULES
# ============================================================


def load_module(name: str, path: Path):
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location(name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


TRAIN = load_module(
    "moneyline_test_train",
    BASE / "scripts/modeling/train_run_model.py",
)

JUICE = load_module(
    "moneyline_test_juice",
    BASE / "scripts/01_merge/build_juice_files.py",
)

TRAIN._log = lambda *args, **kwargs: None

if hasattr(JUICE, "_log"):
    JUICE._log = lambda *args, **kwargs: None


# ============================================================
# SETTINGS
# ============================================================

STATCAST_CHUNK_DAYS = 4
STATCAST_FETCH_RETRIES = 3

EVALUATION_FRACTION = 0.30
MIN_DEVELOPMENT_DATES = 45


TEAM_ALIASES = {
    "ARI": "ARI",
    "AZ": "ARI",
    "ARIZONA DIAMONDBACKS": "ARI",

    "ATL": "ATL",
    "ATLANTA BRAVES": "ATL",

    "ATH": "ATH",
    "OAK": "ATH",
    "ATHLETICS": "ATH",
    "OAKLAND ATHLETICS": "ATH",
    "SACRAMENTO ATHLETICS": "ATH",

    "BAL": "BAL",
    "BALTIMORE ORIOLES": "BAL",

    "BOS": "BOS",
    "BOSTON RED SOX": "BOS",

    "CHC": "CHC",
    "CHICAGO CUBS": "CHC",

    "CWS": "CWS",
    "CHW": "CWS",
    "CHICAGO WHITE SOX": "CWS",

    "CIN": "CIN",
    "CINCINNATI REDS": "CIN",

    "CLE": "CLE",
    "CLEVELAND GUARDIANS": "CLE",

    "COL": "COL",
    "COLORADO ROCKIES": "COL",

    "DET": "DET",
    "DETROIT TIGERS": "DET",

    "HOU": "HOU",
    "HOUSTON ASTROS": "HOU",

    "KC": "KC",
    "KCR": "KC",
    "KANSAS CITY ROYALS": "KC",

    "LAA": "LAA",
    "LOS ANGELES ANGELS": "LAA",

    "LAD": "LAD",
    "LOS ANGELES DODGERS": "LAD",

    "MIA": "MIA",
    "MIAMI MARLINS": "MIA",

    "MIL": "MIL",
    "MILWAUKEE BREWERS": "MIL",

    "MIN": "MIN",
    "MINNESOTA TWINS": "MIN",

    "NYM": "NYM",
    "NEW YORK METS": "NYM",

    "NYY": "NYY",
    "NEW YORK YANKEES": "NYY",

    "PHI": "PHI",
    "PHILADELPHIA PHILLIES": "PHI",

    "PIT": "PIT",
    "PITTSBURGH PIRATES": "PIT",

    "SD": "SD",
    "SDP": "SD",
    "SAN DIEGO PADRES": "SD",

    "SF": "SF",
    "SFG": "SF",
    "SAN FRANCISCO GIANTS": "SF",

    "SEA": "SEA",
    "SEATTLE MARINERS": "SEA",

    "STL": "STL",
    "ST. LOUIS CARDINALS": "STL",
    "ST LOUIS CARDINALS": "STL",

    "TB": "TB",
    "TBR": "TB",
    "TAMPA BAY RAYS": "TB",

    "TEX": "TEX",
    "TEXAS RANGERS": "TEX",

    "TOR": "TOR",
    "TORONTO BLUE JAYS": "TOR",

    "WSH": "WSH",
    "WSN": "WSH",
    "WASHINGTON NATIONALS": "WSH",
}


STARTER_EXTRA_FEATURES = [
    "home_sp_pitch_types_extra",
    "away_sp_pitch_types_extra",

    "home_sp_avg_spin_extra",
    "away_sp_avg_spin_extra",

    "home_sp_avg_spin_30d_extra",
    "away_sp_avg_spin_30d_extra",

    "home_sp_spin_delta_30d_extra",
    "away_sp_spin_delta_30d_extra",

    "home_sp_stuff_sample_extra",
    "away_sp_stuff_sample_extra",

    "home_sp_command_sample_extra",
    "away_sp_command_sample_extra",

    "home_sp_days_rest_extra",
    "away_sp_days_rest_extra",
]


TEAM_HITTING_FEATURES = [
    "home_hit_pa_30d",
    "away_hit_pa_30d",

    "home_hit_woba_30d",
    "away_hit_woba_30d",

    "home_hit_k_rate_30d",
    "away_hit_k_rate_30d",

    "home_hit_bb_rate_30d",
    "away_hit_bb_rate_30d",

    "home_hit_hard_rate_30d",
    "away_hit_hard_rate_30d",

    "home_hit_avg_ev_30d",
    "away_hit_avg_ev_30d",
]


BULLPEN_FEATURES = [
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


TEAM_FORM_FEATURES = [
    "home_form_win_pct_10",
    "away_form_win_pct_10",

    "home_form_runs_for_10",
    "away_form_runs_for_10",

    "home_form_runs_against_10",
    "away_form_runs_against_10",

    "home_form_run_diff_10",
    "away_form_run_diff_10",
]


STATCAST_KEEP_COLUMNS = [
    "game_date",
    "game_pk",
    "home_team",
    "away_team",
    "inning_topbot",
    "inning",
    "at_bat_number",
    "pitch_number",
    "pitcher",
    "batter",
    "events",
    "woba_value",
    "launch_speed",
]


STATCAST_REQUIRED_COLUMNS = [
    "game_date",
    "game_pk",
    "home_team",
    "away_team",
    "inning_topbot",
    "at_bat_number",
    "pitch_number",
    "pitcher",
    "events",
    "woba_value",
    "launch_speed",
]


STRIKEOUT_EVENTS = {
    "strikeout",
    "strikeout_double_play",
}

WALK_EVENTS = {
    "walk",
    "intent_walk",
}


# ============================================================
# BASIC HELPERS
# ============================================================


def canonical_team(value) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip().upper()

    if not text:
        return None

    return TEAM_ALIASES.get(text)


def normalize_gamepk(value) -> str | None:
    if value is None or pd.isna(value):
        return None

    try:
        return str(int(float(value)))

    except (TypeError, ValueError):
        text = str(value).strip()
        return text or None


def normalize_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        series.astype("string").str.replace("_", "-", regex=False),
        errors="coerce",
    ).dt.normalize()


def numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator is None or denominator <= 0:
        return np.nan

    if not np.isfinite(denominator):
        return np.nan

    if numerator is None or not np.isfinite(numerator):
        return np.nan

    return float(numerator / denominator)


def as_pandas(frame) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()

    if isinstance(frame, pd.DataFrame):
        return frame.copy()

    if hasattr(frame, "to_pandas"):
        return frame.to_pandas()

    return pd.DataFrame(frame)


# ============================================================
# TRAINING DATA
# ============================================================


def load_training():
    df = TRAIN.load_training_set(TRAINING_FILE)

    baseline_features = TRAIN.determine_feature_columns(df)

    df = TRAIN.coerce_and_validate_training_data(
        df,
        baseline_features,
    )

    forbidden = [
        col
        for col in baseline_features
        if any(
            token in col.lower()
            for token in (
                "odds",
                "moneyline",
                "run_line",
                "dk_",
            )
        )
    ]

    if forbidden:
        raise RuntimeError(
            f"Sportsbook columns entered model features: {forbidden}"
        )

    df["_gamePk"] = df["gamePk"].map(normalize_gamepk)

    df["_home_code"] = df["home_team"].map(canonical_team)
    df["_away_code"] = df["away_team"].map(canonical_team)

    if df["_gamePk"].isna().any():
        raise RuntimeError("Training data contains missing gamePk")

    if df["_gamePk"].duplicated().any():
        raise RuntimeError("Training data contains duplicate gamePk")

    bad_teams = df[
        df["_home_code"].isna()
        | df["_away_code"].isna()
    ]

    if not bad_teams.empty:
        sample = (
            bad_teams[
                ["home_team", "away_team"]
            ]
            .drop_duplicates()
            .head(20)
        )

        raise RuntimeError(
            "Could not map training teams:\n"
            + sample.to_string(index=False)
        )

    df = (
        df.sort_values(
            ["_game_date_dt", "_gamePk"]
        )
        .reset_index(drop=True)
    )

    return df, baseline_features


# ============================================================
# EXTRA STARTING-PITCHER DATA
# ============================================================


def load_starter_extra(
    training: pd.DataFrame,
) -> pd.DataFrame:

    source_map = {
        "sdv_home_sp_pitch_types":
            "home_sp_pitch_types_extra",

        "sdv_away_sp_pitch_types":
            "away_sp_pitch_types_extra",

        "sdv_home_sp_avg_spin":
            "home_sp_avg_spin_extra",

        "sdv_away_sp_avg_spin":
            "away_sp_avg_spin_extra",

        "sdv_home_sp_avg_spin_30d":
            "home_sp_avg_spin_30d_extra",

        "sdv_away_sp_avg_spin_30d":
            "away_sp_avg_spin_30d_extra",

        "sdv_home_sp_stuff_scored_pitches":
            "home_sp_stuff_sample_extra",

        "sdv_away_sp_stuff_scored_pitches":
            "away_sp_stuff_sample_extra",

        "sdv_home_sp_command_scored_pitches":
            "home_sp_command_sample_extra",

        "sdv_away_sp_command_scored_pitches":
            "away_sp_command_sample_extra",
    }

    training_dates = {
        row["_gamePk"]: pd.Timestamp(
            row["_game_date_dt"]
        ).normalize()
        for _, row in training[
            ["_gamePk", "_game_date_dt"]
        ].iterrows()
    }

    frames = []

    for path in sorted(
        SDV_DIR.glob("*_sportsdataverse.csv")
    ):
        df = pd.read_csv(
            path,
            encoding="utf-8-sig",
        )

        if (
            df.empty
            or "gamePk" not in df.columns
            or "game_date" not in df.columns
        ):
            continue

        df["_gamePk"] = (
            df["gamePk"]
            .map(normalize_gamepk)
        )

        df["_game_date_dt"] = (
            normalize_date_series(
                df["game_date"]
            )
        )

        if "sdv_as_of_date" in df.columns:
            df["_sdv_as_of_dt"] = (
                normalize_date_series(
                    df["sdv_as_of_date"]
                )
            )

        else:
            df["_sdv_as_of_dt"] = pd.NaT

        # Keep only games that actually exist
        # in the historical training dataset.
        df = df[
            df["_gamePk"].isin(
                training_dates
            )
        ].copy()

        if df.empty:
            continue

        # A repeated gamePk can exist in more than
        # one historical SDV snapshot. Only accept
        # rows whose stated game date matches the
        # training dataset's actual game date.
        expected_date = (
            df["_gamePk"]
            .map(training_dates)
        )

        df = df[
            df["_game_date_dt"]
            == expected_date
        ].copy()

        if df.empty:
            continue

        # Pregame safety check.
        valid_as_of = (
            df["_sdv_as_of_dt"].isna()
            | (
                df["_sdv_as_of_dt"]
                < df["_game_date_dt"]
            )
        )

        df = df[
            valid_as_of
        ].copy()

        if df.empty:
            continue

        for source, target in source_map.items():
            if source in df.columns:
                df[target] = numeric(
                    df[source]
                )
            else:
                df[target] = np.nan

        for side in ("home", "away"):
            season_spin = (
                f"{side}_sp_avg_spin_extra"
            )

            recent_spin = (
                f"{side}_sp_avg_spin_30d_extra"
            )

            delta_col = (
                f"{side}_sp_spin_delta_30d_extra"
            )

            df[delta_col] = (
                df[recent_spin]
                - df[season_spin]
            )

            last_game_source = (
                f"sdv_{side}_sp_last_game_date"
            )

            rest_target = (
                f"{side}_sp_days_rest_extra"
            )

            if last_game_source in df.columns:
                last_date = (
                    normalize_date_series(
                        df[last_game_source]
                    )
                )

                df[rest_target] = (
                    df["_game_date_dt"]
                    - last_date
                ).dt.days

            else:
                df[rest_target] = np.nan

        for col in STARTER_EXTRA_FEATURES:
            if col not in df.columns:
                df[col] = np.nan

        df["_feature_count"] = (
            df[STARTER_EXTRA_FEATURES]
            .notna()
            .sum(axis=1)
        )

        df["_source_file"] = path.name

        frames.append(
            df[
                [
                    "_gamePk",
                    "_game_date_dt",
                    "_sdv_as_of_dt",
                    "_feature_count",
                    "_source_file",
                    *STARTER_EXTRA_FEATURES,
                ]
            ].copy()
        )

    if not frames:
        raise RuntimeError(
            f"No usable historical SportsDataverse files found in {SDV_DIR}"
        )

    out = pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )

    raw_rows = len(out)

    duplicate_rows = int(
        out["_gamePk"]
        .duplicated(keep=False)
        .sum()
    )

    duplicate_games = int(
        out.loc[
            out["_gamePk"].duplicated(
                keep=False
            ),
            "_gamePk",
        ]
        .nunique()
    )

    # Important:
    # duplicate historical SDV snapshots are not
    # an error. Prefer the row with the most usable
    # features, then the latest valid pregame
    # as-of date.
    out = (
        out.sort_values(
            [
                "_gamePk",
                "_feature_count",
                "_sdv_as_of_dt",
                "_source_file",
            ],
            ascending=[
                True,
                False,
                False,
                False,
            ],
            na_position="last",
        )
        .drop_duplicates(
            subset=["_gamePk"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    if out["_gamePk"].duplicated().any():
        raise RuntimeError(
            "SportsDataverse deduplication failed"
        )

    print(
        "SportsDataverse starter rows: "
        f"{raw_rows} raw, "
        f"{duplicate_games} duplicated games, "
        f"{len(out)} unique games retained."
    )

    if duplicate_rows:
        print(
            "Historical duplicate SDV snapshots "
            "were safely reduced to one pregame "
            "row per game."
        )

    return out[
        [
            "_gamePk",
            *STARTER_EXTRA_FEATURES,
        ]
    ].copy()


# ============================================================
# STATCAST DOWNLOAD CACHE
# ============================================================


def statcast_chunk_path(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> Path:

    return (
        STATCAST_CACHE_DIR
        / (
            f"{start_date.strftime('%Y_%m_%d')}"
            "_"
            f"{end_date.strftime('%Y_%m_%d')}"
            ".parquet"
        )
    )


def write_parquet(
    path: Path,
    df: pd.DataFrame,
) -> None:

    try:
        import polars as pl

        pl.from_pandas(df).write_parquet(
            path
        )

    except Exception:
        df.to_parquet(
            path,
            index=False,
        )


def read_parquet(path: Path) -> pd.DataFrame:
    try:
        import polars as pl

        return (
            pl.read_parquet(path)
            .to_pandas()
        )

    except Exception:
        return pd.read_parquet(path)


def fetch_one_statcast_chunk(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:

    try:
        from sportsdataverse.mlb import (
            mlb_statcast_search,
        )

    except Exception as exc:
        raise RuntimeError(
            "sportsdataverse.mlb.mlb_statcast_search "
            "is required"
        ) from exc

    last_error = None

    for attempt in range(
        1,
        STATCAST_FETCH_RETRIES + 1,
    ):
        try:
            raw = mlb_statcast_search(
                start_date.strftime(
                    "%Y-%m-%d"
                ),
                end_date.strftime(
                    "%Y-%m-%d"
                ),
                player_type="batter",
                game_type="R",
            )

            df = as_pandas(raw)

            if df.empty:
                return pd.DataFrame(
                    columns=STATCAST_KEEP_COLUMNS
                )

            missing = [
                col
                for col in STATCAST_REQUIRED_COLUMNS
                if col not in df.columns
            ]

            if missing:
                raise RuntimeError(
                    "Statcast response missing "
                    f"required columns: {missing}"
                )

            keep = [
                col
                for col in STATCAST_KEEP_COLUMNS
                if col in df.columns
            ]

            return df[keep].copy()

        except Exception as exc:
            last_error = exc

            if attempt < STATCAST_FETCH_RETRIES:
                time.sleep(3 * attempt)

    raise RuntimeError(
        "Statcast download failed for "
        f"{start_date.date()}.."
        f"{end_date.date()}: "
        f"{last_error}"
    )


def load_or_fetch_statcast(
    first_date: pd.Timestamp,
    last_date: pd.Timestamp,
) -> pd.DataFrame:

    chunks = []

    cursor = first_date

    while cursor <= last_date:
        chunk_end = min(
            cursor
            + pd.Timedelta(
                days=STATCAST_CHUNK_DAYS - 1
            ),
            last_date,
        )

        chunks.append(
            (cursor, chunk_end)
        )

        cursor = (
            chunk_end
            + pd.Timedelta(days=1)
        )

    frames = []

    for index, (
        start_date,
        end_date,
    ) in enumerate(
        chunks,
        start=1,
    ):
        path = statcast_chunk_path(
            start_date,
            end_date,
        )

        if path.exists():
            print(
                f"STATCAST {index}/{len(chunks)} "
                f"cache hit "
                f"{start_date.date()}.."
                f"{end_date.date()}"
            )

            frame = read_parquet(path)

        else:
            print(
                f"STATCAST {index}/{len(chunks)} "
                f"downloading "
                f"{start_date.date()}.."
                f"{end_date.date()}"
            )

            frame = fetch_one_statcast_chunk(
                start_date,
                end_date,
            )

            write_parquet(
                path,
                frame,
            )

        frames.append(frame)

    raw = pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )

    if raw.empty:
        raise RuntimeError(
            "Statcast history is empty"
        )

    missing = [
        col
        for col in STATCAST_REQUIRED_COLUMNS
        if col not in raw.columns
    ]

    if missing:
        raise RuntimeError(
            "Combined Statcast data missing "
            f"columns: {missing}"
        )

    raw["_game_date_dt"] = (
        pd.to_datetime(
            raw["game_date"],
            errors="coerce",
        )
        .dt.normalize()
    )

    raw["_gamePk"] = (
        raw["game_pk"]
        .map(normalize_gamepk)
    )

    raw["_home_code"] = (
        raw["home_team"]
        .map(canonical_team)
    )

    raw["_away_code"] = (
        raw["away_team"]
        .map(canonical_team)
    )

    bad = (
        raw["_game_date_dt"].isna()
        | raw["_gamePk"].isna()
        | raw["_home_code"].isna()
        | raw["_away_code"].isna()
    )

    if bad.any():
        sample = (
            raw.loc[
                bad,
                [
                    "game_date",
                    "game_pk",
                    "home_team",
                    "away_team",
                ],
            ]
            .drop_duplicates()
            .head(20)
        )

        raise RuntimeError(
            "Statcast rows contain invalid "
            "dates, game IDs, or team names:\n"
            + sample.to_string(index=False)
        )

    dedupe_keys = [
        col
        for col in (
            "_gamePk",
            "at_bat_number",
            "pitch_number",
            "pitcher",
            "batter",
        )
        if col in raw.columns
    ]

    raw = raw.drop_duplicates(
        subset=dedupe_keys,
        keep="last",
    )

    return (
        raw.sort_values(
            [
                "_game_date_dt",
                "_gamePk",
                "at_bat_number",
                "pitch_number",
            ]
        )
        .reset_index(drop=True)
    )


# ============================================================
# PREPARE STATCAST
# ============================================================


def prepare_statcast(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    df = raw.copy()

    top = (
        df["inning_topbot"]
        .astype("string")
        .str.lower()
        .str.startswith("top")
    )

    bottom = (
        df["inning_topbot"]
        .astype("string")
        .str.lower()
        .str.startswith("bot")
    )

    if (~(top | bottom)).any():
        bad_values = sorted(
            df.loc[
                ~(top | bottom),
                "inning_topbot",
            ]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

        raise RuntimeError(
            "Unexpected inning_topbot "
            f"values: {bad_values}"
        )

    df["_batting_team"] = np.where(
        top,
        df["_away_code"],
        df["_home_code"],
    )

    df["_pitching_team"] = np.where(
        top,
        df["_home_code"],
        df["_away_code"],
    )

    df["_at_bat_number"] = numeric(
        df["at_bat_number"]
    )

    df["_pitch_number"] = numeric(
        df["pitch_number"]
    )

    if "inning" in df.columns:
        df["_inning"] = numeric(
            df["inning"]
        )

    else:
        df["_inning"] = np.nan

    df["_pitcher"] = (
        numeric(df["pitcher"])
        .astype("Int64")
    )

    df["_events"] = (
        df["events"]
        .astype("string")
        .str.strip()
        .str.lower()
    )

    df["_woba_value"] = numeric(
        df["woba_value"]
    )

    df["_launch_speed"] = numeric(
        df["launch_speed"]
    )

    ordered = (
        df.sort_values(
            [
                "_game_date_dt",
                "_gamePk",
                "_pitching_team",
                "_inning",
                "_at_bat_number",
                "_pitch_number",
            ]
        )
        .copy()
    )

    starters = (
        ordered[
            ordered["_pitcher"].notna()
        ]
        .groupby(
            [
                "_gamePk",
                "_pitching_team",
            ],
            as_index=False,
            sort=False,
        )
        .first()[
            [
                "_gamePk",
                "_pitching_team",
                "_pitcher",
            ]
        ]
        .rename(
            columns={
                "_pitcher":
                    "_starter_pitcher"
            }
        )
    )

    df = df.merge(
        starters,
        on=[
            "_gamePk",
            "_pitching_team",
        ],
        how="left",
        validate="many_to_one",
    )

    df["_is_bullpen"] = (
        df["_pitcher"].notna()
        & df["_starter_pitcher"].notna()
        & (
            df["_pitcher"]
            != df["_starter_pitcher"]
        )
    )

    df["_is_pa"] = (
        df["_events"].notna()
        & (df["_events"] != "")
    )

    df["_is_k"] = (
        df["_events"]
        .isin(STRIKEOUT_EVENTS)
    )

    df["_is_bb"] = (
        df["_events"]
        .isin(WALK_EVENTS)
    )

    df["_is_bbe"] = (
        df["_launch_speed"].notna()
    )

    df["_is_hard"] = (
        df["_launch_speed"] >= 95.0
    )

    return df


# ============================================================
# DAILY TEAM HITTING
# ============================================================


def build_daily_hitting(
    statcast: pd.DataFrame,
) -> pd.DataFrame:

    pa = statcast[
        statcast["_is_pa"]
    ].copy()

    if pa.empty:
        raise RuntimeError(
            "No plate appearances found"
        )

    pa["_woba_sum"] = (
        pa["_woba_value"]
        .fillna(0.0)
    )

    pa["_woba_n"] = (
        pa["_woba_value"]
        .notna()
        .astype(int)
    )

    pa["_ev_sum"] = (
        pa["_launch_speed"]
        .fillna(0.0)
    )

    pa["_ev_n"] = (
        pa["_launch_speed"]
        .notna()
        .astype(int)
    )

    return (
        pa.groupby(
            [
                "_game_date_dt",
                "_batting_team",
            ],
            as_index=False,
        )
        .agg(
            hit_pa=("_is_pa", "sum"),
            hit_woba_sum=("_woba_sum", "sum"),
            hit_woba_n=("_woba_n", "sum"),
            hit_k=("_is_k", "sum"),
            hit_bb=("_is_bb", "sum"),
            hit_bbe=("_is_bbe", "sum"),
            hit_hard=("_is_hard", "sum"),
            hit_ev_sum=("_ev_sum", "sum"),
            hit_ev_n=("_ev_n", "sum"),
        )
        .rename(
            columns={
                "_batting_team": "team"
            }
        )
    )


# ============================================================
# DAILY BULLPEN
# ============================================================


def build_daily_bullpen(
    statcast: pd.DataFrame,
) -> pd.DataFrame:

    bp = statcast[
        statcast["_is_bullpen"]
    ].copy()

    if bp.empty:
        raise RuntimeError(
            "No bullpen pitches identified"
        )

    workload = (
        bp.groupby(
            [
                "_game_date_dt",
                "_pitching_team",
            ],
            as_index=False,
        )
        .size()
        .rename(
            columns={
                "_pitching_team": "team",
                "size": "bp_pitches",
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

    results = (
        pa.groupby(
            [
                "_game_date_dt",
                "_pitching_team",
            ],
            as_index=False,
        )
        .agg(
            bp_pa=("_is_pa", "sum"),
            bp_woba_sum=("_woba_sum", "sum"),
            bp_woba_n=("_woba_n", "sum"),
            bp_k=("_is_k", "sum"),
            bp_bb=("_is_bb", "sum"),
            bp_bbe=("_is_bbe", "sum"),
            bp_hard=("_is_hard", "sum"),
        )
        .rename(
            columns={
                "_pitching_team": "team"
            }
        )
    )

    return workload.merge(
        results,
        on=[
            "_game_date_dt",
            "team",
        ],
        how="left",
        validate="one_to_one",
    )


# ============================================================
# ROLLING TEAM FEATURES
# ============================================================


def rolling_hitting_features(
    daily: pd.DataFrame,
    team: str,
    target_date: pd.Timestamp,
):

    start = (
        target_date
        - pd.Timedelta(days=30)
    )

    part = daily[
        (daily["team"] == team)
        & (
            daily["_game_date_dt"]
            >= start
        )
        & (
            daily["_game_date_dt"]
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

    pa = float(
        part["hit_pa"].sum()
    )

    woba_n = float(
        part["hit_woba_n"].sum()
    )

    bbe = float(
        part["hit_bbe"].sum()
    )

    ev_n = float(
        part["hit_ev_n"].sum()
    )

    return {
        "pa":
            pa,

        "woba":
            safe_ratio(
                float(
                    part["hit_woba_sum"].sum()
                ),
                woba_n,
            ),

        "k_rate":
            safe_ratio(
                float(
                    part["hit_k"].sum()
                ),
                pa,
            ),

        "bb_rate":
            safe_ratio(
                float(
                    part["hit_bb"].sum()
                ),
                pa,
            ),

        "hard_rate":
            safe_ratio(
                float(
                    part["hit_hard"].sum()
                ),
                bbe,
            ),

        "avg_ev":
            safe_ratio(
                float(
                    part["hit_ev_sum"].sum()
                ),
                ev_n,
            ),
    }


def rolling_bullpen_features(
    daily: pd.DataFrame,
    team: str,
    target_date: pd.Timestamp,
):

    performance_start = (
        target_date
        - pd.Timedelta(days=14)
    )

    workload_start = (
        target_date
        - pd.Timedelta(days=3)
    )

    performance = daily[
        (daily["team"] == team)
        & (
            daily["_game_date_dt"]
            >= performance_start
        )
        & (
            daily["_game_date_dt"]
            < target_date
        )
    ]

    workload = daily[
        (daily["team"] == team)
        & (
            daily["_game_date_dt"]
            >= workload_start
        )
        & (
            daily["_game_date_dt"]
            < target_date
        )
    ]

    if performance.empty:
        pa = 0.0
        woba = np.nan
        k_rate = np.nan
        bb_rate = np.nan
        hard_rate = np.nan

    else:
        pa = float(
            performance["bp_pa"]
            .fillna(0.0)
            .sum()
        )

        woba_n = float(
            performance["bp_woba_n"]
            .fillna(0.0)
            .sum()
        )

        bbe = float(
            performance["bp_bbe"]
            .fillna(0.0)
            .sum()
        )

        woba = safe_ratio(
            float(
                performance[
                    "bp_woba_sum"
                ]
                .fillna(0.0)
                .sum()
            ),
            woba_n,
        )

        k_rate = safe_ratio(
            float(
                performance["bp_k"]
                .fillna(0.0)
                .sum()
            ),
            pa,
        )

        bb_rate = safe_ratio(
            float(
                performance["bp_bb"]
                .fillna(0.0)
                .sum()
            ),
            pa,
        )

        hard_rate = safe_ratio(
            float(
                performance["bp_hard"]
                .fillna(0.0)
                .sum()
            ),
            bbe,
        )

    pitches_3d = (
        float(
            workload["bp_pitches"]
            .fillna(0.0)
            .sum()
        )
        if not workload.empty
        else 0.0
    )

    return {
        "pa": pa,
        "woba": woba,
        "k_rate": k_rate,
        "bb_rate": bb_rate,
        "hard_rate": hard_rate,
        "pitches_3d": pitches_3d,
    }


def attach_statcast_team_features(
    games: pd.DataFrame,
    daily_hitting: pd.DataFrame,
    daily_bullpen: pd.DataFrame,
) -> pd.DataFrame:

    out = games.copy()

    hitting_cache = {}
    bullpen_cache = {}

    values = {
        col: []
        for col in [
            *TEAM_HITTING_FEATURES,
            *BULLPEN_FEATURES,
        ]
    }

    for _, row in out.iterrows():
        target_date = pd.Timestamp(
            row["_game_date_dt"]
        ).normalize()

        for side in ("home", "away"):
            team = row[
                f"_{side}_code"
            ]

            key = (
                target_date,
                team,
            )

            if key not in hitting_cache:
                hitting_cache[key] = (
                    rolling_hitting_features(
                        daily_hitting,
                        team,
                        target_date,
                    )
                )

            if key not in bullpen_cache:
                bullpen_cache[key] = (
                    rolling_bullpen_features(
                        daily_bullpen,
                        team,
                        target_date,
                    )
                )

            hitting = hitting_cache[key]
            bullpen = bullpen_cache[key]

            values[
                f"{side}_hit_pa_30d"
            ].append(
                hitting["pa"]
            )

            values[
                f"{side}_hit_woba_30d"
            ].append(
                hitting["woba"]
            )

            values[
                f"{side}_hit_k_rate_30d"
            ].append(
                hitting["k_rate"]
            )

            values[
                f"{side}_hit_bb_rate_30d"
            ].append(
                hitting["bb_rate"]
            )

            values[
                f"{side}_hit_hard_rate_30d"
            ].append(
                hitting["hard_rate"]
            )

            values[
                f"{side}_hit_avg_ev_30d"
            ].append(
                hitting["avg_ev"]
            )

            values[
                f"{side}_bp_pa_14d"
            ].append(
                bullpen["pa"]
            )

            values[
                f"{side}_bp_woba_allowed_14d"
            ].append(
                bullpen["woba"]
            )

            values[
                f"{side}_bp_k_rate_14d"
            ].append(
                bullpen["k_rate"]
            )

            values[
                f"{side}_bp_bb_rate_14d"
            ].append(
                bullpen["bb_rate"]
            )

            values[
                f"{side}_bp_hard_rate_14d"
            ].append(
                bullpen["hard_rate"]
            )

            values[
                f"{side}_bp_pitches_3d"
            ].append(
                bullpen["pitches_3d"]
            )

    for col, col_values in values.items():
        out[col] = col_values

    return out


# ============================================================
# TEAM FORM
# ============================================================


def attach_team_form(
    games: pd.DataFrame,
) -> pd.DataFrame:

    out = games.copy()

    history = {}

    feature_values = {
        col: pd.Series(
            np.nan,
            index=out.index,
            dtype=float,
        )
        for col in TEAM_FORM_FEATURES
    }

    grouped = out.groupby(
        "_game_date_dt",
        sort=True,
    )

    for _, indices in grouped.groups.items():
        day_indices = list(indices)

        # Build features before adding results
        # from this date.
        for idx in day_indices:
            row = out.loc[idx]

            for side in ("home", "away"):
                team = row[
                    f"_{side}_code"
                ]

                prior = history.get(
                    team,
                    [],
                )[-10:]

                if prior:
                    win_pct = float(
                        np.mean(
                            [
                                item["win"]
                                for item in prior
                            ]
                        )
                    )

                    runs_for = float(
                        np.mean(
                            [
                                item["runs_for"]
                                for item in prior
                            ]
                        )
                    )

                    runs_against = float(
                        np.mean(
                            [
                                item["runs_against"]
                                for item in prior
                            ]
                        )
                    )

                    run_diff = (
                        runs_for
                        - runs_against
                    )

                else:
                    win_pct = np.nan
                    runs_for = np.nan
                    runs_against = np.nan
                    run_diff = np.nan

                feature_values[
                    f"{side}_form_win_pct_10"
                ].loc[idx] = win_pct

                feature_values[
                    f"{side}_form_runs_for_10"
                ].loc[idx] = runs_for

                feature_values[
                    f"{side}_form_runs_against_10"
                ].loc[idx] = runs_against

                feature_values[
                    f"{side}_form_run_diff_10"
                ].loc[idx] = run_diff

        # Now add this date's completed games.
        for idx in day_indices:
            row = out.loc[idx]

            home_runs = float(
                row["target_home_runs"]
            )

            away_runs = float(
                row["target_away_runs"]
            )

            home_team = row[
                "_home_code"
            ]

            away_team = row[
                "_away_code"
            ]

            history.setdefault(
                home_team,
                [],
            ).append(
                {
                    "win":
                        1.0
                        if home_runs > away_runs
                        else 0.0,

                    "runs_for":
                        home_runs,

                    "runs_against":
                        away_runs,
                }
            )

            history.setdefault(
                away_team,
                [],
            ).append(
                {
                    "win":
                        1.0
                        if away_runs > home_runs
                        else 0.0,

                    "runs_for":
                        away_runs,

                    "runs_against":
                        home_runs,
                }
            )

    for col, values in feature_values.items():
        out[col] = values

    return out


# ============================================================
# FEATURE COVERAGE
# ============================================================


def feature_coverage(
    frame: pd.DataFrame,
    groups: dict[str, list[str]],
) -> pd.DataFrame:

    rows = []

    for group, columns in groups.items():
        for col in columns:
            coverage = (
                float(
                    frame[col]
                    .notna()
                    .mean()
                )
                if col in frame.columns
                else 0.0
            )

            rows.append(
                {
                    "group": group,
                    "feature": col,
                    "coverage_pct":
                        coverage * 100.0,
                }
            )

    return pd.DataFrame(rows)


# ============================================================
# TEST DATE SPLIT
# ============================================================


def choose_evaluation_dates(
    frame: pd.DataFrame,
):

    dates = [
        pd.Timestamp(value).normalize()
        for value in sorted(
            frame[
                "_game_date_dt"
            ]
            .dropna()
            .unique()
        )
    ]

    if len(dates) < (
        MIN_DEVELOPMENT_DATES + 5
    ):
        raise RuntimeError(
            "Not enough historical dates "
            f"for experiment: {len(dates)}"
        )

    evaluation_count = max(
        5,
        int(
            math.ceil(
                len(dates)
                * EVALUATION_FRACTION
            )
        ),
    )

    split_index = max(
        MIN_DEVELOPMENT_DATES,
        len(dates)
        - evaluation_count,
    )

    if split_index >= len(dates):
        raise RuntimeError(
            "Evaluation split left "
            "no test dates"
        )

    return (
        dates[split_index],
        dates[split_index:],
    )


# ============================================================
# MODEL TRAINING
# ============================================================


def select_params(
    development: pd.DataFrame,
    variants: dict[str, list[str]],
):

    split = TRAIN.chronological_date_split(
        development
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
            f"TUNING {index}/{len(variants)} "
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

            "home_validation_score":
                home_score,

            "away_validation_score":
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


# ============================================================
# WALK-FORWARD MONEYLINE TEST
# ============================================================


def walk_forward_predictions(
    frame: pd.DataFrame,
    evaluation_dates,
    variants,
    selected_params,
):

    rows = []

    for date_index, target_date in enumerate(
        evaluation_dates,
        start=1,
    ):
        print(
            f"TEST DATE {date_index}/"
            f"{len(evaluation_dates)} "
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

        for variant_name, features in (
            variants.items()
        ):
            home_model = fit_model(
                prior,
                features,
                "target_home_runs",
                selected_params[
                    variant_name
                ]["home"],
            )

            away_model = fit_model(
                prior,
                features,
                "target_away_runs",
                selected_params[
                    variant_name
                ]["away"],
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
                    f"{variant_name} produced "
                    "invalid home-run predictions"
                )

            if (
                np.any(~np.isfinite(away_runs))
                or np.any(away_runs < 0)
            ):
                raise RuntimeError(
                    f"{variant_name} produced "
                    "invalid away-run predictions"
                )

            for position, (_, game) in enumerate(
                current.iterrows()
            ):
                actual_home_runs = float(
                    game["target_home_runs"]
                )

                actual_away_runs = float(
                    game["target_away_runs"]
                )

                if (
                    actual_home_runs
                    == actual_away_runs
                ):
                    continue

                (
                    home_probability,
                    away_probability,
                    tie_mass,
                ) = JUICE.moneyline_probabilities(
                    float(
                        home_runs[position]
                    ),
                    float(
                        away_runs[position]
                    ),
                )

                if abs(
                    home_probability
                    + away_probability
                    - 1.0
                ) > 1e-9:
                    raise RuntimeError(
                        "Moneyline probabilities "
                        "do not sum to 1"
                    )

                rows.append(
                    {
                        "variant":
                            variant_name,

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

                        "tie_mass":
                            float(tie_mass),

                        "actual_home_runs":
                            actual_home_runs,

                        "actual_away_runs":
                            actual_away_runs,

                        "actual_home_win":
                            (
                                1.0
                                if actual_home_runs
                                > actual_away_runs
                                else 0.0
                            ),
                    }
                )

    if not rows:
        raise RuntimeError(
            "No moneyline predictions produced"
        )

    return pd.DataFrame(rows)


# ============================================================
# RESULTS
# ============================================================


def score_variant(
    part: pd.DataFrame,
):

    work = part[
        [
            "model_home_probability",
            "actual_home_win",
        ]
    ].dropna().copy()

    probabilities = (
        work[
            "model_home_probability"
        ].to_numpy(dtype=float)
    )

    outcomes = (
        work[
            "actual_home_win"
        ].to_numpy(dtype=float)
    )

    if (
        len(work) < 25
    ):
        raise RuntimeError(
            "Too few predictions "
            f"to score: {len(work)}"
        )

    if (
        np.std(probabilities) > 0
        and np.std(outcomes) > 0
    ):
        correlation = float(
            np.corrcoef(
                probabilities,
                outcomes,
            )[0, 1]
        )

    else:
        correlation = np.nan

    probability_error = float(
        np.mean(
            (
                probabilities
                - outcomes
            ) ** 2
        )
    )

    work["bucket"] = pd.qcut(
        work[
            "model_home_probability"
        ].rank(method="first"),
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

    low_win = float(
        buckets.iloc[
            0
        ]["actual_home_win_rate"]
    )

    high_win = float(
        buckets.iloc[
            -1
        ]["actual_home_win_rate"]
    )

    win_spread = (
        high_win
        - low_win
    )

    differences = np.diff(
        buckets[
            "actual_home_win_rate"
        ].to_numpy(dtype=float)
    )

    rising_steps = int(
        np.sum(differences > 0)
    )

    gaps = np.abs(
        buckets[
            "average_model_probability"
        ].to_numpy(dtype=float)
        - buckets[
            "actual_home_win_rate"
        ].to_numpy(dtype=float)
    )

    weights = (
        buckets["games"]
        .to_numpy(dtype=float)
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
                low_win,

            "highest_group_actual_win_rate":
                high_win,

            "high_minus_low_win_rate":
                win_spread,

            "rising_bucket_steps_out_of_4":
                rising_steps,

            "probability_error":
                probability_error,

            "average_bucket_probability_gap":
                average_gap,
        },
        buckets,
    )


def build_summary(
    predictions: pd.DataFrame,
):

    rows = []
    bucket_tables = {}

    for variant, part in predictions.groupby(
        "variant",
        sort=False,
    ):
        metrics, buckets = score_variant(
            part
        )

        rows.append(
            {
                "variant": variant,
                **metrics,
            }
        )

        bucket_tables[
            variant
        ] = buckets

    summary = pd.DataFrame(rows)

    baseline = summary[
        summary["variant"]
        == "baseline"
    ]

    if baseline.empty:
        raise RuntimeError(
            "Baseline result missing"
        )

    baseline_row = baseline.iloc[0]

    summary[
        "correlation_change_vs_baseline"
    ] = (
        summary[
            "probability_win_correlation"
        ]
        - float(
            baseline_row[
                "probability_win_correlation"
            ]
        )
    )

    summary[
        "probability_error_change_vs_baseline"
    ] = (
        summary["probability_error"]
        - float(
            baseline_row[
                "probability_error"
            ]
        )
    )

    summary[
        "bucket_gap_change_vs_baseline"
    ] = (
        summary[
            "average_bucket_probability_gap"
        ]
        - float(
            baseline_row[
                "average_bucket_probability_gap"
            ]
        )
    )

    return summary, bucket_tables


# ============================================================
# HTML
# ============================================================


def pct(value):
    if value is None or pd.isna(value):
        return ""

    return f"{float(value) * 100:.2f}%"


def num(value):
    if value is None or pd.isna(value):
        return ""

    return f"{float(value):.4f}"


def build_report(
    summary: pd.DataFrame,
    buckets,
    coverage: pd.DataFrame,
    evaluation_start,
    evaluation_dates,
    statcast_rows,
):

    display = summary.copy()

    for col in [
        "lowest_group_actual_win_rate",
        "highest_group_actual_win_rate",
        "high_minus_low_win_rate",
        "average_bucket_probability_gap",
        "bucket_gap_change_vs_baseline",
    ]:
        display[col] = display[col].map(
            pct
        )

    for col in [
        "probability_win_correlation",
        "probability_error",
        "correlation_change_vs_baseline",
        "probability_error_change_vs_baseline",
    ]:
        display[col] = display[col].map(
            num
        )

    display = display.rename(
        columns={
            "variant":
                "Version",

            "games":
                "Games",

            "probability_win_correlation":
                "Probability vs win result",

            "lowest_group_actual_win_rate":
                "Lowest probability group win %",

            "highest_group_actual_win_rate":
                "Highest probability group win %",

            "high_minus_low_win_rate":
                "High minus low win %",

            "rising_bucket_steps_out_of_4":
                "Rising groups out of 4",

            "probability_error":
                "Probability error",

            "average_bucket_probability_gap":
                "Predicted vs actual gap",

            "correlation_change_vs_baseline":
                "Signal change vs baseline",

            "probability_error_change_vs_baseline":
                "Error change vs baseline",

            "bucket_gap_change_vs_baseline":
                "Gap change vs baseline",
        }
    )

    coverage_display = coverage.copy()

    coverage_display[
        "coverage_pct"
    ] = coverage_display[
        "coverage_pct"
    ].map(
        lambda x: f"{x:.1f}%"
    )

    bucket_sections = []

    for variant, table in buckets.items():
        shown = table.copy()

        shown[
            "average_model_probability"
        ] = shown[
            "average_model_probability"
        ].map(pct)

        shown[
            "actual_home_win_rate"
        ] = shown[
            "actual_home_win_rate"
        ].map(pct)

        shown = shown.rename(
            columns={
                "bucket":
                    "Probability group",

                "games":
                    "Games",

                "average_model_probability":
                    "Average predicted probability",

                "actual_home_win_rate":
                    "Actual win %",
            }
        )

        bucket_sections.append(
            "<h3>"
            + html.escape(variant)
            + "</h3>"
            + shown.to_html(
                index=False,
                border=0,
            )
        )

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Moneyline Feature Test</title>
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

<h1>MLB Moneyline Feature Expansion Test</h1>

<p>
Goal: determine whether adding more pregame information causes
higher predicted moneyline probability to produce a higher actual win rate.
</p>

<p>
Evaluation begins: {evaluation_start.strftime("%Y-%m-%d")}<br>
Evaluation dates: {len(evaluation_dates)}<br>
Statcast rows: {statcast_rows:,}
</p>

<h2>Summary</h2>
{display.to_html(index=False, border=0)}

<h2>Probability Groups</h2>
{''.join(bucket_sections)}

<h2>Extra Feature Availability</h2>
{coverage_display.to_html(index=False, border=0)}

</body>
</html>
"""


# ============================================================
# MAIN
# ============================================================


def main():

    print("Loading training data...")

    frame, baseline_features = (
        load_training()
    )

    print(
        "Loading extra starting-pitcher "
        "SDV features..."
    )

    starter_extra = (
        load_starter_extra(frame)
    )

    frame = frame.merge(
        starter_extra,
        on="_gamePk",
        how="left",
        validate="one_to_one",
    )

    print(
        "Building pregame recent team form..."
    )

    frame = attach_team_form(
        frame
    )

    first_statcast_date = (
        pd.Timestamp(
            frame["_game_date_dt"].min()
        ).normalize()
    )

    last_statcast_date = (
        pd.Timestamp(
            frame["_game_date_dt"].max()
        ).normalize()
        - pd.Timedelta(days=1)
    )

    print(
        "Loading/downloading league "
        "Statcast history..."
    )

    raw_statcast = (
        load_or_fetch_statcast(
            first_statcast_date,
            last_statcast_date,
        )
    )

    statcast = prepare_statcast(
        raw_statcast
    )

    print(
        "Building team hitting "
        "and bullpen history..."
    )

    daily_hitting = build_daily_hitting(
        statcast
    )

    daily_bullpen = build_daily_bullpen(
        statcast
    )

    frame = attach_statcast_team_features(
        frame,
        daily_hitting,
        daily_bullpen,
    )

    all_extra = [
        *STARTER_EXTRA_FEATURES,
        *BULLPEN_FEATURES,
        *TEAM_HITTING_FEATURES,
        *TEAM_FORM_FEATURES,
    ]

    for col in all_extra:
        if col not in frame.columns:
            frame[col] = np.nan

        frame[col] = numeric(
            frame[col]
        )

        bad = (
            frame[col].notna()
            & ~np.isfinite(
                frame[col]
            )
        )

        if bad.any():
            raise RuntimeError(
                "Invalid experimental "
                f"feature values: {col}"
            )

    variants = {
        "baseline":
            list(
                baseline_features
            ),

        "starter_extra":
            [
                *baseline_features,
                *STARTER_EXTRA_FEATURES,
            ],

        "bullpen":
            [
                *baseline_features,
                *BULLPEN_FEATURES,
            ],

        "team_hitting":
            [
                *baseline_features,
                *TEAM_HITTING_FEATURES,
            ],

        "team_form":
            [
                *baseline_features,
                *TEAM_FORM_FEATURES,
            ],

        "all_extra":
            [
                *baseline_features,
                *STARTER_EXTRA_FEATURES,
                *BULLPEN_FEATURES,
                *TEAM_HITTING_FEATURES,
                *TEAM_FORM_FEATURES,
            ],
    }

    coverage = feature_coverage(
        frame,
        {
            "starter_extra":
                STARTER_EXTRA_FEATURES,

            "bullpen":
                BULLPEN_FEATURES,

            "team_hitting":
                TEAM_HITTING_FEATURES,

            "team_form":
                TEAM_FORM_FEATURES,
        },
    )

    (
        evaluation_start,
        evaluation_dates,
    ) = choose_evaluation_dates(
        frame
    )

    development = frame[
        frame["_game_date_dt"]
        < evaluation_start
    ].copy()

    print(
        f"Evaluation starts "
        f"{evaluation_start.date()} "
        f"with {len(evaluation_dates)} dates."
    )

    print(
        "Selecting settings using "
        "pre-evaluation games only..."
    )

    selected_params = select_params(
        development,
        variants,
    )

    print(
        "Running walk-forward "
        "moneyline test..."
    )

    predictions = (
        walk_forward_predictions(
            frame,
            evaluation_dates,
            variants,
            selected_params,
        )
    )

    summary, buckets = (
        build_summary(
            predictions
        )
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
            evaluation_start,
            evaluation_dates,
            len(statcast),
        ),
        encoding="utf-8",
    )

    print("")
    print(
        "Moneyline feature test complete."
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
        raise SystemExit(main())

    except SystemExit:
        raise

    except Exception as exc:
        print(
            f"ERROR: {exc}",
            file=sys.stderr,
        )

        traceback.print_exc()

        raise SystemExit(1)