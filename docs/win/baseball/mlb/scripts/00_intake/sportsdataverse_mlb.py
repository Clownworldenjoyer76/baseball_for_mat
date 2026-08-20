#!/usr/bin/env python3
"""Build SportsDataverse pregame features for existing MLB game files.

Reads the authoritative ``00_intake/games/{date}_games.csv`` spine, pulls or
loads cached SportsDataverse/Statcast pitcher history, and writes one clean row
per game to:

    00_intake/sportsdataverse/{date}_sportsdataverse.csv

All features are leakage-safe. For a target game date, every pitch used in
feature construction must satisfy:

    pitch.game_date < target_game_date

For the recent-form window:

    target_game_date - lookback_days <= pitch.game_date < target_game_date

The raw seasonal cache may contain pitches after a historical target date, but
those rows are filtered out before any xERA/xwOBA, Stuff+, Command+, velocity,
spin, pitch-count, or game-count calculation.

This file remains a separate upstream model input. Its ``sdv_*`` columns are
not intended to be copied wholesale through the downstream merge/edge/EV
pipeline. The run-projection stage should read this file directly and convert
the relevant SDV features into ``model_home_runs`` and ``model_away_runs``.

Model-facing feature meanings:
    sdv_*_sp_stuff_plus
        SportsDataverse pitch-quality score.
    sdv_*_sp_command_plus
        SportsDataverse location/command-quality score.

The raw SDV output column names are preserved intentionally.
"""

from __future__ import annotations

import argparse
import sys
import traceback
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl

from sportsdataverse.mlb import (
    mlb_command_plus,
    mlb_statcast_search,
    mlb_stuff_plus,
    x_era,
)
from sportsdataverse.mlb.mlb_pitch_features import pitch_features


BASE_DIR = Path("docs/win/baseball/mlb")
GAMES_DIR = BASE_DIR / "00_intake/games"
OUT_DIR = BASE_DIR / "00_intake/sportsdataverse"
CACHE_DIR = OUT_DIR / "cache"
ERROR_DIR = BASE_DIR / "errors/00_intake"

OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
ERROR_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = ERROR_DIR / "sportsdataverse_mlb.txt"

DEFAULT_LOOKBACK_DAYS = 30

# Raw SportsDataverse feature names are intentionally preserved in output.
# Model-training code may map these to clearer internal feature labels, but
# this source script should remain consistent with the existing sdv_* schema.
SDV_MODEL_FEATURE_DESCRIPTIONS = {
    "sp_stuff_plus": "SportsDataverse pitch-quality score",
    "sp_command_plus": "SportsDataverse location/command-quality score",
}

PITCHER_FEATURE_COLUMNS = [
    "pitcher_id",
    "sp_pitches",
    "sp_games",
    "sp_pitch_types",
    "sp_avg_velo",
    "sp_avg_spin",
    "sp_stuff_plus",
    "sp_stuff_scored_pitches",
    "sp_command_plus",
    "sp_command_scored_pitches",
    "sp_xwoba",
    "sp_xera",
    "sp_pitches_30d",
    "sp_games_30d",
    "sp_avg_velo_30d",
    "sp_avg_spin_30d",
    "sp_xwoba_30d",
    "sp_xera_30d",
    "sp_velo_delta_30d",
    "sp_last_game_date",
]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _log(message: str, level: str = "INFO") -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level:<5} | {message.rstrip()}\n")


def normalize_date(value: str) -> str:
    return str(value or "").strip().replace("-", "_")


def parse_date(value: str) -> date:
    text = str(value or "").strip().replace("_", "-")
    return datetime.strptime(text, "%Y-%m-%d").date()


def _empty_pitcher_features(pitcher_ids: list[int]) -> pd.DataFrame:
    frame = pd.DataFrame(
        {"pitcher_id": pd.Series(pitcher_ids, dtype="Int64")}
    )
    for col in PITCHER_FEATURE_COLUMNS[1:]:
        frame[col] = pd.NA
    return frame[PITCHER_FEATURE_COLUMNS]


def _safe_ints(values) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()

    for value in values:
        try:
            text = str(value).strip()
            if not text or text.lower() in {"nan", "none", "<na>"}:
                continue
            parsed = int(float(text))
        except (TypeError, ValueError):
            continue

        if parsed not in seen:
            seen.add(parsed)
            out.append(parsed)

    return out


def _game_date_for_file(date_str: str, games: pd.DataFrame) -> date:
    if "game_date" in games.columns:
        for value in games["game_date"].tolist():
            try:
                return parse_date(value)
            except (TypeError, ValueError):
                continue

    return parse_date(date_str)


def _as_polars(frame) -> pl.DataFrame:
    if frame is None:
        return pl.DataFrame()
    if isinstance(frame, pl.DataFrame):
        return frame
    if isinstance(frame, pd.DataFrame):
        return pl.from_pandas(frame)
    raise TypeError(f"Unsupported Statcast frame type: {type(frame)!r}")


def _with_parsed_game_date(pitches: pl.DataFrame) -> pl.DataFrame:
    if pitches is None or pitches.height == 0:
        return pl.DataFrame() if pitches is None else pitches

    if "game_date" not in pitches.columns:
        raise ValueError("Statcast data missing required game_date column")

    return pitches.with_columns(
        pl.col("game_date")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("_sdv_game_date")
    )


def _filter_before_target(
    pitches: pl.DataFrame,
    target_game_date: date,
) -> pl.DataFrame:
    """Keep only pitches strictly before the target game date."""
    if pitches is None or pitches.height == 0:
        return pl.DataFrame() if pitches is None else pitches

    parsed = _with_parsed_game_date(pitches)
    invalid_dates = parsed.filter(pl.col("_sdv_game_date").is_null()).height
    if invalid_dates:
        raise ValueError(
            f"Statcast data contains {invalid_dates} rows with invalid game_date"
        )

    return (
        parsed
        .filter(pl.col("_sdv_game_date") < pl.lit(target_game_date))
        .drop("_sdv_game_date")
    )


def _filter_recent_window(
    pitches: pl.DataFrame,
    target_game_date: date,
    lookback_days: int,
) -> pl.DataFrame:
    """Keep target-lookback <= pitch date < target."""
    if pitches is None or pitches.height == 0:
        return pl.DataFrame() if pitches is None else pitches

    recent_start = target_game_date - timedelta(days=lookback_days)
    parsed = _with_parsed_game_date(pitches)

    invalid_dates = parsed.filter(pl.col("_sdv_game_date").is_null()).height
    if invalid_dates:
        raise ValueError(
            f"Statcast data contains {invalid_dates} rows with invalid game_date"
        )

    return (
        parsed
        .filter(
            (pl.col("_sdv_game_date") >= pl.lit(recent_start))
            & (pl.col("_sdv_game_date") < pl.lit(target_game_date))
        )
        .drop("_sdv_game_date")
    )


def _filter_pitchers(
    pitches: pl.DataFrame,
    pitcher_ids: list[int],
) -> pl.DataFrame:
    if pitches is None or pitches.height == 0:
        return pl.DataFrame() if pitches is None else pitches
    if "pitcher" not in pitches.columns:
        raise ValueError("Statcast data missing required pitcher column")

    return (
        pitches
        .with_columns(pl.col("pitcher").cast(pl.Int64, strict=False))
        .filter(pl.col("pitcher").is_in(pitcher_ids))
    )


def _base_pitcher_stats(
    pitches: pl.DataFrame,
    suffix: str = "",
) -> pl.DataFrame:
    if pitches is None or pitches.height == 0 or "pitcher" not in pitches.columns:
        return pl.DataFrame()

    aggs: list[pl.Expr] = [
        pl.len().alias(f"sp_pitches{suffix}")
    ]

    if "game_pk" in pitches.columns:
        aggs.append(
            pl.col("game_pk").n_unique().alias(f"sp_games{suffix}")
        )

    if "pitch_type" in pitches.columns and not suffix:
        aggs.append(
            pl.col("pitch_type")
            .drop_nulls()
            .n_unique()
            .alias("sp_pitch_types")
        )

    if "release_speed" in pitches.columns:
        aggs.append(
            pl.col("release_speed")
            .mean()
            .alias(f"sp_avg_velo{suffix}")
        )

    if "release_spin_rate" in pitches.columns:
        aggs.append(
            pl.col("release_spin_rate")
            .mean()
            .alias(f"sp_avg_spin{suffix}")
        )

    if "game_date" in pitches.columns and not suffix:
        aggs.append(
            pl.col("game_date")
            .cast(pl.Utf8)
            .max()
            .alias("sp_last_game_date")
        )

    return pitches.group_by("pitcher").agg(aggs)


def _safe_model(label: str, callback) -> pl.DataFrame:
    try:
        result = callback()
        if result is None:
            return pl.DataFrame()
        return _as_polars(result)
    except Exception as exc:
        _log(f"{label} failed: {exc}", "WARN")
        return pl.DataFrame()


def _merge_polars_feature(
    base: pd.DataFrame,
    frame: pl.DataFrame,
    rename: dict[str, str] | None = None,
) -> pd.DataFrame:
    if frame is None or frame.height == 0 or "pitcher" not in frame.columns:
        return base

    pdf = frame.to_pandas()
    pdf["pitcher_id"] = pd.to_numeric(
        pdf["pitcher"],
        errors="coerce",
    ).astype("Int64")
    pdf = pdf.drop(columns=["pitcher"])

    if rename:
        pdf = pdf.rename(columns=rename)

    return base.merge(pdf, on="pitcher_id", how="left")


def build_pitcher_features(
    raw_pitches: pl.DataFrame,
    pitcher_ids: list[int],
    season: int,
    target_game_date: date,
    lookback_days: int,
) -> pd.DataFrame:
    """Build all pitcher features from strictly pregame pitch rows only."""
    features = pd.DataFrame(
        {"pitcher_id": pd.Series(pitcher_ids, dtype="Int64")}
    )

    if raw_pitches is None or raw_pitches.height == 0:
        return _empty_pitcher_features(pitcher_ids)

    # Fatal leakage guard: this filter happens before every aggregation/model.
    pregame = _filter_before_target(raw_pitches, target_game_date)
    pregame = _filter_pitchers(pregame, pitcher_ids)

    if pregame.height == 0:
        return _empty_pitcher_features(pitcher_ids)

    season_base = _base_pitcher_stats(pregame)
    features = _merge_polars_feature(features, season_base)

    feats = _safe_model(
        "pitch_features",
        lambda: pitch_features(pregame),
    )

    stuff_pitch = _safe_model(
        "mlb_stuff_plus",
        lambda: mlb_stuff_plus(feats, level="pitch"),
    )
    if stuff_pitch.height:
        stuff = stuff_pitch.group_by("pitcher").agg(
            pl.col("stuff_plus").mean().alias("sp_stuff_plus"),
            pl.len().alias("sp_stuff_scored_pitches"),
        )
        features = _merge_polars_feature(features, stuff)

    command_pitch = _safe_model(
        "mlb_command_plus",
        lambda: mlb_command_plus(feats, level="pitch"),
    )
    if command_pitch.height:
        command = command_pitch.group_by("pitcher").agg(
            pl.col("command_plus").mean().alias("sp_command_plus"),
            pl.len().alias("sp_command_scored_pitches"),
        )
        features = _merge_polars_feature(features, command)

    xera = _safe_model(
        "x_era",
        lambda: x_era(pregame, season),
    )
    if xera.height:
        features = _merge_polars_feature(
            features,
            xera.select("pitcher", "x_woba", "x_era"),
            rename={
                "x_woba": "sp_xwoba",
                "x_era": "sp_xera",
            },
        )

    # Recent rows are also strictly bounded above by target_game_date.
    recent = _filter_recent_window(
        pregame,
        target_game_date,
        lookback_days,
    )

    recent_base = _base_pitcher_stats(
        recent,
        suffix="_30d",
    )
    features = _merge_polars_feature(
        features,
        recent_base,
    )

    recent_xera = _safe_model(
        "x_era_30d",
        lambda: x_era(recent, season),
    )
    if recent_xera.height:
        features = _merge_polars_feature(
            features,
            recent_xera.select(
                "pitcher",
                "x_woba",
                "x_era",
            ),
            rename={
                "x_woba": "sp_xwoba_30d",
                "x_era": "sp_xera_30d",
            },
        )

    if (
        "sp_avg_velo" in features.columns
        and "sp_avg_velo_30d" in features.columns
    ):
        features["sp_velo_delta_30d"] = (
            pd.to_numeric(
                features["sp_avg_velo_30d"],
                errors="coerce",
            )
            - pd.to_numeric(
                features["sp_avg_velo"],
                errors="coerce",
            )
        )

    for col in PITCHER_FEATURE_COLUMNS:
        if col not in features.columns:
            features[col] = pd.NA

    return features[PITCHER_FEATURE_COLUMNS]


# =========================
# RAW STATCAST SEASON CACHE
# =========================

def _cache_path(season: int) -> Path:
    return CACHE_DIR / f"{season}_pitcher_statcast.parquet"


def _read_cache(season: int) -> pl.DataFrame:
    path = _cache_path(season)

    if not path.exists():
        return pl.DataFrame()

    try:
        cached = pl.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(
            f"Could not read Statcast cache {path}: {exc}"
        ) from exc

    if cached.height and "game_date" not in cached.columns:
        raise ValueError(
            f"Statcast cache {path} is missing game_date"
        )

    if cached.height and "pitcher" not in cached.columns:
        raise ValueError(
            f"Statcast cache {path} is missing pitcher"
        )

    return cached


def _dedupe_raw_statcast(frame: pl.DataFrame) -> pl.DataFrame:
    if frame is None or frame.height == 0:
        return pl.DataFrame() if frame is None else frame

    preferred_keys = [
        "game_pk",
        "at_bat_number",
        "pitch_number",
        "pitcher",
    ]
    keys = [col for col in preferred_keys if col in frame.columns]

    if len(keys) >= 3:
        return frame.unique(
            subset=keys,
            keep="last",
            maintain_order=True,
        )

    return frame.unique(
        keep="last",
        maintain_order=True,
    )


def _write_cache_atomic(
    season: int,
    frame: pl.DataFrame,
) -> None:
    path = _cache_path(season)
    tmp = path.with_suffix(".tmp.parquet")

    frame.write_parquet(tmp)
    tmp.replace(path)


def _pitcher_cache_max_dates(
    cache: pl.DataFrame,
) -> dict[int, date]:
    if cache is None or cache.height == 0:
        return {}

    parsed = _with_parsed_game_date(cache)
    parsed = parsed.filter(
        pl.col("pitcher").is_not_null()
        & pl.col("_sdv_game_date").is_not_null()
    )

    if parsed.height == 0:
        return {}

    maxima = (
        parsed
        .with_columns(
            pl.col("pitcher").cast(pl.Int64, strict=False)
        )
        .group_by("pitcher")
        .agg(
            pl.col("_sdv_game_date").max().alias("max_game_date")
        )
    )

    out: dict[int, date] = {}
    for row in maxima.iter_rows(named=True):
        pitcher = row.get("pitcher")
        max_date = row.get("max_game_date")
        if pitcher is not None and max_date is not None:
            out[int(pitcher)] = max_date

    return out


def _fetch_statcast(
    season: int,
    start_date: date,
    end_date: date,
    pitcher_ids: list[int],
) -> pl.DataFrame:
    if not pitcher_ids or start_date > end_date:
        return pl.DataFrame()

    _log(
        f"Statcast fetch season={season} "
        f"start={start_date.isoformat()} "
        f"end={end_date.isoformat()} "
        f"pitchers={len(pitcher_ids)}"
    )

    raw = mlb_statcast_search(
        start_date.isoformat(),
        end_date.isoformat(),
        player_type="pitcher",
        game_type="R",
        pitchers_lookup=pitcher_ids,
    )
    return _as_polars(raw)


def ensure_season_cache(
    season: int,
    pitcher_ids: list[int],
    required_through_date: date,
    summary: dict,
) -> pl.DataFrame:
    """Ensure the season cache covers requested pitchers through the cutoff.

    The cache is allowed to contain rows after a later historical target. Those
    rows are harmless because ``build_pitcher_features`` always applies the
    strict ``pitch.game_date < target_game_date`` filter before aggregation.
    """
    cache = _read_cache(season)
    season_start = date(season, 3, 1)

    if required_through_date < season_start or not pitcher_ids:
        return cache

    max_dates = _pitcher_cache_max_dates(cache)

    missing_ids = [
        pitcher_id
        for pitcher_id in pitcher_ids
        if pitcher_id not in max_dates
    ]

    stale_ids = [
        pitcher_id
        for pitcher_id in pitcher_ids
        if (
            pitcher_id in max_dates
            and max_dates[pitcher_id] < required_through_date
        )
    ]

    fetched_frames: list[pl.DataFrame] = []

    # Pitchers absent from cache need their full season-to-cutoff history.
    if missing_ids:
        fetched = _fetch_statcast(
            season,
            season_start,
            required_through_date,
            missing_ids,
        )
        if fetched.height:
            fetched_frames.append(fetched)

    # Existing pitchers are incrementally refreshed from the earliest missing
    # day among the stale requested pitchers. Duplicate rows are removed later.
    if stale_ids:
        earliest_refresh = min(
            max_dates[pitcher_id] + timedelta(days=1)
            for pitcher_id in stale_ids
        )
        fetched = _fetch_statcast(
            season,
            earliest_refresh,
            required_through_date,
            stale_ids,
        )
        if fetched.height:
            fetched_frames.append(fetched)

    if fetched_frames:
        pieces = [cache] if cache.height else []
        pieces.extend(fetched_frames)

        cache = pl.concat(
            pieces,
            how="diagonal_relaxed",
        )
        cache = _dedupe_raw_statcast(cache)
        _write_cache_atomic(season, cache)

        fetched_rows = sum(frame.height for frame in fetched_frames)
        summary["statcast_pitches_fetched"] += fetched_rows
        summary["cache_writes"] += 1

        _log(
            f"CACHE WROTE {_cache_path(season)} "
            f"rows={cache.height} fetched_rows={fetched_rows}"
        )
    elif cache.height:
        _log(
            f"CACHE HIT {_cache_path(season)} rows={cache.height}"
        )

    return cache


# =========================
# OUTPUT BUILD / VALIDATION
# =========================

def attach_side_features(
    games: pd.DataFrame,
    pitcher_features: pd.DataFrame,
    side: str,
) -> pd.DataFrame:
    key = f"{side}_pitcher_id"

    if key not in games.columns:
        games[key] = ""

    side_features = pitcher_features.copy()
    side_features["_pitcher_key"] = (
        side_features["pitcher_id"]
        .astype("Int64")
        .astype("string")
    )
    side_features = side_features.drop(columns=["pitcher_id"])

    rename = {
        col: f"sdv_{side}_{col}"
        for col in side_features.columns
        if col != "_pitcher_key"
    }
    side_features = side_features.rename(columns=rename)

    games[key] = games[key].astype("string").str.strip()
    games = (
        games
        .merge(
            side_features,
            left_on=key,
            right_on="_pitcher_key",
            how="left",
        )
        .drop(columns=["_pitcher_key"])
    )

    pitches_col = f"sdv_{side}_sp_pitches"
    games[f"sdv_{side}_sp_found"] = (
        games[pitches_col].notna().astype(int)
    )

    return games


def validate_output_pregame_cutoff(
    output: pd.DataFrame,
    label: str,
) -> None:
    if "sdv_as_of_date" not in output.columns:
        raise ValueError(f"{label} missing sdv_as_of_date")
    if "game_date" not in output.columns:
        raise ValueError(f"{label} missing game_date")

    as_of = pd.to_datetime(
        output["sdv_as_of_date"]
        .astype("string")
        .str.replace("_", "-", regex=False),
        errors="coerce",
    )
    game_date = pd.to_datetime(
        output["game_date"]
        .astype("string")
        .str.replace("_", "-", regex=False),
        errors="coerce",
    )

    bad = (
        as_of.isna()
        | game_date.isna()
        | (as_of >= game_date)
    )

    if bad.any():
        sample = (
            output.loc[
                bad,
                ["game_id", "game_date", "sdv_as_of_date"]
                if "game_id" in output.columns
                else ["game_date", "sdv_as_of_date"],
            ]
            .head(10)
            .to_dict("records")
        )
        raise ValueError(
            f"{label} violates leakage cutoff sdv_as_of_date < game_date; "
            f"bad_rows={int(bad.sum())}; sample={sample}"
        )


def write_output_checked(
    output: pd.DataFrame,
    out_path: Path,
) -> None:
    validate_output_pregame_cutoff(
        output,
        str(out_path),
    )
    output.to_csv(out_path, index=False)


def write_base_output(
    games: pd.DataFrame,
    out_path: Path,
    game_date: date,
    lookback_days: int,
    status: str,
) -> None:
    out = games.copy()
    out["sdv_as_of_date"] = (
        game_date - timedelta(days=1)
    ).isoformat()
    out["sdv_season"] = game_date.year
    out["sdv_lookback_days"] = lookback_days
    out["sdv_status"] = status

    for side in ("home", "away"):
        for feature in PITCHER_FEATURE_COLUMNS[1:]:
            out[f"sdv_{side}_{feature}"] = pd.NA
        out[f"sdv_{side}_sp_found"] = 0

    write_output_checked(out, out_path)


def process_date(
    date_str: str,
    lookback_days: int,
    summary: dict,
) -> None:
    games_path = GAMES_DIR / f"{date_str}_games.csv"
    out_path = OUT_DIR / f"{date_str}_sportsdataverse.csv"

    if not games_path.exists():
        _log(
            f"MISSING games file: {games_path}",
            "ERROR",
        )
        summary["errors"] += 1
        return

    games = pd.read_csv(
        games_path,
        dtype=str,
        encoding="utf-8-sig",
    )

    if games.empty:
        _log(
            f"{date_str} | games file is empty",
            "ERROR",
        )
        summary["errors"] += 1
        return

    game_date = _game_date_for_file(date_str, games)
    statcast_end = game_date - timedelta(days=1)
    season = game_date.year
    season_start = date(season, 3, 1)

    pitcher_ids = _safe_ints(
        list(
            games.get(
                "home_pitcher_id",
                pd.Series(dtype=str),
            )
        )
        + list(
            games.get(
                "away_pitcher_id",
                pd.Series(dtype=str),
            )
        )
    )

    _log(
        f"{date_str} | games={len(games)} "
        f"pitchers={len(pitcher_ids)} "
        f"as_of={statcast_end.isoformat()}"
    )

    if not pitcher_ids:
        write_base_output(
            games,
            out_path,
            game_date,
            lookback_days,
            "no_probable_pitchers",
        )
        summary["files_written"] += 1
        summary["rows_written"] += len(games)
        return

    if statcast_end < season_start:
        write_base_output(
            games,
            out_path,
            game_date,
            lookback_days,
            "no_prior_regular_season_data",
        )
        summary["files_written"] += 1
        summary["rows_written"] += len(games)
        return

    try:
        season_cache = ensure_season_cache(
            season,
            pitcher_ids,
            statcast_end,
            summary,
        )
    except Exception as exc:
        _log(
            f"{date_str} | Statcast cache/fetch failed: {exc}",
            "ERROR",
        )
        write_base_output(
            games,
            out_path,
            game_date,
            lookback_days,
            "statcast_pull_error",
        )
        summary["files_written"] += 1
        summary["rows_written"] += len(games)
        summary["errors"] += 1
        return

    if season_cache is None or season_cache.height == 0:
        write_base_output(
            games,
            out_path,
            game_date,
            lookback_days,
            "no_statcast_rows",
        )
        _log(
            f"{date_str} | Statcast cache has zero rows",
            "WARN",
        )
        summary["files_written"] += 1
        summary["rows_written"] += len(games)
        return

    # The cache may contain later dates. Filter strictly before feature work.
    pregame_cache = _filter_before_target(
        season_cache,
        game_date,
    )
    pregame_cache = _filter_pitchers(
        pregame_cache,
        pitcher_ids,
    )

    if pregame_cache.height == 0:
        write_base_output(
            games,
            out_path,
            game_date,
            lookback_days,
            "no_statcast_rows",
        )
        _log(
            f"{date_str} | zero pregame Statcast rows after cutoff",
            "WARN",
        )
        summary["files_written"] += 1
        summary["rows_written"] += len(games)
        return

    _log(
        f"{date_str} | pregame Statcast pitches={pregame_cache.height}"
    )

    pitcher_features = build_pitcher_features(
        pregame_cache,
        pitcher_ids,
        season,
        game_date,
        lookback_days,
    )

    output = games.copy()
    output = attach_side_features(
        output,
        pitcher_features,
        "home",
    )
    output = attach_side_features(
        output,
        pitcher_features,
        "away",
    )

    output["sdv_as_of_date"] = statcast_end.isoformat()
    output["sdv_season"] = season
    output["sdv_lookback_days"] = lookback_days
    output["sdv_status"] = "ok"

    write_output_checked(
        output,
        out_path,
    )

    found_home = int(
        output["sdv_home_sp_found"].sum()
    )
    found_away = int(
        output["sdv_away_sp_found"].sum()
    )

    _log(
        f"{date_str} | WROTE {out_path} "
        f"rows={len(output)} "
        f"home_sp_found={found_home}/{len(output)} "
        f"away_sp_found={found_away}/{len(output)}"
    )

    summary["files_written"] += 1
    summary["rows_written"] += len(output)
    summary["pregame_statcast_pitches"] += pregame_cache.height


# =========================
# CLI / DATE RESOLUTION
# =========================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "dates",
        nargs="*",
        help=(
            "Optional date(s) to process (YYYY_MM_DD or YYYY-MM-DD). "
            "If omitted and no range is supplied, processes the latest "
            "*_games.csv file."
        ),
    )

    parser.add_argument(
        "--from-date",
        dest="from_date",
        help="Inclusive historical range start (YYYY-MM-DD).",
    )

    parser.add_argument(
        "--to-date",
        dest="to_date",
        help="Inclusive historical range end (YYYY-MM-DD).",
    )

    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Recent-form window in calendar days (default: 30).",
    )

    return parser.parse_args()


def _existing_game_dates_in_range(
    from_date: date,
    to_date: date,
) -> list[str]:
    if from_date > to_date:
        raise ValueError(
            "--from-date must be <= --to-date"
        )

    dates: list[str] = []
    current = from_date

    while current <= to_date:
        date_str = current.strftime("%Y_%m_%d")
        games_path = GAMES_DIR / f"{date_str}_games.csv"

        if games_path.exists():
            dates.append(date_str)
        else:
            _log(
                f"RANGE SKIP no games file: {games_path}"
            )

        current += timedelta(days=1)

    return dates


def resolve_dates(args: argparse.Namespace) -> list[str]:
    dates: list[str] = []

    # Existing positional-date behavior remains supported.
    if args.dates:
        dates.extend(
            normalize_date(value)
            for value in args.dates
        )

    range_requested = (
        args.from_date is not None
        or args.to_date is not None
    )

    if range_requested:
        if not args.from_date or not args.to_date:
            raise ValueError(
                "--from-date and --to-date must be provided together"
            )

        from_date = parse_date(args.from_date)
        to_date = parse_date(args.to_date)

        dates.extend(
            _existing_game_dates_in_range(
                from_date,
                to_date,
            )
        )

    if not dates:
        if range_requested:
            return []

        game_files = sorted(
            GAMES_DIR.glob("*_games.csv")
        )
        if not game_files:
            raise FileNotFoundError(
                f"No *_games.csv files found in {GAMES_DIR}"
            )

        dates = [
            game_files[-1]
            .stem
            .replace("_games", "")
        ]

    # De-duplicate while preserving requested order.
    return list(dict.fromkeys(dates))


def main() -> None:
    args = parse_args()

    with LOG_FILE.open("w", encoding="utf-8") as f:
        f.write(
            f"=== sportsdataverse_mlb RUN {_now()} ===\n"
        )

    summary = {
        "files_written": 0,
        "rows_written": 0,
        "pregame_statcast_pitches": 0,
        "statcast_pitches_fetched": 0,
        "cache_writes": 0,
        "errors": 0,
    }

    if args.lookback_days < 1:
        _log(
            "--lookback-days must be >= 1",
            "ERROR",
        )
        sys.exit(2)

    try:
        dates = resolve_dates(args)
    except Exception as exc:
        _log(
            f"DATE RESOLUTION FAILED: {exc}",
            "ERROR",
        )
        print(
            f"sportsdataverse_mlb date resolution failed: {exc}"
        )
        sys.exit(2)

    _log(
        f"dates={dates} "
        f"lookback_days={args.lookback_days} "
        f"cache_dir={CACHE_DIR}"
    )

    if not dates:
        _log(
            "No existing games files found in requested range; nothing to process."
        )
        print(
            "sportsdataverse_mlb complete. "
            "No existing games files found in requested range."
        )
        return

    for date_str in dates:
        try:
            process_date(
                date_str,
                args.lookback_days,
                summary,
            )
        except Exception as exc:
            _log(
                f"{date_str} FAILED: {exc}\n"
                f"{traceback.format_exc()}",
                "ERROR",
            )
            summary["errors"] += 1

    status = (
        "SUCCESS"
        if summary["errors"] == 0
        else "COMPLETED WITH ERRORS"
    )

    _log(
        f"SUMMARY "
        f"files_written={summary['files_written']} "
        f"rows_written={summary['rows_written']} "
        f"pregame_statcast_pitches="
        f"{summary['pregame_statcast_pitches']} "
        f"statcast_pitches_fetched="
        f"{summary['statcast_pitches_fetched']} "
        f"cache_writes={summary['cache_writes']} "
        f"errors={summary['errors']} "
        f"status={status}"
    )

    print(
        "sportsdataverse_mlb complete. "
        f"{summary['files_written']} files written, "
        f"{summary['rows_written']} rows. "
        f"Status: {status}"
    )

    if summary["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
