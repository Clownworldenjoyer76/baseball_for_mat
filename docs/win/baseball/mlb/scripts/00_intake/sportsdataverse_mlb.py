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
import time
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

BULLPEN_FEATURE_COLUMNS = [
    "bp_pa_14d",
    "bp_woba_allowed_14d",
    "bp_k_rate_14d",
    "bp_bb_rate_14d",
    "bp_hard_rate_14d",
    "bp_pitches_3d",
    "bp_pa_7d",
    "bp_woba_allowed_7d",
    "bp_k_rate_7d",
    "bp_bb_rate_7d",
    "bp_hard_rate_7d",
]

TEAM_ALIASES = {
    "ARI": "ARI", "AZ": "ARI", "ARIZONA DIAMONDBACKS": "ARI",
    "ATL": "ATL", "ATLANTA BRAVES": "ATL",
    "ATH": "ATH", "OAK": "ATH", "ATHLETICS": "ATH",
    "OAKLAND ATHLETICS": "ATH", "SACRAMENTO ATHLETICS": "ATH",
    "BAL": "BAL", "BALTIMORE ORIOLES": "BAL",
    "BOS": "BOS", "BOSTON RED SOX": "BOS",
    "CHC": "CHC", "CHICAGO CUBS": "CHC",
    "CWS": "CWS", "CHW": "CWS", "CHICAGO WHITE SOX": "CWS",
    "CIN": "CIN", "CINCINNATI REDS": "CIN",
    "CLE": "CLE", "CLEVELAND GUARDIANS": "CLE",
    "COL": "COL", "COLORADO ROCKIES": "COL",
    "DET": "DET", "DETROIT TIGERS": "DET",
    "HOU": "HOU", "HOUSTON ASTROS": "HOU",
    "KC": "KC", "KCR": "KC", "KANSAS CITY ROYALS": "KC",
    "LAA": "LAA", "LOS ANGELES ANGELS": "LAA",
    "LAD": "LAD", "LOS ANGELES DODGERS": "LAD",
    "MIA": "MIA", "MIAMI MARLINS": "MIA",
    "MIL": "MIL", "MILWAUKEE BREWERS": "MIL",
    "MIN": "MIN", "MINNESOTA TWINS": "MIN",
    "NYM": "NYM", "NEW YORK METS": "NYM",
    "NYY": "NYY", "NEW YORK YANKEES": "NYY",
    "PHI": "PHI", "PHILADELPHIA PHILLIES": "PHI",
    "PIT": "PIT", "PITTSBURGH PIRATES": "PIT",
    "SD": "SD", "SDP": "SD", "SAN DIEGO PADRES": "SD",
    "SF": "SF", "SFG": "SF", "SAN FRANCISCO GIANTS": "SF",
    "SEA": "SEA", "SEATTLE MARINERS": "SEA",
    "STL": "STL", "ST. LOUIS CARDINALS": "STL", "ST LOUIS CARDINALS": "STL",
    "TB": "TB", "TBR": "TB", "TAMPA BAY RAYS": "TB",
    "TEX": "TEX", "TEXAS RANGERS": "TEX",
    "TOR": "TOR", "TORONTO BLUE JAYS": "TOR",
    "WSH": "WSH", "WSN": "WSH", "WASHINGTON NATIONALS": "WSH",
}

STRIKEOUT_EVENTS = {
    "strikeout",
    "strikeout_double_play",
}

WALK_EVENTS = {
    "walk",
    "intent_walk",
}

TEAM_STATCAST_KEEP_COLUMNS = [
    "game_date",
    "game_pk",
    "home_team",
    "away_team",
    "inning_topbot",
    "inning",
    "at_bat_number",
    "pitch_number",
    "pitcher",
    "events",
    "woba_value",
    "launch_speed",
]

TEAM_STATCAST_REQUIRED_COLUMNS = [
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

TEAM_STATCAST_CHUNK_DAYS = 4
TEAM_STATCAST_FETCH_RETRIES = 3


def canonical_team(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    return TEAM_ALIASES.get(text)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator is None or denominator <= 0:
        return float("nan")
    if pd.isna(denominator) or pd.isna(numerator):
        return float("nan")
    return float(numerator / denominator)


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
# LEAGUE STATCAST CACHE FOR BULLPEN FEATURES
# =========================

def _team_cache_path(season: int) -> Path:
    return CACHE_DIR / f"{season}_team_statcast.parquet"


def _read_team_cache(season: int) -> pl.DataFrame:
    path = _team_cache_path(season)
    if not path.exists():
        return pl.DataFrame()

    try:
        frame = pl.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(
            f"Could not read bullpen Statcast cache {path}: {exc}"
        ) from exc

    missing = [
        col
        for col in TEAM_STATCAST_REQUIRED_COLUMNS
        if col not in frame.columns
    ]
    if frame.height and missing:
        raise ValueError(
            f"Bullpen Statcast cache {path} missing columns: {missing}"
        )

    return frame


def _write_team_cache_atomic(
    season: int,
    frame: pl.DataFrame,
) -> None:
    path = _team_cache_path(season)
    tmp = path.with_suffix(".tmp.parquet")
    frame.write_parquet(tmp)
    tmp.replace(path)


def _team_cache_date_bounds(
    cache: pl.DataFrame,
) -> tuple[date | None, date | None]:
    if cache is None or cache.height == 0:
        return None, None

    parsed = _with_parsed_game_date(cache)
    values = (
        parsed
        .select(
            pl.col("_sdv_game_date").min().alias("min_date"),
            pl.col("_sdv_game_date").max().alias("max_date"),
        )
        .to_dicts()
    )
    if not values:
        return None, None
    return values[0].get("min_date"), values[0].get("max_date")


def _fetch_team_statcast_chunk(
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    last_error = None

    for attempt in range(1, TEAM_STATCAST_FETCH_RETRIES + 1):
        try:
            _log(
                "Bullpen Statcast fetch "
                f"start={start_date.isoformat()} "
                f"end={end_date.isoformat()}"
            )

            raw = mlb_statcast_search(
                start_date.isoformat(),
                end_date.isoformat(),
                player_type="batter",
                game_type="R",
            )
            frame = _as_polars(raw)

            if frame.height == 0:
                return pl.DataFrame()

            missing = [
                col
                for col in TEAM_STATCAST_REQUIRED_COLUMNS
                if col not in frame.columns
            ]
            if missing:
                raise RuntimeError(
                    "Bullpen Statcast response missing required columns: "
                    f"{missing}"
                )

            keep = [
                col
                for col in TEAM_STATCAST_KEEP_COLUMNS
                if col in frame.columns
            ]
            return frame.select(keep)

        except Exception as exc:
            last_error = exc
            if attempt < TEAM_STATCAST_FETCH_RETRIES:
                time.sleep(3 * attempt)

    raise RuntimeError(
        "Bullpen Statcast download failed for "
        f"{start_date.isoformat()}..{end_date.isoformat()}: {last_error}"
    )


def _fetch_team_statcast_range(
    start_date: date,
    end_date: date,
) -> list[pl.DataFrame]:
    if start_date > end_date:
        return []

    frames: list[pl.DataFrame] = []
    current = start_date

    while current <= end_date:
        chunk_end = min(
            current + timedelta(days=TEAM_STATCAST_CHUNK_DAYS - 1),
            end_date,
        )
        fetched = _fetch_team_statcast_chunk(current, chunk_end)
        if fetched.height:
            frames.append(fetched)
        current = chunk_end + timedelta(days=1)

    return frames


def ensure_team_season_cache(
    season: int,
    required_from_date: date,
    required_through_date: date,
    summary: dict,
) -> pl.DataFrame:
    """Ensure the bullpen cache covers only the dates the feature needs.

    Bullpen features use a maximum 14-day lookback. A normal daily run therefore
    downloads at most that recent window when no cache is available, instead of
    downloading the entire season. Historical model-training backfills request
    the full union of the needed 14-day windows once, then reuse it for every
    target date.
    """
    cache = _read_team_cache(season)
    season_start = date(season, 3, 1)

    required_from_date = max(required_from_date, season_start)
    if required_through_date < required_from_date:
        return cache

    min_cached, max_cached = _team_cache_date_bounds(cache)
    fetched_frames: list[pl.DataFrame] = []

    if min_cached is None or max_cached is None:
        fetched_frames.extend(
            _fetch_team_statcast_range(
                required_from_date,
                required_through_date,
            )
        )
    else:
        if required_from_date < min_cached:
            fetched_frames.extend(
                _fetch_team_statcast_range(
                    required_from_date,
                    min_cached - timedelta(days=1),
                )
            )

        if required_through_date > max_cached:
            fetched_frames.extend(
                _fetch_team_statcast_range(
                    max_cached + timedelta(days=1),
                    required_through_date,
                )
            )

    if fetched_frames:
        pieces = [cache] if cache.height else []
        pieces.extend(fetched_frames)
        cache = pl.concat(pieces, how="diagonal_relaxed")
        cache = _dedupe_raw_statcast(cache)
        _write_team_cache_atomic(season, cache)

        fetched_rows = sum(frame.height for frame in fetched_frames)
        summary["team_statcast_pitches_fetched"] += fetched_rows
        summary["team_cache_writes"] += 1
        _log(
            f"BULLPEN CACHE WROTE {_team_cache_path(season)} "
            f"rows={cache.height} fetched_rows={fetched_rows}"
        )
    elif cache.height:
        _log(
            f"BULLPEN CACHE HIT {_team_cache_path(season)} rows={cache.height}"
        )

    return cache

def _prepare_bullpen_statcast(
    team_cache: pl.DataFrame,
    target_game_date: date,
) -> pd.DataFrame:
    if team_cache is None or team_cache.height == 0:
        return pd.DataFrame()

    # Only the longest tested performance window is needed. The helper also
    # enforces the strict upper bound pitch.game_date < target_game_date.
    recent = _filter_recent_window(
        team_cache,
        target_game_date,
        14,
    )
    if recent.height == 0:
        return pd.DataFrame()

    df = recent.to_pandas()
    df["_game_date_dt"] = pd.to_datetime(
        df["game_date"],
        errors="coerce",
    ).dt.normalize()
    df["_gamePk"] = pd.to_numeric(
        df["game_pk"],
        errors="coerce",
    ).astype("Int64")
    df["_home_code"] = df["home_team"].map(canonical_team)
    df["_away_code"] = df["away_team"].map(canonical_team)

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

    invalid_half = ~(top | bottom)
    if invalid_half.any():
        bad_values = sorted(
            df.loc[invalid_half, "inning_topbot"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        raise RuntimeError(
            f"Unexpected Statcast inning_topbot values: {bad_values}"
        )

    df["_pitching_team"] = df["_home_code"].where(top, df["_away_code"])
    df["_pitcher"] = pd.to_numeric(df["pitcher"], errors="coerce").astype("Int64")
    df["_inning"] = (
        pd.to_numeric(df["inning"], errors="coerce")
        if "inning" in df.columns
        else 0
    )
    df["_at_bat_number"] = pd.to_numeric(df["at_bat_number"], errors="coerce")
    df["_pitch_number"] = pd.to_numeric(df["pitch_number"], errors="coerce")
    df["_events"] = (
        df["events"]
        .astype("string")
        .str.strip()
        .str.lower()
    )
    df["_woba_value"] = pd.to_numeric(df["woba_value"], errors="coerce")
    df["_launch_speed"] = pd.to_numeric(df["launch_speed"], errors="coerce")

    ordered = df.sort_values(
        [
            "_game_date_dt",
            "_gamePk",
            "_pitching_team",
            "_inning",
            "_at_bat_number",
            "_pitch_number",
        ]
    ).copy()

    starters = (
        ordered[ordered["_pitcher"].notna()]
        .groupby(
            ["_gamePk", "_pitching_team"],
            as_index=False,
            sort=False,
        )
        .first()[["_gamePk", "_pitching_team", "_pitcher"]]
        .rename(columns={"_pitcher": "_starter_pitcher"})
    )

    df = df.merge(
        starters,
        on=["_gamePk", "_pitching_team"],
        how="left",
        validate="many_to_one",
    )
    df["_is_bullpen"] = (
        df["_pitcher"].notna()
        & df["_starter_pitcher"].notna()
        & (df["_pitcher"] != df["_starter_pitcher"])
    )
    df["_is_pa"] = df["_events"].notna() & (df["_events"] != "")
    df["_is_k"] = df["_events"].isin(STRIKEOUT_EVENTS)
    df["_is_bb"] = df["_events"].isin(WALK_EVENTS)
    df["_is_bbe"] = df["_launch_speed"].notna()
    df["_is_hard"] = df["_launch_speed"] >= 95.0

    return df[df["_is_bullpen"]].copy()


def _bullpen_performance(
    bullpen: pd.DataFrame,
    team: str | None,
    target_game_date: date,
    days: int,
) -> dict[str, float]:
    if bullpen.empty or team is None:
        return {
            "pa": 0.0,
            "woba": float("nan"),
            "k_rate": float("nan"),
            "bb_rate": float("nan"),
            "hard_rate": float("nan"),
        }

    target = pd.Timestamp(target_game_date)
    start = target - pd.Timedelta(days=days)
    part = bullpen[
        (bullpen["_pitching_team"] == team)
        & (bullpen["_game_date_dt"] >= start)
        & (bullpen["_game_date_dt"] < target)
        & bullpen["_is_pa"]
    ].copy()

    if part.empty:
        return {
            "pa": 0.0,
            "woba": float("nan"),
            "k_rate": float("nan"),
            "bb_rate": float("nan"),
            "hard_rate": float("nan"),
        }

    pa = float(len(part))
    woba_n = float(part["_woba_value"].notna().sum())
    bbe = float(part["_is_bbe"].sum())

    return {
        "pa": pa,
        "woba": _safe_ratio(float(part["_woba_value"].fillna(0.0).sum()), woba_n),
        "k_rate": _safe_ratio(float(part["_is_k"].sum()), pa),
        "bb_rate": _safe_ratio(float(part["_is_bb"].sum()), pa),
        "hard_rate": _safe_ratio(float(part["_is_hard"].sum()), bbe),
    }


def _bullpen_pitches_3d(
    bullpen: pd.DataFrame,
    team: str | None,
    target_game_date: date,
) -> float:
    if bullpen.empty or team is None:
        return 0.0

    target = pd.Timestamp(target_game_date)
    start = target - pd.Timedelta(days=3)
    part = bullpen[
        (bullpen["_pitching_team"] == team)
        & (bullpen["_game_date_dt"] >= start)
        & (bullpen["_game_date_dt"] < target)
    ]
    return float(len(part))


def attach_bullpen_features(
    games: pd.DataFrame,
    team_cache: pl.DataFrame | None,
    target_game_date: date,
) -> pd.DataFrame:
    output = games.copy()
    bullpen = _prepare_bullpen_statcast(
        team_cache if team_cache is not None else pl.DataFrame(),
        target_game_date,
    )

    for side in ("home", "away"):
        teams = output[f"{side}_team"].map(canonical_team)

        for index, team in teams.items():
            perf14 = _bullpen_performance(
                bullpen,
                team,
                target_game_date,
                14,
            )
            perf7 = _bullpen_performance(
                bullpen,
                team,
                target_game_date,
                7,
            )
            pitches3 = _bullpen_pitches_3d(
                bullpen,
                team,
                target_game_date,
            )

            output.loc[index, f"sdv_{side}_bp_pa_14d"] = perf14["pa"]
            output.loc[index, f"sdv_{side}_bp_woba_allowed_14d"] = perf14["woba"]
            output.loc[index, f"sdv_{side}_bp_k_rate_14d"] = perf14["k_rate"]
            output.loc[index, f"sdv_{side}_bp_bb_rate_14d"] = perf14["bb_rate"]
            output.loc[index, f"sdv_{side}_bp_hard_rate_14d"] = perf14["hard_rate"]
            output.loc[index, f"sdv_{side}_bp_pitches_3d"] = pitches3
            output.loc[index, f"sdv_{side}_bp_pa_7d"] = perf7["pa"]
            output.loc[index, f"sdv_{side}_bp_woba_allowed_7d"] = perf7["woba"]
            output.loc[index, f"sdv_{side}_bp_k_rate_7d"] = perf7["k_rate"]
            output.loc[index, f"sdv_{side}_bp_bb_rate_7d"] = perf7["bb_rate"]
            output.loc[index, f"sdv_{side}_bp_hard_rate_7d"] = perf7["hard_rate"]

    return output


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
    team_cache: pl.DataFrame | None = None,
) -> None:
    out = attach_bullpen_features(
        games,
        team_cache,
        game_date,
    )
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
    team_cache: pl.DataFrame | None = None,
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
            team_cache,
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
            team_cache,
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
            team_cache,
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
            team_cache,
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
            team_cache,
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

    output = attach_bullpen_features(
        games,
        team_cache,
        game_date,
    )
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
        "team_statcast_pitches_fetched": 0,
        "team_cache_writes": 0,
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

    team_caches: dict[int, pl.DataFrame] = {}
    season_required_ranges: dict[int, tuple[date, date]] = {}

    for date_str in dates:
        target_date = parse_date(date_str)
        required_from = target_date - timedelta(days=14)
        required_through = target_date - timedelta(days=1)
        existing = season_required_ranges.get(target_date.year)

        if existing is None:
            season_required_ranges[target_date.year] = (
                required_from,
                required_through,
            )
        else:
            season_required_ranges[target_date.year] = (
                min(existing[0], required_from),
                max(existing[1], required_through),
            )

    for season, required_range in season_required_ranges.items():
        try:
            team_caches[season] = ensure_team_season_cache(
                season,
                required_range[0],
                required_range[1],
                summary,
            )
        except Exception as exc:
            _log(
                f"Bullpen Statcast cache/fetch failed for season {season}: {exc}",
                "ERROR",
            )
            summary["errors"] += 1
            team_caches[season] = pl.DataFrame()

    for date_str in dates:
        try:
            target_date = parse_date(date_str)
            process_date(
                date_str,
                args.lookback_days,
                summary,
                team_caches.get(target_date.year),
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
        f"team_statcast_pitches_fetched="
        f"{summary['team_statcast_pitches_fetched']} "
        f"team_cache_writes={summary['team_cache_writes']} "
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
