#!/usr/bin/env python3
"""Final MLB moneyline feature experiment.

Tests:
    1. Bullpen window: 5-day vs 7-day vs 10-day
    2. Starting pitcher vs expected opposing lineup
       - throwing hand
       - pitch mix
    3. Team defense
       - actual contact results vs expected contact results
    4. Combinations of the above

The original Statcast experiment cache intentionally did not save:
    p_throws
    pitch_type
    estimated_woba_using_speedangle

This script downloads only those missing Statcast columns once and stores
them in a separate experiment cache.

No production models or pipeline files are changed.
"""

from __future__ import annotations

import html
import importlib.util
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# PATHS
# ============================================================


def repo_root() -> Path:
    for start in (
        Path.cwd().resolve(),
        Path(__file__).resolve().parent,
    ):
        for candidate in (
            start,
            *start.parents,
        ):
            if (
                candidate
                / "docs/win/baseball/mlb"
            ).exists():
                return candidate

    raise RuntimeError(
        "Could not locate repository root"
    )


ROOT = repo_root()

BASE = (
    ROOT
    / "docs/win/baseball/mlb"
)

SCRIPTS = (
    BASE
    / "scripts/modeling"
)

SDV_DIR = (
    BASE
    / "00_intake/sportsdataverse"
)

FIRST_SCRIPT = (
    SCRIPTS
    / "test_moneyline_feature_expansion.py"
)

BULLPEN_SCRIPT = (
    SCRIPTS
    / "test_moneyline_bullpen_detail.py"
)

LINEUP_SCRIPT = (
    SCRIPTS
    / "test_moneyline_expected_lineup.py"
)

REFERENCE_FILE = (
    ROOT
    / "mlb_moneyline_bullpen_detail_test_predictions.csv"
)

SUPPLEMENT_CACHE_DIR = (
    BASE
    / "modeling/experiments/moneyline_final_features"
    / "statcast_supplement_cache"
)

SUPPLEMENT_CACHE_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

SUMMARY_FILE = (
    ROOT
    / "mlb_moneyline_final_feature_test_summary.csv"
)

PREDICTIONS_FILE = (
    ROOT
    / "mlb_moneyline_final_feature_test_predictions.csv"
)

REPORT_FILE = (
    ROOT
    / "mlb_moneyline_final_feature_test_report.html"
)


# ============================================================
# LOAD EXISTING EXPERIMENT CODE
# ============================================================


def load_module(
    name: str,
    path: Path,
):
    if not path.exists():
        raise FileNotFoundError(path)

    spec = (
        importlib.util.spec_from_file_location(
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

    module = (
        importlib.util.module_from_spec(
            spec
        )
    )

    spec.loader.exec_module(
        module
    )

    return module


EXP = load_module(
    "final_moneyline_exp",
    FIRST_SCRIPT,
)

BP = load_module(
    "final_moneyline_bp",
    BULLPEN_SCRIPT,
)

LINEUP = load_module(
    "final_moneyline_lineup",
    LINEUP_SCRIPT,
)

TRAIN = EXP.TRAIN
JUICE = EXP.JUICE

TRAIN._log = (
    lambda *args, **kwargs: None
)


# ============================================================
# FEATURES
# ============================================================


BP5 = [
    "home_bp_pa_5d",
    "away_bp_pa_5d",

    "home_bp_woba_allowed_5d",
    "away_bp_woba_allowed_5d",

    "home_bp_k_rate_5d",
    "away_bp_k_rate_5d",

    "home_bp_bb_rate_5d",
    "away_bp_bb_rate_5d",

    "home_bp_hard_rate_5d",
    "away_bp_hard_rate_5d",
]


BP10 = [
    "home_bp_pa_10d",
    "away_bp_pa_10d",

    "home_bp_woba_allowed_10d",
    "away_bp_woba_allowed_10d",

    "home_bp_k_rate_10d",
    "away_bp_k_rate_10d",

    "home_bp_bb_rate_10d",
    "away_bp_bb_rate_10d",

    "home_bp_hard_rate_10d",
    "away_bp_hard_rate_10d",
]


BP5_ALL = list(
    dict.fromkeys(
        [
            *BP.CURRENT,
            *BP5,
        ]
    )
)


BP7_ALL = list(
    dict.fromkeys(
        [
            *BP.CURRENT,
            *BP.PLUS_7D,
        ]
    )
)


BP10_ALL = list(
    dict.fromkeys(
        [
            *BP.CURRENT,
            *BP10,
        ]
    )
)


MATCHUP = [
    "home_matchup_hand_pa",
    "away_matchup_hand_pa",

    "home_matchup_hand_woba",
    "away_matchup_hand_woba",

    "home_matchup_hand_k_rate",
    "away_matchup_hand_k_rate",

    "home_matchup_hand_bb_rate",
    "away_matchup_hand_bb_rate",

    "home_matchup_hand_hard_rate",
    "away_matchup_hand_hard_rate",

    "home_matchup_mix_pitches",
    "away_matchup_mix_pitches",

    "home_matchup_mix_woba",
    "away_matchup_mix_woba",

    "home_matchup_mix_hard_rate",
    "away_matchup_mix_hard_rate",

    "home_matchup_mix_coverage",
    "away_matchup_mix_coverage",
]


DEFENSE = [
    "home_def_bbe_30d",
    "away_def_bbe_30d",

    "home_def_woba_saved_30d",
    "away_def_woba_saved_30d",

    "home_def_bbe_60d",
    "away_def_bbe_60d",

    "home_def_woba_saved_60d",
    "away_def_woba_saved_60d",
]


NEW_VARIANTS = {
    "bullpen_plus_5d":
        BP5_ALL,

    "bullpen_plus_10d":
        BP10_ALL,

    "bullpen_7d_plus_matchup":
        [
            *BP7_ALL,
            *MATCHUP,
        ],

    "bullpen_7d_plus_defense":
        [
            *BP7_ALL,
            *DEFENSE,
        ],

    "bullpen_7d_plus_matchup_defense":
        [
            *BP7_ALL,
            *MATCHUP,
            *DEFENSE,
        ],

    "bullpen_5d_plus_matchup_defense":
        [
            *BP5_ALL,
            *MATCHUP,
            *DEFENSE,
        ],

    "bullpen_10d_plus_matchup_defense":
        [
            *BP10_ALL,
            *MATCHUP,
            *DEFENSE,
        ],
}


VARIANT_ORDER = [
    "baseline",
    "bullpen_plus_5d",
    "bullpen_plus_7d",
    "bullpen_plus_10d",
    "bullpen_7d_plus_matchup",
    "bullpen_7d_plus_defense",
    "bullpen_7d_plus_matchup_defense",
    "bullpen_5d_plus_matchup_defense",
    "bullpen_10d_plus_matchup_defense",
]


# ============================================================
# REFERENCE
# ============================================================


def load_reference():
    if not REFERENCE_FILE.exists():
        raise RuntimeError(
            f"Missing {REFERENCE_FILE}"
        )

    df = pd.read_csv(
        REFERENCE_FILE,
        encoding="utf-8-sig",
    )

    df["_date"] = pd.to_datetime(
        df["game_date"],
        errors="coerce",
    ).dt.normalize()

    baseline = df[
        df["variant"]
        == "baseline"
    ].copy()

    bullpen7 = df[
        df["variant"]
        == "bullpen_plus_7d"
    ].copy()

    if baseline.empty:
        raise RuntimeError(
            "Prior predictions missing baseline"
        )

    if bullpen7.empty:
        raise RuntimeError(
            "Prior predictions missing bullpen_plus_7d"
        )

    dates = [
        pd.Timestamp(value).normalize()
        for value in sorted(
            bullpen7[
                "_date"
            ]
            .dropna()
            .unique()
        )
    ]

    if not dates:
        raise RuntimeError(
            "No evaluation dates found"
        )

    return (
        pd.concat(
            [
                baseline,
                bullpen7,
            ],
            ignore_index=True,
            sort=False,
        ),
        dates,
    )


# ============================================================
# SUPPLEMENTAL STATCAST CACHE
# ============================================================


SUPPLEMENT_COLUMNS = [
    "game_date",
    "game_pk",
    "at_bat_number",
    "pitch_number",
    "pitcher",
    "batter",
    "p_throws",
    "pitch_type",
    "estimated_woba_using_speedangle",
]


def parse_chunk_dates(
    path: Path,
):
    parts = path.stem.split("_")

    if len(parts) != 6:
        raise RuntimeError(
            f"Unexpected Statcast cache filename: "
            f"{path.name}"
        )

    start = pd.Timestamp(
        "-".join(
            parts[:3]
        )
    )

    end = pd.Timestamp(
        "-".join(
            parts[3:]
        )
    )

    return (
        start,
        end,
    )


def fetch_supplement_chunk(
    start: pd.Timestamp,
    end: pd.Timestamp,
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
        4,
    ):
        try:
            raw = mlb_statcast_search(
                start.strftime(
                    "%Y-%m-%d"
                ),
                end.strftime(
                    "%Y-%m-%d"
                ),
                player_type="batter",
                game_type="R",
            )

            df = EXP.as_pandas(
                raw
            )

            if df.empty:
                return pd.DataFrame(
                    columns=SUPPLEMENT_COLUMNS
                )

            required = [
                "game_date",
                "game_pk",
                "at_bat_number",
                "pitch_number",
                "p_throws",
                "pitch_type",
                "estimated_woba_using_speedangle",
            ]

            missing = [
                col
                for col in required
                if col not in df.columns
            ]

            if missing:
                raise RuntimeError(
                    "Statcast response missing "
                    f"columns: {missing}"
                )

            keep = [
                col
                for col in SUPPLEMENT_COLUMNS
                if col in df.columns
            ]

            return df[
                keep
            ].copy()

        except Exception as exc:
            last_error = exc

            if attempt < 3:
                time.sleep(
                    3 * attempt
                )

    raise RuntimeError(
        "Supplemental Statcast download failed "
        f"for {start.date()}..{end.date()}: "
        f"{last_error}"
    )


def load_supplement_cache():
    base_paths = sorted(
        BP.CACHE_DIR.glob(
            "*.parquet"
        )
    )

    if not base_paths:
        raise RuntimeError(
            "Original Statcast cache is missing"
        )

    frames = []

    for index, base_path in enumerate(
        base_paths,
        start=1,
    ):
        supplement_path = (
            SUPPLEMENT_CACHE_DIR
            / base_path.name
        )

        if supplement_path.exists():
            print(
                f"SUPPLEMENT CACHE "
                f"{index}/{len(base_paths)} "
                f"{supplement_path.name}"
            )

            frame = EXP.read_parquet(
                supplement_path
            )

        else:
            start, end = (
                parse_chunk_dates(
                    base_path
                )
            )

            print(
                f"SUPPLEMENT DOWNLOAD "
                f"{index}/{len(base_paths)} "
                f"{start.date()}..{end.date()}"
            )

            frame = (
                fetch_supplement_chunk(
                    start,
                    end,
                )
            )

            EXP.write_parquet(
                supplement_path,
                frame,
            )

        frames.append(
            frame
        )

    supplement = pd.concat(
        frames,
        ignore_index=True,
        sort=False,
    )

    return supplement


def create_join_keys(
    df: pd.DataFrame,
) -> pd.DataFrame:

    out = df.copy()

    out["_join_gamePk"] = (
        out["game_pk"]
        .map(
            EXP.normalize_gamepk
        )
    )

    out["_join_ab"] = (
        pd.to_numeric(
            out["at_bat_number"],
            errors="coerce",
        )
        .astype("Int64")
    )

    out["_join_pitch"] = (
        pd.to_numeric(
            out["pitch_number"],
            errors="coerce",
        )
        .astype("Int64")
    )

    return out


def add_supplement_columns(
    raw: pd.DataFrame,
    supplement: pd.DataFrame,
) -> pd.DataFrame:

    base = create_join_keys(
        raw
    )

    extra = create_join_keys(
        supplement
    )

    extra["p_throws"] = (
        extra["p_throws"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    extra["pitch_type"] = (
        extra["pitch_type"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    extra[
        "estimated_woba_using_speedangle"
    ] = pd.to_numeric(
        extra[
            "estimated_woba_using_speedangle"
        ],
        errors="coerce",
    )

    keys = [
        "_join_gamePk",
        "_join_ab",
        "_join_pitch",
    ]

    extra = (
        extra[
            [
                *keys,
                "p_throws",
                "pitch_type",
                "estimated_woba_using_speedangle",
            ]
        ]
        .dropna(
            subset=keys
        )
        .drop_duplicates(
            subset=keys,
            keep="last",
        )
    )

    merged = base.merge(
        extra,
        on=keys,
        how="left",
        validate="many_to_one",
    )

    matched = int(
        merged[
            "pitch_type"
        ]
        .notna()
        .sum()
    )

    print(
        "Supplemental Statcast matched "
        f"{matched:,} of {len(merged):,} pitch rows."
    )

    if matched == 0:
        raise RuntimeError(
            "Supplemental Statcast did not match "
            "the original cache"
        )

    return merged.drop(
        columns=keys
    )


# ============================================================
# LOAD STATCAST
# ============================================================


def load_statcast():
    print(
        "Loading existing Statcast cache..."
    )

    raw = BP.load_cache()

    print(
        f"Original Statcast rows: "
        f"{len(raw):,}"
    )

    print(
        "Loading missing matchup/defense columns..."
    )

    supplement = (
        load_supplement_cache()
    )

    raw = add_supplement_columns(
        raw,
        supplement,
    )

    prepared = (
        EXP.prepare_statcast(
            raw
        )
    )

    prepared["_batter"] = (
        pd.to_numeric(
            prepared["batter"],
            errors="coerce",
        )
        .astype("Int64")
    )

    prepared["_pitcher"] = (
        pd.to_numeric(
            prepared["pitcher"],
            errors="coerce",
        )
        .astype("Int64")
    )

    return (
        raw,
        prepared,
    )


# ============================================================
# PREGAME STARTERS
# ============================================================


def load_pregame_starters():
    rows = []

    for path in sorted(
        SDV_DIR.glob(
            "*_sportsdataverse.csv"
        )
    ):
        df = pd.read_csv(
            path,
            dtype=str,
            encoding="utf-8-sig",
        )

        required = {
            "gamePk",
            "game_date",
            "home_pitcher_id",
            "away_pitcher_id",
        }

        if not required.issubset(
            df.columns
        ):
            continue

        columns = [
            "gamePk",
            "game_date",
            "home_pitcher_id",
            "away_pitcher_id",
        ]

        if (
            "sdv_as_of_date"
            in df.columns
        ):
            columns.append(
                "sdv_as_of_date"
            )

        part = df[
            columns
        ].copy()

        part["_gamePk_norm"] = (
            part["gamePk"]
            .map(
                EXP.normalize_gamepk
            )
        )

        part["_game_date_dt"] = (
            pd.to_datetime(
                part["game_date"],
                errors="coerce",
            )
            .dt.normalize()
        )

        if (
            "sdv_as_of_date"
            in part.columns
        ):
            part["_asof"] = (
                pd.to_datetime(
                    part[
                        "sdv_as_of_date"
                    ],
                    errors="coerce",
                )
                .dt.normalize()
            )

        else:
            part["_asof"] = pd.NaT

        part["_home_starter_id"] = (
            pd.to_numeric(
                part[
                    "home_pitcher_id"
                ],
                errors="coerce",
            )
            .astype("Int64")
        )

        part["_away_starter_id"] = (
            pd.to_numeric(
                part[
                    "away_pitcher_id"
                ],
                errors="coerce",
            )
            .astype("Int64")
        )

        valid = (
            part[
                "_gamePk_norm"
            ].notna()
            & part[
                "_game_date_dt"
            ].notna()
        )

        valid &= (
            part["_asof"].isna()
            | (
                part["_asof"]
                < part[
                    "_game_date_dt"
                ]
            )
        )

        rows.append(
            part.loc[
                valid,
                [
                    "_gamePk_norm",
                    "_game_date_dt",
                    "_asof",
                    "_home_starter_id",
                    "_away_starter_id",
                ],
            ]
        )

    if not rows:
        raise RuntimeError(
            "No usable pregame starter IDs found"
        )

    out = pd.concat(
        rows,
        ignore_index=True,
    )

    out["_asof_sort"] = (
        out["_asof"]
        .fillna(
            pd.Timestamp(
                "1900-01-01"
            )
        )
    )

    out = (
        out.sort_values(
            [
                "_gamePk_norm",
                "_game_date_dt",
                "_asof_sort",
            ]
        )
        .drop_duplicates(
            [
                "_gamePk_norm",
                "_game_date_dt",
            ],
            keep="last",
        )
        .drop(
            columns=[
                "_asof_sort",
            ]
        )
    )

    return out


def attach_starters(
    frame: pd.DataFrame,
    starters: pd.DataFrame,
):

    out = frame.copy()

    out["_gamePk_norm"] = (
        out["gamePk"]
        .map(
            EXP.normalize_gamepk
        )
    )

    return out.merge(
        starters,
        on=[
            "_gamePk_norm",
            "_game_date_dt",
        ],
        how="left",
        validate="many_to_one",
    )


# ============================================================
# 5-DAY / 10-DAY BULLPEN
# ============================================================


def attach_bp_windows(
    frame: pd.DataFrame,
    team_daily: pd.DataFrame,
):

    out = frame.copy()

    values = {
        col: []
        for col in [
            *BP5,
            *BP10,
        ]
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

            for days in (
                5,
                10,
            ):
                key = (
                    date,
                    team,
                    days,
                )

                if key not in cache:
                    cache[key] = BP.perf(
                        team_daily,
                        team,
                        date,
                        days,
                    )

                stats = cache[
                    key
                ]

                values[
                    f"{side}_bp_pa_{days}d"
                ].append(
                    stats["pa"]
                )

                values[
                    f"{side}_bp_woba_allowed_{days}d"
                ].append(
                    stats["woba"]
                )

                values[
                    f"{side}_bp_k_rate_{days}d"
                ].append(
                    stats["k"]
                )

                values[
                    f"{side}_bp_bb_rate_{days}d"
                ].append(
                    stats["bb"]
                )

                values[
                    f"{side}_bp_hard_rate_{days}d"
                ].append(
                    stats["hard"]
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


# ============================================================
# MATCHUP DATA
# ============================================================


def make_pa_daily(
    statcast: pd.DataFrame,
    group_cols: list[str],
):

    pa = statcast[
        statcast["_is_pa"]
        & statcast["_batter"].notna()
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

    return (
        pa.groupby(
            group_cols,
            as_index=False,
        )
        .agg(
            pa=(
                "_is_pa",
                "sum",
            ),
            woba_sum=(
                "_woba_sum",
                "sum",
            ),
            woba_n=(
                "_woba_n",
                "sum",
            ),
            k=(
                "_is_k",
                "sum",
            ),
            bb=(
                "_is_bb",
                "sum",
            ),
            bbe=(
                "_is_bbe",
                "sum",
            ),
            hard=(
                "_is_hard",
                "sum",
            ),
        )
    )


def build_matchup_indexes(
    statcast: pd.DataFrame,
):

    work = statcast.copy()

    work["_throws"] = (
        work["p_throws"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    work["_ptype"] = (
        work["pitch_type"]
        .astype("string")
        .str.upper()
        .str.strip()
    )

    hand_data = work[
        work["_throws"]
        .isin(
            [
                "L",
                "R",
            ]
        )
    ].copy()

    pitch_type_data = work[
        work["_ptype"].notna()
        & (
            work["_ptype"]
            != ""
        )
    ].copy()

    hand = make_pa_daily(
        hand_data,
        [
            "_game_date_dt",
            "_batter",
            "_throws",
        ],
    )

    pitch_type = make_pa_daily(
        pitch_type_data,
        [
            "_game_date_dt",
            "_batter",
            "_ptype",
        ],
    )

    pitcher_mix = (
        pitch_type_data[
            pitch_type_data[
                "_pitcher"
            ].notna()
        ]
        .groupby(
            [
                "_game_date_dt",
                "_pitcher",
                "_throws",
                "_ptype",
            ],
            as_index=False,
        )
        .size()
        .rename(
            columns={
                "size":
                    "pitches"
            }
        )
    )

    hand_index = {
        int(key): group
        for key, group in hand.groupby(
            "_batter"
        )
    }

    pitch_type_index = {
        int(key): group
        for key, group in pitch_type.groupby(
            "_batter"
        )
    }

    pitcher_index = {
        int(key): group
        for key, group in pitcher_mix.groupby(
            "_pitcher"
        )
    }

    return (
        hand_index,
        pitch_type_index,
        pitcher_index,
    )


def aggregate_rows(
    parts: list[pd.DataFrame],
):
    parts = [
        part
        for part in parts
        if (
            part is not None
            and not part.empty
        )
    ]

    if not parts:
        return {
            "pa": 0.0,
            "woba": np.nan,
            "k": np.nan,
            "bb": np.nan,
            "hard": np.nan,
        }

    data = pd.concat(
        parts,
        ignore_index=True,
    )

    pa = float(
        data["pa"].sum()
    )

    woba_n = float(
        data["woba_n"].sum()
    )

    bbe = float(
        data["bbe"].sum()
    )

    return {
        "pa":
            pa,

        "woba":
            EXP.safe_ratio(
                float(
                    data[
                        "woba_sum"
                    ].sum()
                ),
                woba_n,
            ),

        "k":
            EXP.safe_ratio(
                float(
                    data[
                        "k"
                    ].sum()
                ),
                pa,
            ),

        "bb":
            EXP.safe_ratio(
                float(
                    data[
                        "bb"
                    ].sum()
                ),
                pa,
            ),

        "hard":
            EXP.safe_ratio(
                float(
                    data[
                        "hard"
                    ].sum()
                ),
                bbe,
            ),
    }


def hitter_stats(
    index: dict,
    hitters: list,
    date: pd.Timestamp,
    days: int,
    key_col: str,
    key_value,
):

    start = (
        date
        - pd.Timedelta(
            days=days
        )
    )

    parts = []

    for hitter in hitters:
        if pd.isna(hitter):
            continue

        table = index.get(
            int(hitter)
        )

        if table is None:
            continue

        part = table[
            (
                table[
                    "_game_date_dt"
                ]
                >= start
            )
            & (
                table[
                    "_game_date_dt"
                ]
                < date
            )
            & (
                table[
                    key_col
                ]
                == key_value
            )
        ]

        if not part.empty:
            parts.append(
                part
            )

    return aggregate_rows(
        parts
    )


def starter_profile(
    pitcher_index: dict,
    starter_id,
    date: pd.Timestamp,
):

    if pd.isna(starter_id):
        return (
            None,
            {},
            0.0,
        )

    table = pitcher_index.get(
        int(starter_id)
    )

    if table is None:
        return (
            None,
            {},
            0.0,
        )

    start = (
        date
        - pd.Timedelta(
            days=30
        )
    )

    part = table[
        (
            table[
                "_game_date_dt"
            ]
            >= start
        )
        & (
            table[
                "_game_date_dt"
            ]
            < date
        )
    ]

    if part.empty:
        return (
            None,
            {},
            0.0,
        )

    hand_counts = (
        part.groupby(
            "_throws"
        )["pitches"]
        .sum()
        .sort_values(
            ascending=False
        )
    )

    hand = next(
        (
            value
            for value in hand_counts.index
            if value in {
                "L",
                "R",
            }
        ),
        None,
    )

    mix_counts = (
        part.groupby(
            "_ptype"
        )["pitches"]
        .sum()
        .sort_values(
            ascending=False
        )
    )

    total = float(
        mix_counts.sum()
    )

    if total > 0:
        mix = {
            str(pitch_type):
                float(count)
                / total
            for (
                pitch_type,
                count,
            ) in mix_counts.items()
        }

    else:
        mix = {}

    return (
        hand,
        mix,
        total,
    )


def pitchmix_stats(
    pitch_type_index: dict,
    hitters: list,
    date: pd.Timestamp,
    mix: dict,
):

    rows = []

    for pitch_type, weight in mix.items():
        stats = hitter_stats(
            pitch_type_index,
            hitters,
            date,
            90,
            "_ptype",
            pitch_type,
        )

        if (
            stats["pa"] > 0
            and pd.notna(
                stats["woba"]
            )
        ):
            rows.append(
                (
                    weight,
                    stats,
                )
            )

    coverage = float(
        sum(
            weight
            for weight, _
            in rows
        )
    )

    if coverage <= 0:
        return {
            "woba": np.nan,
            "hard": np.nan,
            "coverage": 0.0,
        }

    def weighted(
        field: str,
    ):
        valid = [
            (
                weight,
                stats[field],
            )
            for (
                weight,
                stats,
            ) in rows
            if pd.notna(
                stats[field]
            )
        ]

        denominator = sum(
            weight
            for (
                weight,
                _,
            ) in valid
        )

        if denominator <= 0:
            return np.nan

        return float(
            sum(
                weight
                * float(value)
                for (
                    weight,
                    value,
                ) in valid
            )
            / denominator
        )

    return {
        "woba":
            weighted(
                "woba"
            ),

        "hard":
            weighted(
                "hard"
            ),

        "coverage":
            coverage,
    }


def attach_matchup(
    frame: pd.DataFrame,
    historical_lineups: pd.DataFrame,
    hand_index: dict,
    pitch_type_index: dict,
    pitcher_index: dict,
):

    out = frame.copy()

    values = {
        col: []
        for col in MATCHUP
    }

    lineup_cache = {}
    matchup_cache = {}

    for position, (
        _,
        row,
    ) in enumerate(
        out.iterrows(),
        start=1,
    ):
        if (
            position == 1
            or position % 250 == 0
            or position == len(out)
        ):
            print(
                f"MATCHUP FEATURES "
                f"{position}/{len(out)}"
            )

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

            if side == "home":
                starter_id = row[
                    "_away_starter_id"
                ]

            else:
                starter_id = row[
                    "_home_starter_id"
                ]

            lineup_key = (
                date,
                team,
            )

            if (
                lineup_key
                not in lineup_cache
            ):
                (
                    hitters,
                    _,
                ) = (
                    LINEUP.expected_lineup(
                        historical_lineups,
                        team,
                        date,
                    )
                )

                lineup_cache[
                    lineup_key
                ] = hitters

            hitters = lineup_cache[
                lineup_key
            ]

            starter_key = (
                None
                if pd.isna(
                    starter_id
                )
                else int(
                    starter_id
                )
            )

            matchup_key = (
                date,
                team,
                starter_key,
            )

            if (
                matchup_key
                not in matchup_cache
            ):
                (
                    hand,
                    mix,
                    pitch_count,
                ) = starter_profile(
                    pitcher_index,
                    starter_id,
                    date,
                )

                if hand:
                    hand_stats = hitter_stats(
                        hand_index,
                        hitters,
                        date,
                        60,
                        "_throws",
                        hand,
                    )

                else:
                    hand_stats = {
                        "pa": 0.0,
                        "woba": np.nan,
                        "k": np.nan,
                        "bb": np.nan,
                        "hard": np.nan,
                    }

                mix_stats = pitchmix_stats(
                    pitch_type_index,
                    hitters,
                    date,
                    mix,
                )

                matchup_cache[
                    matchup_key
                ] = (
                    hand_stats,
                    mix_stats,
                    pitch_count,
                )

            (
                hand_stats,
                mix_stats,
                pitch_count,
            ) = matchup_cache[
                matchup_key
            ]

            values[
                f"{side}_matchup_hand_pa"
            ].append(
                hand_stats["pa"]
            )

            values[
                f"{side}_matchup_hand_woba"
            ].append(
                hand_stats["woba"]
            )

            values[
                f"{side}_matchup_hand_k_rate"
            ].append(
                hand_stats["k"]
            )

            values[
                f"{side}_matchup_hand_bb_rate"
            ].append(
                hand_stats["bb"]
            )

            values[
                f"{side}_matchup_hand_hard_rate"
            ].append(
                hand_stats["hard"]
            )

            values[
                f"{side}_matchup_mix_pitches"
            ].append(
                pitch_count
            )

            values[
                f"{side}_matchup_mix_woba"
            ].append(
                mix_stats["woba"]
            )

            values[
                f"{side}_matchup_mix_hard_rate"
            ].append(
                mix_stats["hard"]
            )

            values[
                f"{side}_matchup_mix_coverage"
            ].append(
                mix_stats["coverage"]
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


# ============================================================
# DEFENSE
# ============================================================


def build_defense_daily(
    statcast: pd.DataFrame,
):

    expected_col = (
        "estimated_woba_using_speedangle"
    )

    if (
        expected_col
        not in statcast.columns
    ):
        raise RuntimeError(
            f"Statcast missing {expected_col}"
        )

    data = statcast[
        statcast["_is_bbe"]
        & statcast[
            "_pitching_team"
        ].notna()
        & statcast[
            "_woba_value"
        ].notna()
    ].copy()

    data["_expected_woba"] = (
        pd.to_numeric(
            data[
                expected_col
            ],
            errors="coerce",
        )
    )

    data = data[
        data[
            "_expected_woba"
        ].notna()
    ].copy()

    if data.empty:
        raise RuntimeError(
            "No usable expected-contact "
            "defense data found"
        )

    # Positive value means the defense allowed a
    # better actual outcome than the contact quality
    # would have predicted.
    data["_woba_saved"] = (
        data["_expected_woba"]
        - data["_woba_value"]
    )

    return (
        data.groupby(
            [
                "_game_date_dt",
                "_pitching_team",
            ],
            as_index=False,
        )
        .agg(
            def_bbe=(
                "_woba_saved",
                "size",
            ),
            def_saved=(
                "_woba_saved",
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


def defense_window(
    daily: pd.DataFrame,
    team: str,
    date: pd.Timestamp,
    days: int,
):

    start = (
        date
        - pd.Timedelta(
            days=days
        )
    )

    part = daily[
        (
            daily["team"]
            == team
        )
        & (
            daily[
                "_game_date_dt"
            ]
            >= start
        )
        & (
            daily[
                "_game_date_dt"
            ]
            < date
        )
    ]

    if part.empty:
        return (
            0.0,
            np.nan,
        )

    bbe = float(
        part[
            "def_bbe"
        ].sum()
    )

    saved = EXP.safe_ratio(
        float(
            part[
                "def_saved"
            ].sum()
        ),
        bbe,
    )

    return (
        bbe,
        saved,
    )


def attach_defense(
    frame: pd.DataFrame,
    daily: pd.DataFrame,
):

    out = frame.copy()

    values = {
        col: []
        for col in DEFENSE
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

            for days in (
                30,
                60,
            ):
                key = (
                    date,
                    team,
                    days,
                )

                if key not in cache:
                    cache[key] = (
                        defense_window(
                            daily,
                            team,
                            date,
                            days,
                        )
                    )

                (
                    bbe,
                    saved,
                ) = cache[
                    key
                ]

                values[
                    f"{side}_def_bbe_{days}d"
                ].append(
                    bbe
                )

                values[
                    f"{side}_def_woba_saved_{days}d"
                ].append(
                    saved
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


# ============================================================
# MODEL
# ============================================================


def predict_new(
    frame: pd.DataFrame,
    dates: list[pd.Timestamp],
    baseline_features: list[str],
    selected: dict,
):

    variants = {
        name:
            list(
                dict.fromkeys(
                    [
                        *baseline_features,
                        *extras,
                    ]
                )
            )
        for (
            name,
            extras,
        ) in NEW_VARIANTS.items()
    }

    rows = []

    for date_index, date in enumerate(
        dates,
        start=1,
    ):
        print(
            f"TEST DATE "
            f"{date_index}/{len(dates)} "
            f"{date.date()}"
        )

        prior = frame[
            frame[
                "_game_date_dt"
            ]
            < date
        ]

        current = frame[
            frame[
                "_game_date_dt"
            ]
            == date
        ]

        if current.empty:
            continue

        for (
            name,
            features,
        ) in variants.items():

            home_model = BP.fit(
                prior,
                features,
                "target_home_runs",
                selected[
                    name
                ]["home"],
            )

            away_model = BP.fit(
                prior,
                features,
                "target_away_runs",
                selected[
                    name
                ]["away"],
            )

            home_runs = np.asarray(
                home_model.predict(
                    current[
                        features
                    ]
                ),
                dtype=float,
            )

            away_runs = np.asarray(
                away_model.predict(
                    current[
                        features
                    ]
                ),
                dtype=float,
            )

            if (
                np.any(
                    ~np.isfinite(
                        home_runs
                    )
                )
                or np.any(
                    home_runs < 0
                )
            ):
                raise RuntimeError(
                    f"{name} invalid home predictions"
                )

            if (
                np.any(
                    ~np.isfinite(
                        away_runs
                    )
                )
                or np.any(
                    away_runs < 0
                )
            ):
                raise RuntimeError(
                    f"{name} invalid away predictions"
                )

            for position, (
                _,
                game,
            ) in enumerate(
                current.iterrows()
            ):
                actual_home = float(
                    game[
                        "target_home_runs"
                    ]
                )

                actual_away = float(
                    game[
                        "target_away_runs"
                    ]
                )

                if (
                    actual_home
                    == actual_away
                ):
                    continue

                (
                    home_probability,
                    away_probability,
                    _,
                ) = (
                    JUICE.moneyline_probabilities(
                        float(
                            home_runs[
                                position
                            ]
                        ),
                        float(
                            away_runs[
                                position
                            ]
                        ),
                    )
                )

                rows.append(
                    {
                        "variant":
                            name,

                        "game_date":
                            date.strftime(
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
                                home_runs[
                                    position
                                ]
                            ),

                        "model_away_runs":
                            float(
                                away_runs[
                                    position
                                ]
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
                                if (
                                    actual_home
                                    > actual_away
                                )
                                else 0.0
                            ),
                    }
                )

    if not rows:
        raise RuntimeError(
            "No final-test predictions produced"
        )

    return pd.DataFrame(
        rows
    )


# ============================================================
# SUMMARY
# ============================================================


def summarize(
    predictions: pd.DataFrame,
):

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

        (
            metrics,
            buckets,
        ) = BP.score(
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

    reference = (
        summary[
            summary[
                "variant"
            ]
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
        - reference[
            "probability_win_correlation"
        ]
    )

    summary[
        "win_spread_change_vs_bullpen_7d"
    ] = (
        summary[
            "high_minus_low_win_rate"
        ]
        - reference[
            "high_minus_low_win_rate"
        ]
    )

    summary[
        "probability_error_change_vs_bullpen_7d"
    ] = (
        summary[
            "probability_error"
        ]
        - reference[
            "probability_error"
        ]
    )

    summary[
        "bucket_gap_change_vs_bullpen_7d"
    ] = (
        summary[
            "average_bucket_probability_gap"
        ]
        - reference[
            "average_bucket_probability_gap"
        ]
    )

    return (
        summary,
        bucket_tables,
    )


# ============================================================
# REPORT
# ============================================================


def pct(value):
    if pd.isna(value):
        return ""

    return (
        f"{float(value) * 100:.2f}%"
    )


def num(value):
    if pd.isna(value):
        return ""

    return (
        f"{float(value):.4f}"
    )


def report_html(
    summary: pd.DataFrame,
    buckets: dict,
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
        shown[col] = (
            shown[col]
            .map(
                pct
            )
        )

    for col in [
        "probability_win_correlation",
        "correlation_change_vs_bullpen_7d",
        "probability_error",
        "probability_error_change_vs_bullpen_7d",
    ]:
        shown[col] = (
            shown[col]
            .map(
                num
            )
        )

    sections = []

    for (
        name,
        table,
    ) in buckets.items():

        output = table.copy()

        output[
            "average_model_probability"
        ] = (
            output[
                "average_model_probability"
            ]
            .map(
                pct
            )
        )

        output[
            "actual_home_win_rate"
        ] = (
            output[
                "actual_home_win_rate"
            ]
            .map(
                pct
            )
        )

        sections.append(
            "<h3>"
            + html.escape(
                name
            )
            + "</h3>"
            + output.to_html(
                index=False,
                border=0,
            )
        )

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB Moneyline Final Feature Test</title>
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

<h1>MLB Moneyline Final Feature Test</h1>

<p>
Tests 5/7/10-day bullpen performance, starter matchup,
defense, and combinations.
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

{''.join(sections)}

</body>
</html>
"""


# ============================================================
# MAIN
# ============================================================


def main():
    print(
        "Loading prior moneyline results..."
    )

    (
        reference,
        dates,
    ) = load_reference()

    print(
        "Loading training data..."
    )

    (
        frame,
        baseline_features,
    ) = EXP.load_training()

    print(
        "Loading Statcast data..."
    )

    (
        raw_statcast,
        statcast,
    ) = load_statcast()

    print(
        "Loading pregame starter IDs..."
    )

    frame = attach_starters(
        frame,
        load_pregame_starters(),
    )

    print(
        "Building bullpen features..."
    )

    (
        team_daily,
        pitcher_daily,
    ) = BP.build_tables(
        raw_statcast
    )

    frame = BP.attach_features(
        frame,
        team_daily,
        pitcher_daily,
    )

    frame = attach_bp_windows(
        frame,
        team_daily,
    )

    print(
        "Building historical lineups..."
    )

    historical_lineups = (
        LINEUP.build_historical_starting_lineups(
            statcast
        )
    )

    print(
        "Building starter matchup features..."
    )

    (
        hand_index,
        pitch_type_index,
        pitcher_index,
    ) = build_matchup_indexes(
        statcast
    )

    frame = attach_matchup(
        frame,
        historical_lineups,
        hand_index,
        pitch_type_index,
        pitcher_index,
    )

    print(
        "Building defense features..."
    )

    defense_daily = (
        build_defense_daily(
            statcast
        )
    )

    frame = attach_defense(
        frame,
        defense_daily,
    )

    all_features = list(
        dict.fromkeys(
            [
                *BP5_ALL,
                *BP7_ALL,
                *BP10_ALL,
                *MATCHUP,
                *DEFENSE,
            ]
        )
    )

    for col in all_features:
        if col not in frame.columns:
            raise RuntimeError(
                f"Missing generated feature: {col}"
            )

        frame[col] = pd.to_numeric(
            frame[col],
            errors="coerce",
        )

        invalid = (
            frame[col].notna()
            & ~np.isfinite(
                frame[col]
            )
        )

        if invalid.any():
            raise RuntimeError(
                f"Invalid values in feature {col}"
            )

    development = frame[
        frame[
            "_game_date_dt"
        ]
        < dates[0]
    ].copy()

    variants = {
        name:
            list(
                dict.fromkeys(
                    [
                        *baseline_features,
                        *extras,
                    ]
                )
            )
        for (
            name,
            extras,
        ) in NEW_VARIANTS.items()
    }

    print(
        "Selecting settings for final variants..."
    )

    selected = BP.select_params(
        development,
        variants,
    )

    print(
        "Running final walk-forward test..."
    )

    new_predictions = predict_new(
        frame,
        dates,
        baseline_features,
        selected,
    )

    output_columns = [
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

    predictions = pd.concat(
        [
            reference[
                output_columns
            ].copy(),

            new_predictions[
                output_columns
            ].copy(),
        ],
        ignore_index=True,
        sort=False,
    )

    (
        summary,
        buckets,
    ) = summarize(
        predictions
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
            dates,
        ),
        encoding="utf-8",
    )

    print("")
    print(
        "Moneyline final feature test complete."
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