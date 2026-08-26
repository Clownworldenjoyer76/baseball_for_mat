#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/01_merge/build_juice_files.py

import glob
import math
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson, skellam

INPUT_DIR = Path("docs/win/baseball/mlb/01_merge")
OUTPUT_DIR = Path("docs/win/baseball/mlb/02_juice")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ERROR_DIR = Path("docs/win/baseball/mlb/errors/01_merge")
ERROR_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = ERROR_DIR / "build_juice_files.txt"

PROB_TOLERANCE = 1e-6

# Run-line calibration fitted out-of-sample on the current production model.
# The underlying Skellam run-line probability formula remains unchanged.
RUN_LINE_CALIBRATION_INTERCEPT = 0.0
RUN_LINE_CALIBRATION_SLOPE = 0.281791
RUN_LINE_CALIBRATION_EPS = 1e-12

LEGACY_OFFICIAL_PROBABILITY_COLUMNS = [
    "home_normalized_prob_moneyline",
    "away_normalized_prob_moneyline",
    "home_normalized_prob_run_line",
    "away_normalized_prob_run_line",
    "over_normalized_prob_total",
    "under_normalized_prob_total",
]

RUN_PROJECTION_COLUMNS = [
    "dratings_home_projected_runs",
    "dratings_away_projected_runs",
    "dratings_total_projected_runs",
    "model_home_runs",
    "model_away_runs",
    "model_total_runs",
]

CONTEXT_COLS = [
    "gamePk",
    "home_team_id", "away_team_id", "venue_id",
    "roof_type", "turf_type",
    "home_pitcher_id", "away_pitcher_id",
    "home_pitcher_hand", "away_pitcher_hand",
    "home_sp_xwoba", "away_sp_xwoba",
    "home_sp_k_pct", "away_sp_k_pct",
    "home_sp_bb_pct", "away_sp_bb_pct",
    "home_sp_barrel_pct", "away_sp_barrel_pct",
    "home_sp_whiff_pct", "away_sp_whiff_pct",
    "home_sp_sample_flag", "away_sp_sample_flag",
    "home_lineup_xwoba", "home_lineup_barrel_pct", "home_lineup_hard_hit_pct",
    "home_lineup_k_pct", "home_lineup_bb_pct", "home_lineup_exit_velo",
    "home_lineup_frv", "home_lineup_brv", "home_catcher_framing",
    "home_low_sample_count", "home_n_left", "home_n_right", "home_n_switch",
    "away_lineup_xwoba", "away_lineup_barrel_pct", "away_lineup_hard_hit_pct",
    "away_lineup_k_pct", "away_lineup_bb_pct", "away_lineup_exit_velo",
    "away_lineup_frv", "away_lineup_brv", "away_catcher_framing",
    "away_low_sample_count", "away_n_left", "away_n_right", "away_n_switch",
    "park_factor", "park_wOBAcon", "park_xwOBAcon", "park_HR", "park_R",
    "park_factor_B", "park_wOBAcon_B", "park_xwOBAcon_B", "park_HR_B", "park_R_B",
    "weather_applicable", "weather_time",
    "temp_f", "wind_mph", "wind_dir",
    "precip_in", "humidity", "will_it_rain", "wind_blowing_out",
    "air_pressure_at_sea_level", "dew_point_f", "symbol_code",
    "home_batters_found", "away_batters_found",
    "home_sp_found", "away_sp_found",
    "sp_data_available", "lineup_data_available",
]

BASE_REQUIRED = [
    "game_id", "sport", "league", "game_date", "game_time", "home_team", "away_team",
    "away_run_line", "home_run_line", "total",
    "home_pitcher", "away_pitcher",
] + RUN_PROJECTION_COLUMNS + CONTEXT_COLS

MONEYLINE_REQUIRED_COLUMNS = BASE_REQUIRED + [
    "away_dk_moneyline_american", "home_dk_moneyline_american",
    "away_dk_moneyline_decimal", "home_dk_moneyline_decimal",
]

RUN_LINE_REQUIRED_COLUMNS = BASE_REQUIRED + [
    "away_dk_run_line_american", "home_dk_run_line_american",
    "away_dk_run_line_decimal", "home_dk_run_line_decimal",
]

TOTAL_REQUIRED_COLUMNS = BASE_REQUIRED + [
    "dk_total_over_american", "dk_total_under_american",
    "dk_total_over_decimal", "dk_total_under_decimal",
]


def _now():
    return datetime.now(UTC).isoformat()


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{_now()} | {msg}\n")


def american_to_decimal(odds):
    try:
        if pd.isna(odds):
            return None
        odds = float(odds)
        if odds == 0:
            return None
        return 1 + (odds / 100) if odds > 0 else 1 + (100 / abs(odds))
    except Exception:
        return None


def parse_slate_date_and_market(file_path: str):
    stem = Path(file_path).stem
    if stem.endswith("_mlb_moneyline"):
        return stem.replace("_mlb_moneyline", ""), "moneyline"
    if stem.endswith("_mlb_run_line"):
        return stem.replace("_mlb_run_line", ""), "run_line"
    if stem.endswith("_mlb_total"):
        return stem.replace("_mlb_total", ""), "total"
    return None, None


def duplicate_columns(columns):
    seen = set()
    dupes = []
    for col in columns:
        if col in seen and col not in dupes:
            dupes.append(col)
        seen.add(col)
    return dupes


def validate_no_duplicate_columns(df, label):
    dupes = duplicate_columns(list(df.columns))
    if dupes:
        raise ValueError(f"{label} duplicate columns: {dupes}")


def validate_schema(df, required_columns, label):
    validate_no_duplicate_columns(df, label)
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{label} missing columns: {missing}")
    legacy = [c for c in LEGACY_OFFICIAL_PROBABILITY_COLUMNS if c in df.columns]
    if legacy:
        raise ValueError(f"{label} contains obsolete official probability columns: {legacy}")


def coerce_numeric(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")


def _validate_prob_series(df, cols, label):
    for col in cols:
        values = pd.to_numeric(df[col], errors="coerce")
        bad = values.isna() | ~np.isfinite(values) | (values < 0) | (values > 1)
        if bad.any():
            sample = df.loc[bad, ["game_id", col]].head(10).to_dict("records")
            raise ValueError(
                f"{label} invalid probability column {col}; bad_rows={int(bad.sum())}; sample={sample}"
            )


def _validate_pair(df, a_col, b_col, label):
    _validate_prob_series(df, [a_col, b_col], label)
    a = pd.to_numeric(df[a_col], errors="coerce")
    b = pd.to_numeric(df[b_col], errors="coerce")
    bad = ((a + b - 1.0).abs() > PROB_TOLERANCE)
    if bad.any():
        sample = df.loc[bad, ["game_id", a_col, b_col]].head(10).to_dict("records")
        raise ValueError(
            f"{label} probability pair does not sum to 1 within {PROB_TOLERANCE}; "
            f"bad_rows={int(bad.sum())}; sample={sample}"
        )


def _validate_totals(df, label):
    cols = [
        "over_model_prob_total_win",
        "over_model_prob_total_loss",
        "under_model_prob_total_win",
        "under_model_prob_total_loss",
        "total_model_prob_push",
    ]
    _validate_prob_series(df, cols, label)
    ow = pd.to_numeric(df["over_model_prob_total_win"], errors="coerce")
    ol = pd.to_numeric(df["over_model_prob_total_loss"], errors="coerce")
    uw = pd.to_numeric(df["under_model_prob_total_win"], errors="coerce")
    ul = pd.to_numeric(df["under_model_prob_total_loss"], errors="coerce")
    push = pd.to_numeric(df["total_model_prob_push"], errors="coerce")
    bad = (
        ((ow + ol + push - 1.0).abs() > PROB_TOLERANCE)
        | ((uw + ul + push - 1.0).abs() > PROB_TOLERANCE)
        | ((uw - ol).abs() > PROB_TOLERANCE)
        | ((ul - ow).abs() > PROB_TOLERANCE)
    )
    if bad.any():
        sample_cols = ["game_id"] + cols
        sample = df.loc[bad, sample_cols].head(10).to_dict("records")
        raise ValueError(
            f"{label} totals probability contract failed; bad_rows={int(bad.sum())}; sample={sample}"
        )


def _validate_run_projection(df, label):
    coerce_numeric(df, RUN_PROJECTION_COLUMNS)
    for col in RUN_PROJECTION_COLUMNS:
        values = pd.to_numeric(df[col], errors="coerce")
        bad = values.isna() | ~np.isfinite(values) | (values < 0)
        if bad.any():
            sample = df.loc[bad, ["game_id", col]].head(10).to_dict("records")
            raise ValueError(f"{label} invalid {col}; bad_rows={int(bad.sum())}; sample={sample}")
    total_diff = (df["model_total_runs"] - (df["model_home_runs"] + df["model_away_runs"])).abs()
    bad_total = total_diff > 1e-6
    if bad_total.any():
        sample = df.loc[
            bad_total,
            ["game_id", "model_home_runs", "model_away_runs", "model_total_runs"],
        ].head(10).to_dict("records")
        raise ValueError(f"{label} model_total_runs mismatch; sample={sample}")


def write_csv_checked(df, out_path, market):
    validate_no_duplicate_columns(df, f"{out_path} output")
    if market == "moneyline":
        _validate_pair(df, "home_model_prob_moneyline", "away_model_prob_moneyline", str(out_path))
    elif market == "run_line":
        _validate_pair(df, "home_model_prob_run_line", "away_model_prob_run_line", str(out_path))
    elif market == "total":
        _validate_totals(df, str(out_path))
    else:
        raise ValueError(f"Unknown market {market}")
    legacy = [c for c in LEGACY_OFFICIAL_PROBABILITY_COLUMNS if c in df.columns]
    if legacy:
        raise ValueError(f"{out_path} contains obsolete official probability columns: {legacy}")
    df.to_csv(out_path, index=False)


def moneyline_probabilities(model_home_runs, model_away_runs):
    p_home_raw = 1.0 - skellam.cdf(0, model_home_runs, model_away_runs)
    p_away_raw = skellam.cdf(-1, model_home_runs, model_away_runs)
    p_tie = skellam.pmf(0, model_home_runs, model_away_runs)
    resolved = p_home_raw + p_away_raw
    if not np.isfinite(resolved) or resolved <= 0:
        raise ValueError("invalid moneyline resolved probability mass")
    return p_home_raw / resolved, p_away_raw / resolved, p_tie


def run_line_probabilities(model_home_runs, model_away_runs, home_line, away_line):
    if not np.isfinite(home_line) or not np.isfinite(away_line):
        raise ValueError("missing run line")
    if abs(home_line + away_line) > PROB_TOLERANCE:
        raise ValueError(f"run lines are not complementary: home={home_line} away={away_line}")
    if sorted([round(home_line, 6), round(away_line, 6)]) != [-1.5, 1.5]:
        raise ValueError(f"unsupported run-line pair: home={home_line} away={away_line}")
    threshold = math.floor(-home_line) + 1
    p_home = 1.0 - skellam.cdf(threshold - 1, model_home_runs, model_away_runs)
    p_away = 1.0 - p_home
    return p_home, p_away


def _calibrate_run_line_probability(probability):
    probability = float(probability)

    if not np.isfinite(probability) or probability < 0.0 or probability > 1.0:
        raise ValueError(f"invalid raw run-line probability: {probability}")

    clipped = min(
        max(probability, RUN_LINE_CALIBRATION_EPS),
        1.0 - RUN_LINE_CALIBRATION_EPS,
    )

    raw_logit = math.log(clipped / (1.0 - clipped))
    calibrated_logit = (
        RUN_LINE_CALIBRATION_INTERCEPT
        + RUN_LINE_CALIBRATION_SLOPE * raw_logit
    )
    calibrated = 1.0 / (1.0 + math.exp(-calibrated_logit))

    if not np.isfinite(calibrated) or calibrated <= 0.0 or calibrated >= 1.0:
        raise ValueError(
            f"invalid calibrated run-line probability: raw={probability} calibrated={calibrated}"
        )

    return calibrated


def calibrate_run_line_probabilities(home_probability, away_probability):
    home_probability = float(home_probability)
    away_probability = float(away_probability)

    if (
        not np.isfinite(home_probability)
        or not np.isfinite(away_probability)
        or abs(home_probability + away_probability - 1.0) > PROB_TOLERANCE
    ):
        raise ValueError(
            "raw run-line probability pair must be finite and sum to 1: "
            f"home={home_probability} away={away_probability}"
        )

    calibrated_home = _calibrate_run_line_probability(home_probability)
    calibrated_away = 1.0 - calibrated_home

    return calibrated_home, calibrated_away


def calibrated_run_line_probabilities(
    model_home_runs,
    model_away_runs,
    home_line,
    away_line,
):
    raw_home, raw_away = run_line_probabilities(
        model_home_runs,
        model_away_runs,
        home_line,
        away_line,
    )

    return calibrate_run_line_probabilities(
        raw_home,
        raw_away,
    )


def totals_probabilities(model_home_runs, model_away_runs, total_line):
    if not np.isfinite(model_home_runs) or not np.isfinite(model_away_runs):
        raise ValueError("missing model run projection")
    if model_home_runs < 0 or model_away_runs < 0:
        raise ValueError("negative model run projection")
    if not np.isfinite(total_line):
        raise ValueError("missing total line")

    lambda_total = model_home_runs + model_away_runs
    frac = abs(total_line - round(total_line))

    if frac < 1e-9:
        k = int(round(total_line))
        p_under = poisson.cdf(k - 1, lambda_total)
        p_push = poisson.pmf(k, lambda_total)
        p_over = 1.0 - poisson.cdf(k, lambda_total)
    elif abs(frac - 0.5) < 1e-9:
        k = math.floor(total_line)
        p_under = poisson.cdf(k, lambda_total)
        p_push = 0.0
        p_over = 1.0 - p_under
    else:
        raise ValueError(f"unsupported total line: {total_line}")

    return p_over, p_under, p_push


def _prepare(file_path, required_columns, numeric_cols):
    df = pd.read_csv(file_path)
    if df.empty:
        raise ValueError(f"{file_path} is empty")
    validate_schema(df, required_columns, str(file_path))
    coerce_numeric(df, list(dict.fromkeys(RUN_PROJECTION_COLUMNS + numeric_cols)))
    _validate_run_projection(df, str(file_path))
    return df


def process_moneyline(file_path, summary):
    df = _prepare(
        file_path,
        MONEYLINE_REQUIRED_COLUMNS,
        [
            "away_run_line", "home_run_line", "total",
            "away_dk_moneyline_american", "home_dk_moneyline_american",
            "away_dk_moneyline_decimal", "home_dk_moneyline_decimal",
        ],
    )
    home_probs, away_probs, ties = [], [], []
    for i, r in df.iterrows():
        try:
            hp, ap, tp = moneyline_probabilities(r["model_home_runs"], r["model_away_runs"])
        except Exception as e:
            raise ValueError(f"{file_path} idx={i} moneyline probability failure: {e}") from e
        home_probs.append(hp)
        away_probs.append(ap)
        ties.append(tp)

    ml = df.copy()
    ml["away_dk_decimal_moneyline"] = ml["away_dk_moneyline_american"].apply(american_to_decimal)
    ml["home_dk_decimal_moneyline"] = ml["home_dk_moneyline_american"].apply(american_to_decimal)
    ml["home_model_prob_moneyline"] = home_probs
    ml["away_model_prob_moneyline"] = away_probs
    ml["model_prob_moneyline_tie_raw"] = ties
    ml["home_model_fair_decimal_moneyline"] = 1.0 / ml["home_model_prob_moneyline"]
    ml["away_model_fair_decimal_moneyline"] = 1.0 / ml["away_model_prob_moneyline"]

    slate_date, market = parse_slate_date_and_market(file_path)
    if not slate_date or market != "moneyline":
        raise ValueError(f"FILENAME ERROR: {file_path}")
    out = OUTPUT_DIR / f"{slate_date}_mlb_moneyline.csv"
    write_csv_checked(ml, out, market)
    log(f"WROTE {out} ({len(ml)} rows)")
    summary["files_written"] += 1
    summary["rows_written"] += len(ml)


def process_run_line(file_path, summary):
    df = _prepare(
        file_path,
        RUN_LINE_REQUIRED_COLUMNS,
        [
            "away_run_line", "home_run_line", "total",
            "away_dk_run_line_american", "home_dk_run_line_american",
            "away_dk_run_line_decimal", "home_dk_run_line_decimal",
        ],
    )
    home_probs, away_probs = [], []
    for i, r in df.iterrows():
        try:
            raw_hp, raw_ap = run_line_probabilities(
                r["model_home_runs"],
                r["model_away_runs"],
                r["home_run_line"],
                r["away_run_line"],
            )
            hp, ap = calibrate_run_line_probabilities(raw_hp, raw_ap)
        except Exception as e:
            raise ValueError(f"{file_path} idx={i} run-line probability failure: {e}") from e
        home_probs.append(hp)
        away_probs.append(ap)

    rl = df.copy()
    rl["home_dk_run_line_decimal"] = rl["home_dk_run_line_american"].apply(american_to_decimal)
    rl["away_dk_run_line_decimal"] = rl["away_dk_run_line_american"].apply(american_to_decimal)
    rl["home_model_prob_run_line"] = home_probs
    rl["away_model_prob_run_line"] = away_probs
    rl["home_model_fair_decimal_run_line"] = 1.0 / rl["home_model_prob_run_line"]
    rl["away_model_fair_decimal_run_line"] = 1.0 / rl["away_model_prob_run_line"]

    slate_date, market = parse_slate_date_and_market(file_path)
    if not slate_date or market != "run_line":
        raise ValueError(f"FILENAME ERROR: {file_path}")
    out = OUTPUT_DIR / f"{slate_date}_mlb_run_line.csv"
    write_csv_checked(rl, out, market)
    log(f"WROTE {out} ({len(rl)} rows)")
    summary["files_written"] += 1
    summary["rows_written"] += len(rl)


def process_total(file_path, summary):
    df = _prepare(
        file_path,
        TOTAL_REQUIRED_COLUMNS,
        [
            "away_run_line", "home_run_line", "total",
            "dk_total_over_american", "dk_total_under_american",
            "dk_total_over_decimal", "dk_total_under_decimal",
        ],
    )
    over_win, over_loss, under_win, under_loss, pushes = [], [], [], [], []
    for i, r in df.iterrows():
        try:
            p_over, p_under, p_push = totals_probabilities(
                r["model_home_runs"], r["model_away_runs"], r["total"]
            )
        except Exception as e:
            raise ValueError(f"{file_path} idx={i} total probability failure: {e}") from e
        over_win.append(p_over)
        over_loss.append(p_under)
        under_win.append(p_under)
        under_loss.append(p_over)
        pushes.append(p_push)

    tot = df.copy()
    tot["dk_total_over_decimal"] = tot["dk_total_over_american"].apply(american_to_decimal)
    tot["dk_total_under_decimal"] = tot["dk_total_under_american"].apply(american_to_decimal)
    tot["over_model_prob_total_win"] = over_win
    tot["over_model_prob_total_loss"] = over_loss
    tot["under_model_prob_total_win"] = under_win
    tot["under_model_prob_total_loss"] = under_loss
    tot["total_model_prob_push"] = pushes

    # Push-aware fair decimal: 1 + (p_loss / p_win).
    # For half-run totals, p_push == 0 and this reduces to 1 / p_win.
    tot["fair_total_over_decimal"] = (
        1.0
        + tot["over_model_prob_total_loss"] / tot["over_model_prob_total_win"]
    )
    tot["fair_total_under_decimal"] = (
        1.0
        + tot["under_model_prob_total_loss"] / tot["under_model_prob_total_win"]
    )

    slate_date, market = parse_slate_date_and_market(file_path)
    if not slate_date or market != "total":
        raise ValueError(f"FILENAME ERROR: {file_path}")
    out = OUTPUT_DIR / f"{slate_date}_mlb_total.csv"
    write_csv_checked(tot, out, market)
    log(f"WROTE {out} ({len(tot)} rows)")
    summary["files_written"] += 1
    summary["rows_written"] += len(tot)


def main():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== build_juice_files RUN {_now()} ===\n")

    summary = {
        "files_written": 0,
        "rows_written": 0,
        "empty": 0,
        "schema_errors": 0,
        "row_issues": 0,
        "errors": 0,
    }

    log("MODEL PROBABILITIES ARE PRICE-INDEPENDENT: sportsbook odds are not probability inputs")

    for f in OUTPUT_DIR.glob("*.csv"):
        f.unlink()

    try:
        groups = [
            ("moneyline", sorted(glob.glob(str(INPUT_DIR / "*_mlb_moneyline.csv"))), process_moneyline),
            ("run_line", sorted(glob.glob(str(INPUT_DIR / "*_mlb_run_line.csv"))), process_run_line),
            ("total", sorted(glob.glob(str(INPUT_DIR / "*_mlb_total.csv"))), process_total),
        ]
        for market, files, processor in groups:
            log(f"{market} files: {len(files)}")
            for file_path in files:
                try:
                    processor(file_path, summary)
                except ValueError as e:
                    log(f"SCHEMA/CONTRACT ERROR {market} {file_path}: {e}\n{traceback.format_exc()}")
                    summary["schema_errors"] += 1
                except Exception as e:
                    log(f"ERROR {market} {file_path}: {e}\n{traceback.format_exc()}")
                    summary["errors"] += 1

        status = "SUCCESS" if summary["errors"] == 0 and summary["schema_errors"] == 0 else "COMPLETED WITH ERRORS"
        log("--- SUMMARY ---")
        for key, value in summary.items():
            log(f"{key}={value}")
        log(f"STATUS: {status}")

        if summary["errors"] > 0 or summary["schema_errors"] > 0:
            print(
                f"build_juice_files completed with errors. errors={summary['errors']} "
                f"schema_errors={summary['schema_errors']}"
            )
            sys.exit(1)

        print(
            f"build_juice_files complete. files_written={summary['files_written']} "
            f"rows_written={summary['rows_written']} schema_errors={summary['schema_errors']} "
            f"errors={summary['errors']}"
        )
    except Exception as e:
        log(f"FATAL ERROR: {e}\n{traceback.format_exc()}")
        log("STATUS: FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()