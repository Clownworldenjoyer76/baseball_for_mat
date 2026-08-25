#!/usr/bin/env python3
"""Leakage-safe historical replay of the rebuilt MLB model/pipeline.

Reads existing historical training features, sportsbook prices, current selector
config, and historical final scores. For each target date it retrains the rebuilt
home/away run models using ONLY dates before that target date, then applies the
current probability/EV/Kelly math, grades every candidate, replays the current
selector rules, and writes one inspectable HTML report at the repository root.

This script does not overwrite production models or pipeline outputs.

Output:
    <repo>/mlb_new_pipeline_backtest_report.html
"""

from __future__ import annotations

import html
import importlib.util
import math
import sys
import traceback
import warnings
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr


def repo_root() -> Path:
    for start in (Path.cwd().resolve(), Path(__file__).resolve().parent):
        for p in (start, *start.parents):
            if (p / "docs/win/baseball/mlb").exists():
                return p
    raise RuntimeError("Could not locate repository root")


ROOT = repo_root()
BASE = ROOT / "docs/win/baseball/mlb"
TRAINING_FILE = BASE / "modeling/data/mlb_run_training_set.csv"
SPORTSBOOK_DIR = BASE / "00_intake/sportsbook"
CONTEXT_DIR = BASE / "00_intake/mlb_raw"
REPORT_FILE = ROOT / "mlb_new_pipeline_backtest_report.html"


def load_module(name: str, path: Path):
    if not path.exists():
        raise FileNotFoundError(path)

    spec = importlib.util.spec_from_file_location(name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {path}")

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TRAIN = load_module(
    "mlb_backtest_train",
    BASE / "scripts/modeling/train_run_model.py",
)

JUICE = load_module(
    "mlb_backtest_juice",
    BASE / "scripts/01_merge/build_juice_files.py",
)

SELECT = load_module(
    "mlb_backtest_select",
    BASE / "scripts/04_select/baseball_select_bets.py",
)

# Do not write backtest training chatter into the production modeling log.
TRAIN._log = lambda *args, **kwargs: None

PROB_TOL = 1e-9

METRICS = [
    "model_prob",
    "probability_edge",
    "ev",
    "kelly",
    "projection_edge_runs",
]


def f(value):
    try:
        if value is None or pd.isna(value):
            return None

        value = float(value)

        return value if np.isfinite(value) else None

    except (TypeError, ValueError):
        return None


def gamepk(value):
    try:
        if value is None or pd.isna(value):
            return None

        return str(int(float(value)))

    except (TypeError, ValueError):
        text = str(value).strip()
        return text or None


def american_to_decimal(value):
    return JUICE.american_to_decimal(value)


def binary_ev(p, d):
    return p * d - 1.0


def binary_kelly_raw(p, d):
    b = d - 1.0
    return ((b * p) - (1.0 - p)) / b


def total_ev(p_win, p_loss, d):
    return p_win * (d - 1.0) - p_loss


def total_kelly_raw(p_win, p_loss, d):
    b = d - 1.0
    resolved = p_win + p_loss

    if d <= 1.0 or resolved <= 0:
        raise ValueError("invalid totals Kelly inputs")

    return ((p_win * b) - p_loss) / (b * resolved)


def probability_edge(p_win, p_loss, d):
    resolved = p_win + p_loss

    if resolved <= 0:
        raise ValueError("invalid resolved probability mass")

    return p_win / resolved - 1.0 / d


def units(result, decimal_odds):
    if result == "Win":
        return decimal_odds - 1.0

    if result == "Loss":
        return -1.0

    return 0.0


def date_series(series):
    return pd.to_datetime(
        series.astype("string").str.replace("_", "-", regex=False),
        errors="coerce",
    ).dt.normalize()


def sportsbook_files():
    out = {}

    for path in sorted(SPORTSBOOK_DIR.glob("*_MLB.csv")):
        dt = pd.to_datetime(
            path.stem.replace("_MLB", "").replace("_", "-"),
            errors="coerce",
        )

        if pd.isna(dt):
            continue

        dt = pd.Timestamp(dt).normalize()

        if dt in out:
            raise RuntimeError(
                f"duplicate sportsbook date: {dt.date()}"
            )

        out[dt] = path

    return out


def load_training():
    df = TRAIN.load_training_set(TRAINING_FILE)

    features = TRAIN.determine_feature_columns(df)

    forbidden = [
        c
        for c in features
        if any(
            x in c.lower()
            for x in (
                "odds",
                "moneyline",
                "run_line",
                "dk_",
            )
        )
    ]

    if forbidden:
        raise RuntimeError(
            "sportsbook columns entered model features: "
            f"{forbidden}"
        )

    df = TRAIN.coerce_and_validate_training_data(
        df,
        features,
    )

    df["game_id"] = (
        df["game_id"]
        .astype("string")
        .str.strip()
    )

    df["_gamePk"] = df["gamePk"].map(gamepk)

    if df["game_id"].duplicated().any():
        raise RuntimeError(
            "training set contains duplicate game_id"
        )

    return (
        df.sort_values(
            [
                "_game_date_dt",
                "game_id",
            ]
        ).reset_index(drop=True),
        features,
    )


def load_sportsbook(path):
    df = pd.read_csv(
        path,
        encoding="utf-8-sig",
    )

    required = [
        "game_id",
        "home_team",
        "away_team",
        "home_run_line",
        "away_run_line",
        "total",
        "home_dk_moneyline_american",
        "away_dk_moneyline_american",
        "home_dk_run_line_american",
        "away_dk_run_line_american",
        "dk_total_over_american",
        "dk_total_under_american",
    ]

    missing = [
        c
        for c in required
        if c not in df.columns
    ]

    if missing:
        raise RuntimeError(
            f"{path.name} missing columns: {missing}"
        )

    df["game_id"] = (
        df["game_id"]
        .astype("string")
        .str.strip()
    )

    if df["game_id"].duplicated().any():
        raise RuntimeError(
            f"{path.name} duplicate game_id"
        )

    for c in required[3:]:
        df[c] = pd.to_numeric(
            df[c],
            errors="coerce",
        )

    return df


def load_context(target_date):
    path = (
        CONTEXT_DIR
        / f"{target_date.strftime('%Y_%m_%d')}_game_context.csv"
    )

    if not path.exists():
        return {}, "missing"

    df = pd.read_csv(
        path,
        encoding="utf-8-sig",
    )

    if df.empty or "gamePk" not in df.columns:
        return {}, "empty_or_missing_gamePk"

    df["_gamePk"] = df["gamePk"].map(gamepk)

    nonblank = df["_gamePk"].notna()

    if (
        df.loc[
            nonblank,
            "_gamePk",
        ]
        .duplicated()
        .any()
    ):
        raise RuntimeError(
            f"{path.name} duplicate gamePk"
        )

    return {
        row["_gamePk"]: row.to_dict()
        for _, row in df.loc[nonblank].iterrows()
    }, "ok"


def train_models(
    training,
    target_date,
    features,
):
    prior = training[
        training["_game_date_dt"] < target_date
    ].copy()

    split = TRAIN.chronological_date_split(prior)

    hp, hval = TRAIN.select_hyperparameters(
        split["train"],
        split["validation"],
        features,
        "target_home_runs",
        "home_runs",
    )

    ap, aval = TRAIN.select_hyperparameters(
        split["train"],
        split["validation"],
        features,
        "target_away_runs",
        "away_runs",
    )

    hm = TRAIN.fit_final_model(
        split["train"],
        split["validation"],
        features,
        "target_home_runs",
        hp,
    )

    am = TRAIN.fit_final_model(
        split["train"],
        split["validation"],
        features,
        "target_away_runs",
        ap,
    )

    def rng(frame):
        return (
            f"{frame['_game_date_dt'].min().date()}.."
            f"{frame['_game_date_dt'].max().date()}"
        )

    return hm, am, {
        "prior_rows": len(prior),
        "prior_dates": prior[
            "_game_date_dt"
        ].nunique(),
        "train_rows": len(split["train"]),
        "validation_rows": len(
            split["validation"]
        ),
        "test_rows": len(split["test"]),
        "train_dates": rng(split["train"]),
        "validation_dates": rng(
            split["validation"]
        ),
        "test_dates": rng(split["test"]),
        "home_validation_poisson": hval,
        "away_validation_poisson": aval,
        "home_params": str(hp),
        "away_params": str(ap),
    }


def context_allowed(
    context_row,
    market,
):
    failure = SELECT.context_data_exclusion_reason(
        None
        if context_row is None
        else pd.Series(context_row)
    )

    if failure:
        return False, "context_data_excluded"

    rain_failure = SELECT.rain_exclusion_reason(
        None
        if context_row is None
        else pd.Series(context_row)
    )

    if rain_failure:
        return False, "rain_excluded"

    if (
        market == "total"
        and SELECT.sp_sample_excluded_for_total(
            None
            if context_row is None
            else pd.Series(context_row)
        )
    ):
        return False, "sp_sample_excluded"

    return True, ""


def grade_moneyline(
    side,
    hs,
    aws,
):
    if hs == aws:
        return "Push"

    home_win = hs > aws

    return (
        "Win"
        if (side == "home") == home_win
        else "Loss"
    )


def grade_run_line(
    side,
    line,
    hs,
    aws,
):
    margin = (
        hs + line - aws
        if side == "home"
        else aws + line - hs
    )

    if abs(margin) <= 1e-12:
        return "Push"

    return (
        "Win"
        if margin > 0
        else "Loss"
    )


def grade_total(
    side,
    line,
    hs,
    aws,
):
    margin = hs + aws - line

    if abs(margin) <= 1e-12:
        return "Push"

    return (
        "Win"
        if (margin > 0) == (side == "over")
        else "Loss"
    )


def candidate(
    row,
    market,
    side,
    line,
    american,
    p_win,
    p_loss,
    p_push,
    ev,
    kr,
    proj_edge,
    result,
    context_pass,
    context_reason,
):
    d = american_to_decimal(american)
    k = max(0.0, kr)

    return {
        "game_date": row[
            "_game_date_dt"
        ].strftime("%Y-%m-%d"),
        "game_id": row["game_id"],
        "gamePk": row.get("gamePk"),
        "home_team": row.get(
            "home_team_train"
        ),
        "away_team": row.get(
            "away_team_train"
        ),
        "market_type": market,
        "side": side,
        "line": line,
        "american_odds": american,
        "decimal_odds": d,
        "model_home_runs": row[
            "model_home_runs"
        ],
        "model_away_runs": row[
            "model_away_runs"
        ],
        "model_total_runs": row[
            "model_total_runs"
        ],
        "dratings_home_projected_runs": row.get(
            "dratings_home_projected_runs"
        ),
        "dratings_away_projected_runs": row.get(
            "dratings_away_projected_runs"
        ),
        "dratings_total_projected_runs": row.get(
            "dratings_total_projected_runs"
        ),
        "model_prob": p_win,
        "p_loss": p_loss,
        "p_push": p_push,
        "probability_edge": probability_edge(
            p_win,
            p_loss,
            d,
        ),
        "ev": ev,
        "kelly_raw": kr,
        "kelly": k,
        "projection_edge_runs": proj_edge,
        "final_home_score": row[
            "target_home_runs"
        ],
        "final_away_score": row[
            "target_away_runs"
        ],
        "final_total": (
            row["target_home_runs"]
            + row["target_away_runs"]
        ),
        "bet_result": result,
        "win_binary": (
            1.0
            if result == "Win"
            else (
                0.0
                if result == "Loss"
                else np.nan
            )
        ),
        "bet_units": units(
            result,
            d,
        ),
        "context_pass": context_pass,
        "selector_rule_pass": False,
        "selected_current_config": False,
        "selector_rejection_reason": (
            context_reason
            or "not_evaluated"
        ),
    }


def candidates_for_game(
    row,
    context_row,
):
    out = []
    errors = []

    mh = float(row["model_home_runs"])
    ma = float(row["model_away_runs"])

    hs = float(row["target_home_runs"])
    aws = float(row["target_away_runs"])

    # MONEYLINE
    try:
        hp, ap, _ = (
            JUICE.moneyline_probabilities(
                mh,
                ma,
            )
        )

        for side, p, odds_col in (
            (
                "home",
                hp,
                "home_dk_moneyline_american",
            ),
            (
                "away",
                ap,
                "away_dk_moneyline_american",
            ),
        ):
            american = f(
                row.get(odds_col)
            )

            d = american_to_decimal(
                american
            )

            if (
                american is None
                or d is None
                or d <= 1
            ):
                raise ValueError(
                    f"moneyline {side} "
                    "invalid odds"
                )

            ctx, reason = context_allowed(
                context_row,
                "moneyline",
            )

            edge = (
                mh - ma
                if side == "home"
                else ma - mh
            )

            result = grade_moneyline(
                side,
                hs,
                aws,
            )

            out.append(
                candidate(
                    row,
                    "moneyline",
                    side,
                    None,
                    american,
                    p,
                    1 - p,
                    0.0,
                    binary_ev(p, d),
                    binary_kelly_raw(
                        p,
                        d,
                    ),
                    edge,
                    result,
                    ctx,
                    reason,
                )
            )

    except Exception as exc:
        errors.append(
            f"{row['game_id']} "
            f"moneyline: {exc}"
        )

    # RUN LINE
    try:
        hl = f(
            row.get("home_run_line")
        )

        al = f(
            row.get("away_run_line")
        )

        if hl is None or al is None:
            raise ValueError(
                "missing run line"
            )

        hp, ap = (
            JUICE.run_line_probabilities(
                mh,
                ma,
                hl,
                al,
            )
        )

        for (
            side,
            p,
            line,
            odds_col,
        ) in (
            (
                "home",
                hp,
                hl,
                "home_dk_run_line_american",
            ),
            (
                "away",
                ap,
                al,
                "away_dk_run_line_american",
            ),
        ):
            american = f(
                row.get(odds_col)
            )

            d = american_to_decimal(
                american
            )

            if (
                american is None
                or d is None
                or d <= 1
            ):
                raise ValueError(
                    f"run_line {side} "
                    "invalid odds"
                )

            ctx, reason = context_allowed(
                context_row,
                "run_line",
            )

            edge = (
                mh + line - ma
                if side == "home"
                else ma + line - mh
            )

            result = grade_run_line(
                side,
                line,
                hs,
                aws,
            )

            out.append(
                candidate(
                    row,
                    "run_line",
                    side,
                    line,
                    american,
                    p,
                    1 - p,
                    0.0,
                    binary_ev(p, d),
                    binary_kelly_raw(
                        p,
                        d,
                    ),
                    edge,
                    result,
                    ctx,
                    reason,
                )
            )

    except Exception as exc:
        errors.append(
            f"{row['game_id']} "
            f"run_line: {exc}"
        )

    # TOTAL
    try:
        line = f(
            row.get("total")
        )

        if line is None:
            raise ValueError(
                "missing total"
            )

        po, pu, pp = (
            JUICE.totals_probabilities(
                mh,
                ma,
                line,
            )
        )

        for (
            side,
            pw,
            pl,
            odds_col,
        ) in (
            (
                "over",
                po,
                pu,
                "dk_total_over_american",
            ),
            (
                "under",
                pu,
                po,
                "dk_total_under_american",
            ),
        ):
            american = f(
                row.get(odds_col)
            )

            d = american_to_decimal(
                american
            )

            if (
                american is None
                or d is None
                or d <= 1
            ):
                raise ValueError(
                    f"total {side} invalid odds"
                )

            ctx, reason = context_allowed(
                context_row,
                "total",
            )

            edge = (
                mh + ma - line
                if side == "over"
                else line - (mh + ma)
            )

            result = grade_total(
                side,
                line,
                hs,
                aws,
            )

            out.append(
                candidate(
                    row,
                    "total",
                    side,
                    line,
                    american,
                    pw,
                    pl,
                    pp,
                    total_ev(
                        pw,
                        pl,
                        d,
                    ),
                    total_kelly_raw(
                        pw,
                        pl,
                        d,
                    ),
                    edge,
                    result,
                    ctx,
                    reason,
                )
            )

    except Exception as exc:
        errors.append(
            f"{row['game_id']} "
            f"total: {exc}"
        )

    return out, errors


def apply_selector(df):
    df = df.copy()

    passes = []
    reasons = []

    for _, r in df.iterrows():
        market = r["market_type"]
        side = r["side"]

        if not r["context_pass"]:
            passes.append(False)
            reasons.append(
                r[
                    "selector_rejection_reason"
                ]
                or "context_excluded"
            )
            continue

        if not SELECT.CONFIG.get(
            market,
            {},
        ).get(
            "enabled",
            True,
        ):
            passes.append(False)
            reasons.append(
                "market_disabled"
            )
            continue

        rules = SELECT.CONFIG[
            market
        ][side]

        if not rules.get(
            "enabled",
            True,
        ):
            passes.append(False)
            reasons.append(
                "side_disabled"
            )
            continue

        # Exact current run-line hard gates.
        if (
            market == "run_line"
            and r["ev"] <= 0
        ):
            passes.append(False)
            reasons.append("ev<=0")
            continue

        if (
            market == "run_line"
            and r["kelly"] <= 0
        ):
            passes.append(False)
            reasons.append("kelly<=0")
            continue

        counters = (
            SELECT.init_counter()
        )

        ok, reason, detail = (
            SELECT.check_rules(
                r["ev"],
                r["kelly"],
                r["american_odds"],
                r["line"],
                r["model_prob"],
                rules,
                counters,
            )
        )

        passes.append(ok)

        reasons.append(
            ""
            if ok
            else f"{reason}:{detail}"
        )

    df[
        "selector_rule_pass"
    ] = passes

    df[
        "selector_rejection_reason"
    ] = reasons

    df[
        "selected_current_config"
    ] = False

    for (
        dt,
        gid,
        market,
    ), g in df.groupby(
        [
            "game_date",
            "game_id",
            "market_type",
        ],
        sort=False,
    ):
        eligible = g[
            g["selector_rule_pass"]
        ]

        if eligible.empty:
            continue

        records = eligible.to_dict(
            "records"
        )

        selected = (
            SELECT.select_candidate(
                records,
                SELECT.CONFIG[
                    market
                ].get(
                    "pick_preference",
                    "best_ev",
                ),
                market,
                gid,
            )
        )

        for chosen in selected:
            mask = (
                (
                    df["game_date"]
                    == dt
                )
                & (
                    df["game_id"]
                    == gid
                )
                & (
                    df["market_type"]
                    == market
                )
                & (
                    df["side"]
                    == chosen["side"]
                )
            )

            df.loc[
                mask,
                "selected_current_config",
            ] = True

    return df


def scopes(df):
    return {
        "all_candidates": df,
        "positive_ev_positive_kelly": df[
            (df["ev"] > 0)
            & (df["kelly"] > 0)
        ],
        "current_selector_selected": df[
            df[
                "selected_current_config"
            ]
        ],
    }


def corr_values(
    x,
    y,
):
    z = pd.DataFrame(
        {
            "x": pd.to_numeric(
                x,
                errors="coerce",
            ),
            "y": y,
        }
    ).dropna()

    if (
        len(z) < 3
        or z["x"].nunique() < 2
        or z["y"].nunique() < 2
    ):
        return (
            None,
            None,
            None,
            None,
        )

    with warnings.catch_warnings():
        warnings.simplefilter(
            "ignore"
        )

        pr = pearsonr(
            z["x"],
            z["y"],
        )

        sr = spearmanr(
            z["x"],
            z["y"],
        )

    return (
        float(pr.statistic),
        float(pr.pvalue),
        float(sr.statistic),
        float(sr.pvalue),
    )


def correlation_table(df):
    rows = []

    for scope, sdf in scopes(
        df
    ).items():
        for market in (
            "ALL",
            "moneyline",
            "run_line",
            "total",
        ):
            sub = (
                sdf
                if market == "ALL"
                else sdf[
                    sdf[
                        "market_type"
                    ]
                    == market
                ]
            )

            sub = sub[
                sub[
                    "bet_result"
                ].isin(
                    [
                        "Win",
                        "Loss",
                    ]
                )
            ]

            for metric in METRICS:
                z = sub[
                    [
                        metric,
                        "win_binary",
                    ]
                ].dropna()

                if z.empty:
                    continue

                (
                    pr,
                    pp,
                    sr,
                    sp,
                ) = corr_values(
                    z[metric],
                    z["win_binary"],
                )

                rows.append(
                    {
                        "scope": scope,
                        "market_type": market,
                        "metric": metric,
                        "n": len(z),
                        "wins": int(
                            z[
                                "win_binary"
                            ].sum()
                        ),
                        "win_pct": z[
                            "win_binary"
                        ].mean(),
                        "metric_mean": z[
                            metric
                        ].mean(),
                        "pearson_r": pr,
                        "pearson_p": pp,
                        "spearman_r": sr,
                        "spearman_p": sp,
                    }
                )

    return pd.DataFrame(rows)


def bucket_table(df):
    rows = []

    for scope, sdf in scopes(
        df
    ).items():
        for market in (
            "moneyline",
            "run_line",
            "total",
        ):
            mdf = sdf[
                (
                    sdf[
                        "market_type"
                    ]
                    == market
                )
                & sdf[
                    "bet_result"
                ].isin(
                    [
                        "Win",
                        "Loss",
                    ]
                )
            ]

            for metric in METRICS:
                z = mdf[
                    [
                        metric,
                        "win_binary",
                        "bet_units",
                    ]
                ].dropna().copy()

                if (
                    len(z) < 4
                    or z[
                        metric
                    ].nunique()
                    < 2
                ):
                    continue

                try:
                    z["bucket"] = pd.qcut(
                        z[metric],
                        q=min(
                            5,
                            z[
                                metric
                            ].nunique(),
                        ),
                        duplicates="drop",
                    )

                except ValueError:
                    continue

                for (
                    order,
                    (
                        bucket,
                        g,
                    ),
                ) in enumerate(
                    z.groupby(
                        "bucket",
                        observed=True,
                        sort=True,
                    ),
                    1,
                ):
                    rows.append(
                        {
                            "scope": scope,
                            "market_type": market,
                            "metric": metric,
                            "bucket_order": order,
                            "bucket": str(
                                bucket
                            ),
                            "n": len(g),
                            "metric_min": g[
                                metric
                            ].min(),
                            "metric_mean": g[
                                metric
                            ].mean(),
                            "metric_max": g[
                                metric
                            ].max(),
                            "wins": int(
                                g[
                                    "win_binary"
                                ].sum()
                            ),
                            "win_pct": g[
                                "win_binary"
                            ].mean(),
                            "units": g[
                                "bet_units"
                            ].sum(),
                            "roi_per_bet": g[
                                "bet_units"
                            ].mean(),
                        }
                    )

    return pd.DataFrame(rows)


def summary_table(df):
    rows = []

    for scope, sdf in scopes(
        df
    ).items():
        for market in (
            "ALL",
            "moneyline",
            "run_line",
            "total",
        ):
            sub = (
                sdf
                if market == "ALL"
                else sdf[
                    sdf[
                        "market_type"
                    ]
                    == market
                ]
            )

            if sub.empty:
                continue

            resolved = sub[
                sub[
                    "bet_result"
                ].isin(
                    [
                        "Win",
                        "Loss",
                    ]
                )
            ]

            rows.append(
                {
                    "scope": scope,
                    "market_type": market,
                    "rows": len(sub),
                    "wins": int(
                        (
                            resolved[
                                "bet_result"
                            ]
                            == "Win"
                        ).sum()
                    ),
                    "losses": int(
                        (
                            resolved[
                                "bet_result"
                            ]
                            == "Loss"
                        ).sum()
                    ),
                    "pushes": int(
                        (
                            sub[
                                "bet_result"
                            ]
                            == "Push"
                        ).sum()
                    ),
                    "win_pct": (
                        (
                            resolved[
                                "bet_result"
                            ]
                            == "Win"
                        ).mean()
                        if len(resolved)
                        else np.nan
                    ),
                    "units": sub[
                        "bet_units"
                    ].sum(),
                    "roi_per_bet": sub[
                        "bet_units"
                    ].mean(),
                    "avg_model_prob": sub[
                        "model_prob"
                    ].mean(),
                    "avg_ev": sub[
                        "ev"
                    ].mean(),
                    "avg_kelly": sub[
                        "kelly"
                    ].mean(),
                }
            )

    return pd.DataFrame(rows)


def pretty(
    df,
    pct=(),
):
    out = df.copy()

    for c in out.columns:
        if c in pct:
            out[c] = out[c].map(
                lambda x: (
                    ""
                    if pd.isna(x)
                    else (
                        f"{100 * float(x):.2f}%"
                    )
                )
            )

        elif pd.api.types.is_float_dtype(
            out[c]
        ):
            out[c] = out[c].map(
                lambda x: (
                    ""
                    if pd.isna(x)
                    else f"{float(x):.6f}"
                )
            )

    return out


def table(
    df,
    pct=(),
):
    if df.empty:
        return (
            "<p><em>No rows."
            "</em></p>"
        )

    return pretty(
        df,
        pct,
    ).to_html(
        index=False,
        border=0,
        classes="dataframe",
        escape=True,
    )


def write_report(
    df,
    correlations,
    buckets,
    summary,
    coverage,
    skipped,
    errors,
    features,
):
    details = [
        "game_date",
        "game_id",
        "home_team",
        "away_team",
        "market_type",
        "side",
        "line",
        "american_odds",
        "model_home_runs",
        "model_away_runs",
        "model_total_runs",
        "model_prob",
        "probability_edge",
        "ev",
        "kelly_raw",
        "kelly",
        "projection_edge_runs",
        "final_home_score",
        "final_away_score",
        "bet_result",
        "bet_units",
        "context_pass",
        "selector_rule_pass",
        "selected_current_config",
        "selector_rejection_reason",
    ]

    err_html = (
        "<p>None.</p>"
        if not errors
        else (
            "<ul>"
            + "".join(
                (
                    "<li>"
                    + html.escape(x)
                    + "</li>"
                )
                for x in errors
            )
            + "</ul>"
        )
    )

    css = """
body {
    font-family: Segoe UI, Arial, sans-serif;
    margin: 24px;
    color: #202124;
}
h1, h2 {
    color: #202124;
}
.meta {
    background: #f5f5f5;
    padding: 12px 16px;
    border-radius: 6px;
}
.note {
    padding: 10px 14px;
    background: #fff8e1;
    border-left: 4px solid #d6a100;
}
table.dataframe {
    border-collapse: collapse;
    width: 100%;
    margin: 10px 0 24px;
    font-size: 12px;
}
table.dataframe th,
table.dataframe td {
    border: 1px solid #ddd;
    padding: 6px 8px;
    white-space: nowrap;
    text-align: right;
}
table.dataframe th {
    background: #f2f2f2;
    position: sticky;
    top: 0;
}
.scroll {
    max-height: 700px;
    overflow: auto;
    border: 1px solid #ddd;
}
code {
    background: #f5f5f5;
    padding: 2px 4px;
}
"""

    corr_display = (
        correlations.sort_values(
            [
                "scope",
                "market_type",
                "metric",
            ]
        )
        if not correlations.empty
        else correlations
    )

    bucket_display = (
        buckets.sort_values(
            [
                "scope",
                "market_type",
                "metric",
                "bucket_order",
            ]
        )
        if not buckets.empty
        else buckets
    )

    details_display = df[
        details
    ].sort_values(
        [
            "game_date",
            "game_id",
            "market_type",
            "side",
        ]
    )

    REPORT_FILE.write_text(
        f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>MLB rebuilt pipeline backtest</title>
<style>{css}</style>
</head>
<body>

<h1>MLB Rebuilt Pipeline — Leakage-Safe Historical Backtest</h1>

<div class="meta">
<p><strong>Generated:</strong> {datetime.now(UTC).isoformat()}</p>
<p><strong>Candidate rows:</strong> {len(df):,}</p>
<p><strong>Current-config selected rows:</strong> {int(df["selected_current_config"].sum()):,}</p>
<p><strong>Backtested dates:</strong> {len(coverage):,}</p>
<p><strong>Skipped dates:</strong> {len(skipped):,}</p>
</div>

<div class="note">
<strong>Leakage rule:</strong>
each target date is predicted using models trained only from dates strictly before it.
The current production 70/15/15 chronological split, 81-point hyperparameter search,
train+validation refit, market probability math, EV/Kelly math, and selector rules
are replayed.
</div>

<h2>Model features</h2>
<p><code>{html.escape(", ".join(features))}</code></p>

<h2>Scope / market summary</h2>
{table(
    summary,
    {
        "win_pct",
        "roi_per_bet",
        "avg_model_prob",
        "avg_ev",
        "avg_kelly",
    },
)}

<h2>Correlation with realized win/loss</h2>

<p>
Pushes are excluded. Pearson is linear association; Spearman is monotonic
association. P-values are descriptive because opposite sides from the same
game are not independent observations in the all-candidates scopes.
</p>

{table(
    corr_display,
    {"win_pct"},
)}

<h2>Equal-frequency win-rate buckets</h2>

{table(
    bucket_display,
    {
        "win_pct",
        "roi_per_bet",
    },
)}

<h2>Date coverage</h2>

{table(coverage)}

<h2>Skipped dates</h2>

{table(skipped)}

<h2>Candidate-generation warnings</h2>

{err_html}

<h2>All candidate-level results</h2>

<div class="scroll">

{table(
    details_display,
    {
        "model_prob",
        "probability_edge",
        "ev",
        "kelly_raw",
        "kelly",
    },
)}

</div>

</body>
</html>
""",
        encoding="utf-8",
    )


def main():
    training, features = load_training()

    books = sportsbook_files()

    dates = sorted(
        set(
            training[
                "_game_date_dt"
            ]
        ).intersection(
            books
        )
    )

    if not dates:
        raise RuntimeError(
            "no overlapping "
            "training/sportsbook dates"
        )

    rows = []
    coverage = []
    skipped = []
    errors = []

    for i, target_date in enumerate(
        dates,
        1,
    ):
        label = target_date.strftime(
            "%Y-%m-%d"
        )

        print(
            f"[{i}/{len(dates)}] "
            f"{label}"
        )

        prior = training[
            training[
                "_game_date_dt"
            ]
            < target_date
        ]

        if (
            prior[
                "_game_date_dt"
            ].nunique()
            < 3
        ):
            skipped.append(
                {
                    "game_date": label,
                    "reason": (
                        "fewer_than_3_"
                        "prior_training_dates"
                    ),
                    "prior_rows": len(
                        prior
                    ),
                }
            )
            continue

        target = training[
            training[
                "_game_date_dt"
            ]
            == target_date
        ].copy()

        try:
            (
                hm,
                am,
                meta,
            ) = train_models(
                training,
                target_date,
                features,
            )

            target[
                "model_home_runs"
            ] = hm.predict(
                target[features]
            )

            target[
                "model_away_runs"
            ] = am.predict(
                target[features]
            )

            target[
                "model_total_runs"
            ] = (
                target[
                    "model_home_runs"
                ]
                + target[
                    "model_away_runs"
                ]
            )

            for c in (
                "model_home_runs",
                "model_away_runs",
                "model_total_runs",
            ):
                bad = (
                    ~np.isfinite(
                        target[c]
                    )
                    | (
                        target[c]
                        < 0
                    )
                )

                if bad.any():
                    raise RuntimeError(
                        f"invalid {c} "
                        "predictions"
                    )

            book = load_sportsbook(
                books[target_date]
            )

            joined = target.merge(
                book,
                on="game_id",
                how="inner",
                suffixes=(
                    "_train",
                    "_book",
                ),
                validate="one_to_one",
            )

            (
                context,
                context_status,
            ) = load_context(
                target_date
            )

            rows_before = len(rows)

            for _, r in joined.iterrows():
                ctx = context.get(
                    gamepk(
                        r.get(
                            "gamePk"
                        )
                    )
                )

                (
                    game_rows,
                    game_errors,
                ) = candidates_for_game(
                    r,
                    ctx,
                )

                rows.extend(
                    game_rows
                )

                errors.extend(
                    game_errors
                )

            coverage.append(
                {
                    "game_date": label,
                    "target_games": len(
                        target
                    ),
                    "sportsbook_games": len(
                        book
                    ),
                    "joined_games": len(
                        joined
                    ),
                    "unmatched_training_games": (
                        len(target)
                        - len(joined)
                    ),
                    "unmatched_sportsbook_games": (
                        len(book)
                        - len(joined)
                    ),
                    "candidate_rows_added": (
                        len(rows)
                        - rows_before
                    ),
                    "prior_rows": meta[
                        "prior_rows"
                    ],
                    "prior_dates": meta[
                        "prior_dates"
                    ],
                    "train_rows": meta[
                        "train_rows"
                    ],
                    "validation_rows": meta[
                        "validation_rows"
                    ],
                    "test_rows": meta[
                        "test_rows"
                    ],
                    "train_dates": meta[
                        "train_dates"
                    ],
                    "validation_dates": meta[
                        "validation_dates"
                    ],
                    "test_dates": meta[
                        "test_dates"
                    ],
                    "home_validation_poisson": meta[
                        "home_validation_poisson"
                    ],
                    "away_validation_poisson": meta[
                        "away_validation_poisson"
                    ],
                    "home_params": meta[
                        "home_params"
                    ],
                    "away_params": meta[
                        "away_params"
                    ],
                    "context_file_status": context_status,
                }
            )

        except Exception as exc:
            print(
                f"  SKIPPED: {exc}"
            )

            skipped.append(
                {
                    "game_date": label,
                    "reason": (
                        f"backtest_error: "
                        f"{exc}"
                    ),
                    "prior_rows": len(
                        prior
                    ),
                }
            )

    if not rows:
        raise RuntimeError(
            "backtest produced "
            "no candidate rows"
        )

    df = apply_selector(
        pd.DataFrame(rows)
    )

    correlations = (
        correlation_table(df)
    )

    buckets = bucket_table(df)

    summary = summary_table(df)

    coverage_df = pd.DataFrame(
        coverage
    )

    skipped_df = pd.DataFrame(
        skipped
    )

    write_report(
        df,
        correlations,
        buckets,
        summary,
        coverage_df,
        skipped_df,
        errors,
        features,
    )

    print(
        "Backtest complete."
    )

    print(
        f"Candidate rows: "
        f"{len(df)}"
    )

    print(
        "Current-config selected rows: "
        f"{int(df['selected_current_config'].sum())}"
    )

    print(
        f"Backtested dates: "
        f"{len(coverage_df)}"
    )

    print(
        f"Skipped dates: "
        f"{len(skipped_df)}"
    )

    print(
        f"RESULT FILE: "
        f"{REPORT_FILE}"
    )


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        sys.exit(130)

    except Exception as exc:
        print(
            f"FAILED: {exc}"
        )

        print(
            traceback.format_exc()
        )

        sys.exit(1)