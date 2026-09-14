#!/usr/bin/env python3
"""
Item #11: evaluate MLB moneyline treatment of modeled regulation-score ties.

Compare:
1. current_conditioned:
   P(home wins) = P(H>A) / [P(H>A) + P(A>H)]
   This is the current production method: remove the Skellam tie mass and
   renormalize decisive outcomes.

2. explicit_extra_50_50:
   P(home wins) = P(H>A) + P(H=A) * 0.5

3. explicit_extra_empirical:
   P(home wins) = P(H>A) + P(H=A) * q_extra_home
   where q_extra_home is estimated strictly chronologically from completed
   MLB games tied after nine innings before the evaluation period. A
   Beta(1,1) prior is used:
       q = (home_extra_wins + 1) / (extra_games + 2)

The script uses the exact Poisson/Skellam OOS mean predictions already written
by item #6 and obtains actual regulation-through-nine linescores from MLB's
public StatsAPI. Linescores are cached locally.

Production is NOT modified.

Inputs:
- docs/win/baseball/mlb/modeling/count_distribution_backtest/game_probabilities.csv
- docs/win/baseball/mlb/modeling/data/mlb_run_training_set.csv

Outputs:
- docs/win/baseball/mlb/modeling/moneyline_extra_innings_backtest/
    moneyline_method_metrics.csv
    moneyline_period_parameters.csv
    moneyline_game_probabilities.csv
    linescore_cache.csv
    moneyline_extra_innings_summary.md
"""

from __future__ import annotations

import argparse
import json
import math
import time
import urllib.error
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import skellam

BASE_DIR = Path("docs/win/baseball/mlb")
DEFAULT_PROBS = (
    BASE_DIR
    / "modeling/count_distribution_backtest/game_probabilities.csv"
)
DEFAULT_TRAINING = (
    BASE_DIR
    / "modeling/data/mlb_run_training_set.csv"
)
DEFAULT_OUTPUT_DIR = (
    BASE_DIR
    / "modeling/moneyline_extra_innings_backtest"
)

MLB_LINESCORE_URL = (
    "https://statsapi.mlb.com/api/v1/game/{game_pk}/linescore"
)

EPS = 1e-12
PROB_TOLERANCE = 1e-9
BINS = np.linspace(0.0, 1.0, 11)
RNG_SEED = 42
BOOTSTRAP_DRAWS = 20000


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / BASE_DIR).is_dir():
                return candidate
    raise RuntimeError("Could not locate repository root")


def normalize_game_pk(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    try:
        return str(int(text))
    except Exception:
        return None


def normalize_game_id(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    return out.mask(out.eq(""))


def binary_log_loss(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.clip(np.asarray(p, dtype=float), EPS, 1.0 - EPS)
    valid = np.isfinite(y) & np.isfinite(p)
    y = y[valid]
    p = p[valid]
    if len(y) == 0:
        return float("nan")
    return float(
        np.mean(
            -(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
        )
    )


def ece(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    y = y[valid]
    p = np.clip(p[valid], 0.0, 1.0)
    if len(y) == 0:
        return float("nan")

    ids = np.digitize(
        p,
        BINS[1:-1],
        right=False,
    )
    total = len(y)
    value = 0.0
    for idx in range(10):
        mask = ids == idx
        if not np.any(mask):
            continue
        value += (
            np.sum(mask)
            / total
            * abs(
                float(np.mean(p[mask]))
                - float(np.mean(y[mask]))
            )
        )
    return float(value)


def brier(y, p) -> float:
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    valid = np.isfinite(y) & np.isfinite(p)
    if not np.any(valid):
        return float("nan")
    return float(
        np.mean(
            (p[valid] - y[valid]) ** 2
        )
    )


def load_poisson_oos(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {
        "period",
        "distribution",
        "game_date",
        "game_id",
        "actual_home_runs",
        "actual_away_runs",
        "mean_home_runs",
        "mean_away_runs",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"{path} missing required columns: {missing}"
        )

    frame = frame[
        frame["distribution"].eq("poisson_skellam")
    ].copy()
    if frame.empty:
        raise RuntimeError(
            "No poisson_skellam rows found in item #6 output"
        )

    frame["game_id"] = normalize_game_id(frame["game_id"])
    frame["game_date"] = pd.to_datetime(
        frame["game_date"],
        format="%Y-%m-%d",
        errors="raise",
    )

    for col in [
        "actual_home_runs",
        "actual_away_runs",
        "mean_home_runs",
        "mean_away_runs",
    ]:
        frame[col] = pd.to_numeric(
            frame[col],
            errors="coerce",
        )
        if (
            frame[col].isna().any()
            or (~np.isfinite(frame[col])).any()
        ):
            raise RuntimeError(
                f"Invalid OOS probability column: {col}"
            )

    duplicates = frame.duplicated(
        subset=["period", "game_id"],
        keep=False,
    )
    if duplicates.any():
        sample = frame.loc[
            duplicates,
            ["period", "game_id"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            f"Duplicate Poisson OOS games; sample={sample}"
        )

    return frame.reset_index(drop=True)


def load_training_games(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)

    required = {
        "game_date",
        "game_id",
        "gamePk",
        "target_home_runs",
        "target_away_runs",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"{path} missing required columns: {missing}"
        )

    out = frame[
        [
            "game_date",
            "game_id",
            "gamePk",
            "target_home_runs",
            "target_away_runs",
        ]
    ].copy()

    out["game_id"] = normalize_game_id(out["game_id"])
    out["gamePk"] = out["gamePk"].map(normalize_game_pk)
    out["game_date"] = pd.to_datetime(
        out["game_date"],
        errors="coerce",
    )
    out["target_home_runs"] = pd.to_numeric(
        out["target_home_runs"],
        errors="coerce",
    )
    out["target_away_runs"] = pd.to_numeric(
        out["target_away_runs"],
        errors="coerce",
    )

    out = out.dropna(
        subset=[
            "game_date",
            "game_id",
            "gamePk",
            "target_home_runs",
            "target_away_runs",
        ]
    ).copy()

    duplicates = out["game_id"].duplicated(keep=False)
    if duplicates.any():
        sample = out.loc[
            duplicates,
            ["game_id", "gamePk", "game_date"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            f"Training set has duplicate game_id; sample={sample}"
        )

    return out.sort_values(
        ["game_date", "game_id"]
    ).reset_index(drop=True)


def attach_game_pk(
    oos: pd.DataFrame,
    training: pd.DataFrame,
) -> pd.DataFrame:
    lookup = training[
        ["game_id", "gamePk"]
    ].drop_duplicates()

    out = oos.merge(
        lookup,
        on="game_id",
        how="left",
        validate="many_to_one",
    )

    missing = out["gamePk"].isna()
    if missing.any():
        sample = out.loc[
            missing,
            ["game_id", "game_date", "period"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            "OOS games missing gamePk in training set; "
            f"sample={sample}"
        )
    return out


def parse_linescore_payload(
    game_pk: str,
    payload: dict,
) -> dict:
    innings = payload.get("innings")
    if not isinstance(innings, list) or not innings:
        raise RuntimeError(
            f"gamePk={game_pk}: linescore has no innings"
        )

    home_9 = 0
    away_9 = 0
    max_inning = 0

    for inning in innings:
        try:
            num = int(inning.get("num"))
        except Exception:
            continue

        max_inning = max(max_inning, num)

        if num <= 9:
            home = inning.get("home") or {}
            away = inning.get("away") or {}
            home_runs = home.get("runs")
            away_runs = away.get("runs")

            # A missing bottom-half run value can occur when the bottom half
            # was not played. That contributes zero runs.
            home_9 += (
                int(home_runs)
                if home_runs is not None
                else 0
            )
            away_9 += (
                int(away_runs)
                if away_runs is not None
                else 0
            )

    teams = payload.get("teams") or {}
    home_total = (
        (teams.get("home") or {}).get("runs")
    )
    away_total = (
        (teams.get("away") or {}).get("runs")
    )

    if home_total is None or away_total is None:
        # Fallback: sum all innings.
        home_total = 0
        away_total = 0
        for inning in innings:
            home = inning.get("home") or {}
            away = inning.get("away") or {}
            hr = home.get("runs")
            ar = away.get("runs")
            home_total += int(hr) if hr is not None else 0
            away_total += int(ar) if ar is not None else 0

    home_total = int(home_total)
    away_total = int(away_total)

    went_extra = bool(max_inning > 9)
    tied_after_9 = bool(home_9 == away_9)

    if went_extra and not tied_after_9:
        raise RuntimeError(
            f"gamePk={game_pk}: game went extras but first-nine "
            f"score is {away_9}-{home_9}"
        )

    if home_total == away_total:
        raise RuntimeError(
            f"gamePk={game_pk}: completed MLB game ended tied"
        )

    return {
        "gamePk": str(game_pk),
        "regulation_home_runs": int(home_9),
        "regulation_away_runs": int(away_9),
        "tied_after_9": int(tied_after_9),
        "went_extra": int(went_extra),
        "final_home_runs_api": int(home_total),
        "final_away_runs_api": int(away_total),
        "home_final_win_api": int(home_total > away_total),
    }


def fetch_linescore(
    game_pk: str,
    *,
    timeout: float = 30.0,
    retries: int = 4,
) -> dict:
    url = MLB_LINESCORE_URL.format(
        game_pk=game_pk
    )

    last_error = None
    for attempt in range(retries):
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": (
                        "baseball_for_mat-item11/1.0"
                    )
                },
            )
            with urllib.request.urlopen(
                request,
                timeout=timeout,
            ) as response:
                payload = json.loads(
                    response.read().decode("utf-8")
                )
            return parse_linescore_payload(
                game_pk,
                payload,
            )
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            TimeoutError,
            json.JSONDecodeError,
            RuntimeError,
        ) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(0.5 * (attempt + 1))

    raise RuntimeError(
        f"Failed to fetch linescore for gamePk={game_pk}: "
        f"{last_error}"
    )


def load_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "gamePk",
                "regulation_home_runs",
                "regulation_away_runs",
                "tied_after_9",
                "went_extra",
                "final_home_runs_api",
                "final_away_runs_api",
                "home_final_win_api",
            ]
        )

    cache = pd.read_csv(
        path,
        dtype={"gamePk": "string"},
    )
    if "gamePk" not in cache.columns:
        raise RuntimeError(
            f"Invalid linescore cache: {path}"
        )
    cache["gamePk"] = cache["gamePk"].map(
        normalize_game_pk
    )
    return cache.drop_duplicates(
        subset=["gamePk"],
        keep="last",
    ).reset_index(drop=True)


def ensure_linescores(
    game_pks: list[str],
    cache_path: Path,
) -> pd.DataFrame:
    cache_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    cache = load_cache(cache_path)

    known = set(
        cache["gamePk"].dropna().astype(str)
    )
    needed = [
        str(pk)
        for pk in game_pks
        if str(pk) not in known
    ]

    if needed:
        print(
            f"Fetching {len(needed)} uncached MLB linescores..."
        )

    new_rows = []
    for idx, game_pk in enumerate(needed, 1):
        row = fetch_linescore(game_pk)
        new_rows.append(row)

        if (
            idx % 50 == 0
            or idx == len(needed)
        ):
            updated = pd.concat(
                [
                    cache,
                    pd.DataFrame(new_rows),
                ],
                ignore_index=True,
            )
            updated = updated.drop_duplicates(
                subset=["gamePk"],
                keep="last",
            )
            updated.to_csv(
                cache_path,
                index=False,
            )
            print(
                f"  cached {idx}/{len(needed)}"
            )

    if new_rows:
        cache = pd.concat(
            [
                cache,
                pd.DataFrame(new_rows),
            ],
            ignore_index=True,
        )
        cache = cache.drop_duplicates(
            subset=["gamePk"],
            keep="last",
        )
        cache.to_csv(
            cache_path,
            index=False,
        )

    return cache


def build_extra_history(
    training: pd.DataFrame,
    linescores: pd.DataFrame,
) -> pd.DataFrame:
    out = training.merge(
        linescores,
        on="gamePk",
        how="left",
        validate="many_to_one",
    )

    missing = out["tied_after_9"].isna()
    if missing.any():
        sample = out.loc[
            missing,
            ["gamePk", "game_date", "game_id"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            "Missing cached linescore for training game; "
            f"sample={sample}"
        )

    # Item #11 needs the final winner, not exact final run totals.
    # Historical score revisions can change final run totals while leaving the
    # winner unchanged, so exact-score disagreement is recorded but is not
    # fatal. Winner disagreement is fatal.
    target_home_win = (
        out["target_home_runs"].astype(float)
        > out["target_away_runs"].astype(float)
    ).astype(int)

    winner_mismatch = (
        target_home_win
        != out["home_final_win_api"].astype(int)
    )
    if winner_mismatch.any():
        sample = out.loc[
            winner_mismatch,
            [
                "gamePk",
                "game_date",
                "target_home_runs",
                "target_away_runs",
                "final_home_runs_api",
                "final_away_runs_api",
                "home_final_win_api",
            ],
        ].head(10).to_dict("records")
        raise RuntimeError(
            "MLB API final winner does not match training target; "
            f"sample={sample}"
        )

    score_mismatch = (
        out["final_home_runs_api"].astype(int)
        != out["target_home_runs"].astype(int)
    ) | (
        out["final_away_runs_api"].astype(int)
        != out["target_away_runs"].astype(int)
    )
    out["api_training_score_mismatch"] = score_mismatch.astype(int)

    return out


def empirical_extra_parameter(
    history: pd.DataFrame,
    before_date: pd.Timestamp,
) -> dict:
    prior = history[
        history["game_date"] < before_date
    ]
    extras = prior[
        prior["tied_after_9"].astype(int).eq(1)
    ]

    n = int(len(extras))
    home_wins = int(
        extras["home_final_win_api"]
        .astype(int)
        .sum()
    )

    # Beta(1,1) posterior mean.
    q = (home_wins + 1.0) / (n + 2.0)

    return {
        "history_games": int(len(prior)),
        "prior_extra_games": n,
        "prior_extra_home_wins": home_wins,
        "q_extra_home": float(q),
    }


def add_method_probabilities(
    period_frame: pd.DataFrame,
    q_extra_home: float,
) -> pd.DataFrame:
    out = period_frame.copy()

    home_mean = out["mean_home_runs"].to_numpy(
        dtype=float
    )
    away_mean = out["mean_away_runs"].to_numpy(
        dtype=float
    )

    p_home_decisive = (
        1.0
        - skellam.cdf(
            0,
            home_mean,
            away_mean,
        )
    )
    p_away_decisive = skellam.cdf(
        -1,
        home_mean,
        away_mean,
    )
    p_tie = skellam.pmf(
        0,
        home_mean,
        away_mean,
    )
    resolved = (
        p_home_decisive
        + p_away_decisive
    )

    if (
        np.any(~np.isfinite(resolved))
        or np.any(resolved <= 0.0)
    ):
        raise RuntimeError(
            "Invalid decisive probability mass"
        )

    current = p_home_decisive / resolved
    explicit_50 = p_home_decisive + 0.5 * p_tie
    explicit_emp = (
        p_home_decisive
        + float(q_extra_home) * p_tie
    )

    for name, values in {
        "current_conditioned": current,
        "explicit_extra_50_50": explicit_50,
        "explicit_extra_empirical": explicit_emp,
    }.items():
        if (
            np.any(~np.isfinite(values))
            or np.any(values <= 0.0)
            or np.any(values >= 1.0)
        ):
            raise RuntimeError(
                f"{name} produced invalid moneyline probabilities"
            )
        out[f"{name}_home_prob"] = values

    out["modeled_regulation_tie_prob"] = p_tie
    out["observed_home_win"] = (
        out["actual_home_runs"]
        > out["actual_away_runs"]
    ).astype(int)

    return out


def per_game_log_loss(y, p) -> np.ndarray:
    y = np.asarray(y, dtype=float)
    p = np.clip(
        np.asarray(p, dtype=float),
        EPS,
        1.0 - EPS,
    )
    return -(
        y * np.log(p)
        + (1.0 - y) * np.log(1.0 - p)
    )


def bootstrap_delta_ci(
    y,
    baseline_p,
    alternative_p,
    *,
    draws: int = BOOTSTRAP_DRAWS,
    seed: int = RNG_SEED,
) -> tuple[float, float, float]:
    y = np.asarray(y, dtype=float)
    baseline_loss = per_game_log_loss(
        y,
        baseline_p,
    )
    alternative_loss = per_game_log_loss(
        y,
        alternative_p,
    )
    delta = alternative_loss - baseline_loss

    observed = float(np.mean(delta))

    rng = np.random.default_rng(seed)
    n = len(delta)
    values = np.empty(draws, dtype=float)

    # Chunk to keep memory small.
    chunk = 1000
    done = 0
    while done < draws:
        size = min(chunk, draws - done)
        idx = rng.integers(
            0,
            n,
            size=(size, n),
        )
        values[
            done : done + size
        ] = np.mean(
            delta[idx],
            axis=1,
        )
        done += size

    low, high = np.quantile(
        values,
        [0.025, 0.975],
    )
    return (
        observed,
        float(low),
        float(high),
    )


def score_period(
    frame: pd.DataFrame,
    period: str,
) -> list[dict]:
    methods = [
        "current_conditioned",
        "explicit_extra_50_50",
        "explicit_extra_empirical",
    ]
    y = frame["observed_home_win"].to_numpy(
        dtype=float
    )

    current_p = frame[
        "current_conditioned_home_prob"
    ].to_numpy(dtype=float)

    rows = []
    for method in methods:
        p = frame[
            f"{method}_home_prob"
        ].to_numpy(dtype=float)

        if method == "current_conditioned":
            delta = 0.0
            low = 0.0
            high = 0.0
        else:
            delta, low, high = bootstrap_delta_ci(
                y,
                current_p,
                p,
                seed=(
                    RNG_SEED
                    + sum(ord(c) for c in period + method)
                ),
            )

        rows.append(
            {
                "period": period,
                "method": method,
                "games": int(len(frame)),
                "moneyline_log_loss": binary_log_loss(
                    y,
                    p,
                ),
                "moneyline_ece": ece(
                    y,
                    p,
                ),
                "moneyline_brier": brier(
                    y,
                    p,
                ),
                "log_loss_delta_vs_current": float(
                    delta
                ),
                "log_loss_delta_bootstrap_95_low": float(
                    low
                ),
                "log_loss_delta_bootstrap_95_high": float(
                    high
                ),
                "mean_home_probability": float(
                    np.mean(p)
                ),
                "mean_absolute_probability_change_vs_current": float(
                    np.mean(np.abs(p - current_p))
                ),
                "mean_modeled_regulation_tie_probability": float(
                    frame[
                        "modeled_regulation_tie_prob"
                    ].mean()
                ),
            }
        )

    return rows


def period_sort_key(value: str) -> tuple[int, int]:
    if value.startswith("cv_fold_"):
        return (
            0,
            int(value.replace("cv_fold_", "")),
        )
    if value == "final_test":
        return (1, 0)
    return (2, 0)


def write_summary(
    path: Path,
    metrics: pd.DataFrame,
    params: pd.DataFrame,
) -> None:
    lines = [
        "# Moneyline Extra-Innings Backtest",
        "",
        "## Question",
        "",
        "Does the current method of conditioning away modeled regulation-score ties",
        "produce better final-game MLB moneyline probabilities than explicitly",
        "allocating tie mass to an extra-inning outcome model?",
        "",
        "## Methods",
        "",
        "- `current_conditioned`: remove Skellam tie mass and renormalize decisive outcomes.",
        "- `explicit_extra_50_50`: allocate 50% of tie mass to the home team.",
        "- `explicit_extra_empirical`: allocate tie mass using the strictly prior",
        "  Beta-smoothed empirical home win rate among games actually tied after nine.",
        "",
        "Lower log loss, ECE, and Brier score are better. For the explicit methods,",
        "`log_loss_delta_vs_current < 0` means improvement over current production.",
        "",
        "## Metrics",
        "",
        metrics.to_markdown(index=False),
        "",
        "## Chronological extra-inning parameters",
        "",
        params.to_markdown(index=False),
        "",
    ]
    path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare conditioned-away Skellam ties with explicit "
            "extra-inning moneyline modeling"
        )
    )
    parser.add_argument(
        "--probabilities",
        type=Path,
        default=DEFAULT_PROBS,
    )
    parser.add_argument(
        "--training",
        type=Path,
        default=DEFAULT_TRAINING,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = find_repo_root()

    probabilities_path = (
        args.probabilities
        if args.probabilities.is_absolute()
        else root / args.probabilities
    )
    training_path = (
        args.training
        if args.training.is_absolute()
        else root / args.training
    )
    output_dir = (
        args.output_dir
        if args.output_dir.is_absolute()
        else root / args.output_dir
    )
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    oos = load_poisson_oos(
        probabilities_path
    )
    training = load_training_games(
        training_path
    )
    oos = attach_game_pk(
        oos,
        training,
    )

    # Only fetch linescores needed through the last OOS date.
    max_oos_date = oos["game_date"].max()
    history_source = training[
        training["game_date"] <= max_oos_date
    ].copy()

    cache_path = (
        output_dir
        / "linescore_cache.csv"
    )
    linescores = ensure_linescores(
        history_source["gamePk"]
        .dropna()
        .astype(str)
        .unique()
        .tolist(),
        cache_path,
    )
    history = build_extra_history(
        history_source,
        linescores,
    )

    score_mismatch_count = int(
        history["api_training_score_mismatch"].sum()
    )
    print(
        "Historical exact-score disagreements with MLB API: "
        f"{score_mismatch_count} "
        "(winner agreement required)"
    )

    periods = sorted(
        oos["period"].unique().tolist(),
        key=period_sort_key,
    )

    metric_rows = []
    parameter_rows = []
    game_frames = []

    for period in periods:
        period_frame = oos[
            oos["period"].eq(period)
        ].copy()
        if period_frame.empty:
            continue

        eval_start = period_frame[
            "game_date"
        ].min()
        eval_end = period_frame[
            "game_date"
        ].max()

        parameter = empirical_extra_parameter(
            history,
            eval_start,
        )

        scored = add_method_probabilities(
            period_frame,
            parameter["q_extra_home"],
        )

        # Attach actual tied-after-nine status for descriptive reporting.
        scored = scored.merge(
            linescores[
                [
                    "gamePk",
                    "tied_after_9",
                    "went_extra",
                    "home_final_win_api",
                ]
            ],
            on="gamePk",
            how="left",
            validate="many_to_one",
        )
        if scored["tied_after_9"].isna().any():
            raise RuntimeError(
                f"{period}: missing linescore for OOS game"
            )

        # Score moneyline performance against the authoritative MLB API
        # final winner. Verify item #6's stored winner agrees.
        stored_home_win = (
            scored["actual_home_runs"].astype(float)
            > scored["actual_away_runs"].astype(float)
        ).astype(int)
        api_home_win = scored["home_final_win_api"].astype(int)
        winner_mismatch = stored_home_win != api_home_win
        if winner_mismatch.any():
            sample = scored.loc[
                winner_mismatch,
                [
                    "gamePk",
                    "game_date",
                    "actual_home_runs",
                    "actual_away_runs",
                    "home_final_win_api",
                ],
            ].head(10).to_dict("records")
            raise RuntimeError(
                f"{period}: MLB API winner disagrees with item #6 outcome; "
                f"sample={sample}"
            )
        scored["observed_home_win"] = api_home_win

        eval_extras = scored[
            scored["tied_after_9"]
            .astype(int)
            .eq(1)
        ]
        eval_extra_home_rate = (
            float(
                eval_extras[
                    "home_final_win_api"
                ].mean()
            )
            if len(eval_extras)
            else float("nan")
        )

        parameter_rows.append(
            {
                "period": period,
                "eval_start": eval_start.strftime(
                    "%Y-%m-%d"
                ),
                "eval_end": eval_end.strftime(
                    "%Y-%m-%d"
                ),
                **parameter,
                "evaluation_games": int(
                    len(scored)
                ),
                "evaluation_extra_games": int(
                    len(eval_extras)
                ),
                "evaluation_extra_home_win_rate": (
                    eval_extra_home_rate
                ),
                "evaluation_extra_frequency": float(
                    len(eval_extras)
                    / len(scored)
                ),
            }
        )

        metric_rows.extend(
            score_period(
                scored,
                period,
            )
        )
        game_frames.append(scored)

        print(
            f"{period}: games={len(scored)} "
            f"prior_extras={parameter['prior_extra_games']} "
            f"q_extra_home={parameter['q_extra_home']:.6f} "
            f"eval_extras={len(eval_extras)}"
        )

    games = pd.concat(
        game_frames,
        ignore_index=True,
    )
    metrics = pd.DataFrame(metric_rows)
    params = pd.DataFrame(parameter_rows)

    validation = games[
        games["period"].str.startswith(
            "cv_fold_"
        )
    ]
    if not validation.empty:
        metric_rows.extend(
            score_period(
                validation,
                "validation_combined",
            )
        )
        metrics = pd.DataFrame(metric_rows)

    metrics_path = (
        output_dir
        / "moneyline_method_metrics.csv"
    )
    params_path = (
        output_dir
        / "moneyline_period_parameters.csv"
    )
    games_path = (
        output_dir
        / "moneyline_game_probabilities.csv"
    )
    summary_path = (
        output_dir
        / "moneyline_extra_innings_summary.md"
    )

    metrics.to_csv(
        metrics_path,
        index=False,
    )
    params.to_csv(
        params_path,
        index=False,
    )
    games.to_csv(
        games_path,
        index=False,
    )
    write_summary(
        summary_path,
        metrics,
        params,
    )

    print()
    print("Moneyline extra-innings backtest complete.")
    print()
    focus = metrics[
        metrics["period"].isin(
            [
                "validation_combined",
                "final_test",
            ]
        )
    ][
        [
            "period",
            "method",
            "games",
            "moneyline_log_loss",
            "moneyline_ece",
            "moneyline_brier",
            "log_loss_delta_vs_current",
            "log_loss_delta_bootstrap_95_low",
            "log_loss_delta_bootstrap_95_high",
        ]
    ]
    print(
        focus.to_string(index=False)
    )
    print()
    print(f"Metrics: {metrics_path}")
    print(f"Parameters: {params_path}")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
