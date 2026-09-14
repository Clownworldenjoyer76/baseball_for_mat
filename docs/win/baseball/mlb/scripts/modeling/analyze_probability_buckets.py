#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import expit
from scipy.stats import poisson

BASE = Path("docs/win/baseball/mlb")
PREDICTIONS = BASE / "modeling/probability_calibration/calibration_predictions.csv"
DIST_PROBS = BASE / "modeling/count_distribution_backtest/game_probabilities.csv"
CALIBRATORS = BASE / "models/probability_calibration/market_calibrators.json"
OUTDIR = BASE / "modeling/probability_bucket_calibration"

BUCKET_EDGES = np.linspace(0.0, 1.0, 11)
BOOTSTRAP_DRAWS = 10000
SEED = 1212
EPS = 1e-8


def repo_root() -> Path:
    for start in (Path.cwd().resolve(), Path(__file__).resolve().parent):
        for p in (start, *start.parents):
            if (p / BASE).is_dir():
                return p
    raise RuntimeError("Could not locate repository root")


def parse_total_line(contract: str) -> float:
    vals = re.findall(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)", str(contract))
    if not vals:
        raise RuntimeError(f"Could not parse total line from contract: {contract!r}")
    return float(vals[-1])


def contract_side(contract: str) -> str:
    s = str(contract).lower()
    if "under" in s:
        return "under"
    if "over" in s:
        return "over"
    raise RuntimeError(f"Could not determine total side from contract: {contract!r}")


def bootstrap_gap_ci(p: np.ndarray, y: np.ndarray, seed: int) -> tuple[float, float]:
    d = np.asarray(p, float) - np.asarray(y, float)
    n = len(d)
    rng = np.random.default_rng(seed)
    vals = np.empty(BOOTSTRAP_DRAWS, float)

    pos = 0
    while pos < BOOTSTRAP_DRAWS:
        k = min(500, BOOTSTRAP_DRAWS - pos)
        idx = rng.integers(0, n, size=(k, n))
        vals[pos:pos + k] = d[idx].mean(axis=1)
        pos += k

    lo, hi = np.quantile(vals, [0.025, 0.975])
    return float(lo), float(hi)


def production_total_over_probability(mu: float, line: float, total_cal: dict) -> float:
    frac = abs(line - round(line))

    if frac < 1e-9:
        k = int(round(line))
        p_under = float(poisson.cdf(k - 1, mu))
        p_over = float(1.0 - poisson.cdf(k, mu))
    elif abs(frac - 0.5) < 1e-9:
        k = int(math.floor(line))
        p_under = float(poisson.cdf(k, mu))
        p_over = float(1.0 - p_under)
    else:
        raise RuntimeError(f"Unsupported total line: {line}")

    resolved = p_over + p_under
    if resolved <= 0.0:
        raise RuntimeError(f"Invalid resolved mass for total line {line}")

    q = np.clip(p_over / resolved, EPS, 1.0 - EPS)

    if not bool(total_cal.get("enabled", False)):
        return float(q)

    if total_cal.get("method") != "beta_logistic":
        raise RuntimeError(f"Unsupported total calibrator method: {total_cal.get('method')}")

    a = float(total_cal["a"])
    b = float(total_cal["b"])
    c = float(total_cal["intercept"])

    return float(expit(c + a * math.log(q) - b * math.log1p(-q)))


def load_production_rows(root: Path) -> pd.DataFrame:
    pred = pd.read_csv(root / PREDICTIONS)

    needed = {
        "market", "period", "game_date", "game_id",
        "contract", "y", "raw_p", "calibrated_p",
    }
    missing = needed - set(pred.columns)
    if missing:
        raise RuntimeError(f"Missing prediction columns: {sorted(missing)}")

    pred["game_date"] = pd.to_datetime(pred["game_date"], errors="raise")
    pred["y"] = pd.to_numeric(pred["y"], errors="coerce")
    pred["raw_p"] = pd.to_numeric(pred["raw_p"], errors="coerce")
    pred["calibrated_p"] = pd.to_numeric(pred["calibrated_p"], errors="coerce")

    cal = json.loads((root / CALIBRATORS).read_text(encoding="utf-8"))
    markets = cal.get("markets", {})

    out = []

    # Moneyline: use calibrated_p only if its production calibrator is enabled.
    ml = pred[pred["market"].eq("moneyline")].copy()
    ml_enabled = bool(markets.get("moneyline", {}).get("enabled", False))
    ml["production_p"] = ml["calibrated_p"] if ml_enabled else ml["raw_p"]
    out.append(ml)

    # Run line: same production-selection rule.
    rl = pred[pred["market"].eq("run_line")].copy()
    rl_enabled = bool(markets.get("run_line", {}).get("enabled", False))
    rl["production_p"] = rl["calibrated_p"] if rl_enabled else rl["raw_p"]
    out.append(rl)

    # Totals: rebuild the exact production conditional probability from run means,
    # then apply the production total calibrator once and complement Over/Under.
    total = pred[pred["market"].eq("total")].copy()
    total["line"] = total["contract"].map(parse_total_line)
    total["side"] = total["contract"].map(contract_side)

    dist = pd.read_csv(root / DIST_PROBS)
    needed_dist = {
        "period", "distribution", "game_id",
        "mean_home_runs", "mean_away_runs",
    }
    missing_dist = needed_dist - set(dist.columns)
    if missing_dist:
        raise RuntimeError(f"Missing distribution columns: {sorted(missing_dist)}")

    dist = dist[dist["distribution"].eq("poisson_skellam")].copy()
    dist = dist[
        ["period", "game_id", "mean_home_runs", "mean_away_runs"]
    ].drop_duplicates(["period", "game_id"])

    total = total.merge(
        dist,
        on=["period", "game_id"],
        how="left",
        validate="many_to_one",
    )

    if total[["mean_home_runs", "mean_away_runs"]].isna().any().any():
        raise RuntimeError("Totals rows are missing run means after merge")

    total_cal = markets.get("total")
    if total_cal is None:
        raise RuntimeError("Calibration artifact has no markets.total block")

    probs = []
    for r in total.itertuples(index=False):
        mu = float(r.mean_home_runs) + float(r.mean_away_runs)
        over_p = production_total_over_probability(mu, float(r.line), total_cal)
        probs.append(over_p if r.side == "over" else 1.0 - over_p)

    total["production_p"] = probs
    out.append(total)

    rows = pd.concat(out, ignore_index=True, sort=False)
    rows = rows.dropna(subset=["y", "production_p"]).copy()

    if not ((rows["production_p"] >= 0.0) & (rows["production_p"] <= 1.0)).all():
        raise RuntimeError("Production probabilities outside [0,1]")

    return rows


def bucket_rows(df: pd.DataFrame, view: str, market: str) -> list[dict]:
    p = df["production_p"].to_numpy(float)
    y = df["y"].to_numpy(float)
    ids = np.digitize(np.clip(p, 0.0, 1.0), BUCKET_EDGES[1:-1], right=False)

    rows = []
    for i in range(10):
        mask = ids == i
        if not mask.any():
            continue

        bp = p[mask]
        by = y[mask]
        gap = float(bp.mean() - by.mean())
        lo, hi = bootstrap_gap_ci(bp, by, SEED + i + sum(map(ord, market + view)))

        rows.append({
            "view": view,
            "market": market,
            "bucket": f"{BUCKET_EDGES[i]:.1f}-{BUCKET_EDGES[i + 1]:.1f}",
            "rows": int(mask.sum()),
            "mean_predicted": float(bp.mean()),
            "observed_rate": float(by.mean()),
            "calibration_gap": gap,
            "abs_calibration_gap": abs(gap),
            "ci95_low": lo,
            "ci95_high": hi,
            "systematic_bias": (
                "OVERSTATES" if len(bp) >= 30 and lo > 0.0
                else "UNDERSTATES" if len(bp) >= 30 and hi < 0.0
                else ""
            ),
        })

    return rows


def main() -> None:
    root = repo_root()
    rows = load_production_rows(root)

    views = {
        "chronological_crossfit": ["cv_fold_2", "cv_fold_3", "cv_fold_4"],
        "final_test_reference": ["final_test"],
    }

    bucket_records = []
    for view, periods in views.items():
        v = rows[rows["period"].isin(periods)]
        for market in ("moneyline", "run_line", "total"):
            m = v[v["market"].eq(market)]
            if m.empty:
                continue
            bucket_records.extend(bucket_rows(m, view, market))

    bucket_df = pd.DataFrame(bucket_records)
    systematic = bucket_df[bucket_df["systematic_bias"].ne("")].copy()

    outdir = root / OUTDIR
    outdir.mkdir(parents=True, exist_ok=True)

    bucket_df.to_csv(outdir / "probability_bucket_metrics.csv", index=False)
    systematic.to_csv(outdir / "systematic_bias_ranges.csv", index=False)

    summary = []
    summary.append("# Probability Bucket Calibration")
    summary.append("")
    summary.append("Production probabilities are evaluated exactly as emitted by the runtime.")
    summary.append("")
    summary.append(f"Systematic biased buckets: {len(systematic)}")
    summary.append("")

    if systematic.empty:
        summary.append("No bucket with at least 30 observations has a bootstrap 95% CI excluding zero.")
    else:
        for r in systematic.itertuples(index=False):
            summary.append(
                f"- {r.view} / {r.market} / {r.bucket}: "
                f"{r.systematic_bias}, gap={r.calibration_gap:.6f}, "
                f"CI=[{r.ci95_low:.6f}, {r.ci95_high:.6f}]"
            )

    (outdir / "probability_bucket_summary.md").write_text(
        "\n".join(summary) + "\n",
        encoding="utf-8",
    )

    print("CORRECTED PRODUCTION BUCKET EVALUATION")
    print(bucket_df.to_string(index=False))
    print()
    print("SYSTEMATIC BIAS RANGES")
    if systematic.empty:
        print("NONE")
    else:
        print(systematic.to_string(index=False))


if __name__ == "__main__":
    main()
