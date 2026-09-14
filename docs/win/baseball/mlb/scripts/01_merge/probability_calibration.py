#!/usr/bin/env python3
"""
Runtime application of market-specific MLB probability calibration.

The artifact is produced by:
    docs/win/baseball/mlb/scripts/modeling/fit_probability_calibrators.py
"""

from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path

EPS = 1e-12
BASE_DIR = Path("docs/win/baseball/mlb")
DEFAULT_ARTIFACT = (
    BASE_DIR
    / "models/probability_calibration/market_calibrators.json"
)
REQUIRED_MARKETS = ("moneyline", "run_line", "total")


def find_repo_root() -> Path:
    starts = [Path.cwd().resolve(), Path(__file__).resolve().parent]
    for start in starts:
        for candidate in (start, *start.parents):
            if (candidate / BASE_DIR).is_dir():
                return candidate
    raise RuntimeError("Could not locate repository root")


def calibration_artifact_path() -> Path:
    return find_repo_root() / DEFAULT_ARTIFACT


@lru_cache(maxsize=1)
def _load_artifact() -> dict:
    path = calibration_artifact_path()
    if not path.exists():
        raise FileNotFoundError(
            "Probability calibration artifact is required but missing: "
            f"{path}. Run fit_probability_calibrators.py first."
        )

    artifact = json.loads(
        path.read_text(encoding="utf-8")
    )

    if artifact.get("schema_version") != 1:
        raise RuntimeError(
            "Unsupported probability calibration artifact schema: "
            f"{artifact.get('schema_version')}"
        )

    markets = artifact.get("markets")
    if not isinstance(markets, dict):
        raise RuntimeError(
            "Probability calibration artifact has no markets object"
        )

    for market in REQUIRED_MARKETS:
        if market not in markets:
            raise RuntimeError(
                f"Probability calibration artifact missing market: {market}"
            )
        cal = markets[market]
        if cal.get("method") != "beta_logistic":
            raise RuntimeError(
                f"{market}: unsupported calibration method "
                f"{cal.get('method')}"
            )
        if not isinstance(cal.get("enabled"), bool):
            raise RuntimeError(
                f"{market}: missing/invalid enabled flag"
            )
        for field in ("log_a", "log_b", "intercept"):
            value = cal.get(field)
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                raise RuntimeError(
                    f"{market}: invalid calibration parameter {field}={value}"
                )

    return artifact


def _sigmoid(z: float) -> float:
    if z >= 0.0:
        return 1.0 / (1.0 + math.exp(-z))
    ez = math.exp(z)
    return ez / (1.0 + ez)


def calibrate_binary_probability(
    market: str,
    probability: float,
) -> float:
    if market not in REQUIRED_MARKETS:
        raise ValueError(
            f"Unknown calibration market: {market}"
        )

    p = float(probability)
    if not math.isfinite(p) or p < 0.0 or p > 1.0:
        raise ValueError(
            f"{market}: invalid raw probability {probability}"
        )

    p = min(max(p, EPS), 1.0 - EPS)
    cal = _load_artifact()["markets"][market]

    if not cal["enabled"]:
        return p

    a = math.exp(float(cal["log_a"]))
    b = math.exp(float(cal["log_b"]))
    intercept = float(cal["intercept"])

    z = (
        intercept
        + a * math.log(p)
        - b * math.log1p(-p)
    )
    q = _sigmoid(z)

    if not math.isfinite(q) or q <= 0.0 or q >= 1.0:
        raise RuntimeError(
            f"{market}: invalid calibrated probability {q}"
        )
    return q
