#!/usr/bin/env python3
"""Deterministic MLB probability / EV / Kelly invariant tests."""

from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import numpy as np
import pandas as pd


def _find_repo_root() -> Path:
    file_path = Path(__file__).resolve()

    for parent in file_path.parents:
        if (
            (parent / "requirements.txt").exists()
            and (parent / "docs/win/baseball/mlb").exists()
        ):
            return parent

    raise RuntimeError(
        f"Could not resolve repository root from {file_path}"
    )


REPO_ROOT = _find_repo_root()

PROBABILITY_SCRIPT = (
    REPO_ROOT
    / "docs/win/baseball/mlb/scripts/01_merge/build_juice_files.py"
)

EV_KELLY_SCRIPT = (
    REPO_ROOT
    / "docs/win/baseball/mlb/scripts/03_edges/compute_ev_kelly.py"
)

ABS_TOL = 1e-10
SIGN_TOL = 1e-10


def _load_module(name: str, path: Path):
    if not path.exists():
        raise RuntimeError(
            f"Required production module not found: {path}"
        )

    spec = importlib.util.spec_from_file_location(name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Could not load production module: {path}"
        )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


PROBS = _load_module(
    "mlb_build_juice_files",
    PROBABILITY_SCRIPT,
)

EVK = _load_module(
    "mlb_compute_ev_kelly",
    EV_KELLY_SCRIPT,
)


def _scalar(value) -> float:
    if isinstance(value, pd.Series):
        if len(value) != 1:
            raise AssertionError(
                f"Expected one-value Series, got {len(value)}"
            )

        return float(value.iloc[0])

    array = np.asarray(value, dtype=float)

    if array.size != 1:
        raise AssertionError(
            f"Expected scalar-like value, got shape={array.shape}"
        )

    return float(array.reshape(-1)[0])


def _binary_ev(
    probability: float,
    decimal_odds: float,
) -> float:
    return _scalar(
        EVK.compute_binary_ev(
            pd.Series([probability], dtype=float),
            pd.Series([decimal_odds], dtype=float),
        )
    )


def _binary_kelly_raw(
    probability: float,
    decimal_odds: float,
) -> float:
    return _scalar(
        EVK.compute_binary_kelly_raw(
            pd.Series([probability], dtype=float),
            pd.Series([decimal_odds], dtype=float),
        )
    )


def _total_ev(
    p_win: float,
    p_loss: float,
    decimal_odds: float,
) -> float:
    return _scalar(
        EVK.compute_total_ev(
            pd.Series([p_win], dtype=float),
            pd.Series([p_loss], dtype=float),
            pd.Series([decimal_odds], dtype=float),
        )
    )


def _total_kelly_raw(
    p_win: float,
    p_loss: float,
    decimal_odds: float,
) -> float:
    return _scalar(
        EVK.compute_total_kelly_raw(
            pd.Series([p_win], dtype=float),
            pd.Series([p_loss], dtype=float),
            pd.Series([decimal_odds], dtype=float),
            pd.Series(["deterministic_test"]),
            "deterministic total test",
        )
    )


def _assert_close(
    actual: float,
    expected: float,
    tol: float = ABS_TOL,
) -> None:
    assert math.isfinite(actual), (
        f"actual is non-finite: {actual}"
    )

    assert math.isfinite(expected), (
        f"expected is non-finite: {expected}"
    )

    assert abs(actual - expected) <= tol, (
        f"actual={actual} "
        f"expected={expected} "
        f"diff={actual - expected}"
    )


def _sign(
    value: float,
    tol: float = SIGN_TOL,
) -> int:
    if value > tol:
        return 1

    if value < -tol:
        return -1

    return 0


def test_moneyline_probability_ev_kelly_contract() -> None:
    rows = [
        (4.8, 3.6, 1.80, 2.20),
        (3.9, 4.2, 2.05, 1.85),
        (5.1, 5.1, 1.95, 1.95),
        (2.7, 4.9, 2.75, 1.50),
    ]

    for (
        home_runs,
        away_runs,
        home_decimal,
        away_decimal,
    ) in rows:
        (
            home_prob,
            away_prob,
            _tie_raw,
        ) = PROBS.moneyline_probabilities(
            home_runs,
            away_runs,
        )

        _assert_close(
            home_prob + away_prob,
            1.0,
        )

        for probability, decimal_odds in [
            (home_prob, home_decimal),
            (away_prob, away_decimal),
        ]:
            ev = _binary_ev(
                probability,
                decimal_odds,
            )

            raw_kelly = _binary_kelly_raw(
                probability,
                decimal_odds,
            )

            _assert_close(
                ev,
                (probability * decimal_odds) - 1.0,
            )

            assert _sign(raw_kelly) == _sign(ev), (
                f"Kelly/EV sign mismatch: "
                f"p={probability} "
                f"decimal={decimal_odds} "
                f"ev={ev} "
                f"kelly={raw_kelly}"
            )


def test_run_line_complement_and_home_away_swap() -> None:
    cases = [
        (4.9, 3.6),
        (3.2, 5.0),
        (4.1, 4.1),
        (6.0, 2.8),
    ]

    for home_runs, away_runs in cases:
        (
            home_cover,
            away_cover,
        ) = PROBS.run_line_probabilities(
            home_runs,
            away_runs,
            -1.5,
            1.5,
        )

        _assert_close(
            home_cover + away_cover,
            1.0,
        )

        (
            swapped_home_cover,
            swapped_away_cover,
        ) = PROBS.run_line_probabilities(
            away_runs,
            home_runs,
            1.5,
            -1.5,
        )

        _assert_close(
            swapped_home_cover,
            away_cover,
        )

        _assert_close(
            swapped_away_cover,
            home_cover,
        )


def test_run_line_calibration_contract_and_ranking() -> None:
    _assert_close(
        PROBS.RUN_LINE_CALIBRATION_INTERCEPT,
        0.0,
    )

    _assert_close(
        PROBS.RUN_LINE_CALIBRATION_SLOPE,
        0.281791,
    )

    raw_grid = [
        0.05,
        0.15,
        0.25,
        0.40,
        0.50,
        0.60,
        0.75,
        0.90,
        0.95,
    ]

    calibrated_grid = []

    for raw_home in raw_grid:
        raw_away = 1.0 - raw_home

        (
            calibrated_home,
            calibrated_away,
        ) = PROBS.calibrate_run_line_probabilities(
            raw_home,
            raw_away,
        )

        _assert_close(
            calibrated_home + calibrated_away,
            1.0,
        )

        expected_logit = math.log(
            raw_home / (1.0 - raw_home)
        )

        expected_home = 1.0 / (
            1.0
            + math.exp(
                -(
                    0.0
                    + 0.281791
                    * expected_logit
                )
            )
        )

        _assert_close(
            calibrated_home,
            expected_home,
        )

        assert abs(calibrated_home - 0.5) <= abs(raw_home - 0.5) + ABS_TOL

        calibrated_grid.append(
            calibrated_home
        )

    assert all(
        right > left
        for left, right in zip(
            calibrated_grid,
            calibrated_grid[1:],
        )
    )


def test_calibrated_run_line_probability_home_away_swap() -> None:
    cases = [
        (4.9, 3.6),
        (3.2, 5.0),
        (4.1, 4.1),
        (6.0, 2.8),
    ]

    for home_runs, away_runs in cases:
        (
            home_cover,
            away_cover,
        ) = PROBS.calibrated_run_line_probabilities(
            home_runs,
            away_runs,
            -1.5,
            1.5,
        )

        _assert_close(
            home_cover + away_cover,
            1.0,
        )

        (
            swapped_home_cover,
            swapped_away_cover,
        ) = PROBS.calibrated_run_line_probabilities(
            away_runs,
            home_runs,
            1.5,
            -1.5,
        )

        _assert_close(
            swapped_home_cover,
            away_cover,
        )

        _assert_close(
            swapped_away_cover,
            home_cover,
        )


def test_calibrated_run_line_ev_kelly_contract() -> None:
    cases = [
        (4.9, 3.6, -1.5, 1.5, 1.95, 1.87),
        (3.2, 5.0, 1.5, -1.5, 1.80, 2.05),
        (4.1, 4.1, -1.5, 1.5, 2.20, 1.70),
        (6.0, 2.8, -1.5, 1.5, 1.72, 2.25),
    ]

    for (
        home_runs,
        away_runs,
        home_line,
        away_line,
        home_decimal,
        away_decimal,
    ) in cases:
        (
            home_prob,
            away_prob,
        ) = PROBS.calibrated_run_line_probabilities(
            home_runs,
            away_runs,
            home_line,
            away_line,
        )

        _assert_close(
            home_prob + away_prob,
            1.0,
        )

        for probability, decimal_odds in [
            (home_prob, home_decimal),
            (away_prob, away_decimal),
        ]:
            ev = _binary_ev(
                probability,
                decimal_odds,
            )

            raw_kelly = _binary_kelly_raw(
                probability,
                decimal_odds,
            )

            _assert_close(
                ev,
                (probability * decimal_odds) - 1.0,
            )

            assert _sign(raw_kelly) == _sign(ev), (
                f"Calibrated run-line Kelly/EV sign mismatch: "
                f"p={probability} "
                f"decimal={decimal_odds} "
                f"ev={ev} "
                f"kelly={raw_kelly}"
            )


def test_half_run_total_has_no_push_and_resolves_to_one() -> None:
    cases = [
        (4.6, 3.8, 7.5),
        (5.0, 4.0, 8.5),
        (3.2, 3.4, 6.5),
        (5.5, 4.7, 10.5),
    ]

    for (
        home_runs,
        away_runs,
        total_line,
    ) in cases:
        (
            p_over,
            p_under,
            p_push,
        ) = PROBS.totals_probabilities(
            home_runs,
            away_runs,
            total_line,
        )

        _assert_close(
            p_push,
            0.0,
        )

        _assert_close(
            p_over + p_under,
            1.0,
        )


def test_whole_run_total_probability_and_push_aware_ev() -> None:
    cases = [
        (4.6, 3.8, 8.0, 2.05),
        (5.1, 4.2, 9.0, 1.91),
        (3.4, 3.3, 7.0, 2.20),
    ]

    for (
        home_runs,
        away_runs,
        total_line,
        decimal_odds,
    ) in cases:
        (
            p_over,
            p_under,
            p_push,
        ) = PROBS.totals_probabilities(
            home_runs,
            away_runs,
            total_line,
        )

        _assert_close(
            p_over + p_under + p_push,
            1.0,
        )

        over_ev = _total_ev(
            p_over,
            p_under,
            decimal_odds,
        )

        expected_over_ev = (
            p_over * (decimal_odds - 1.0)
            - p_under
        )

        _assert_close(
            over_ev,
            expected_over_ev,
        )

        under_ev = _total_ev(
            p_under,
            p_over,
            decimal_odds,
        )

        expected_under_ev = (
            p_under * (decimal_odds - 1.0)
            - p_over
        )

        _assert_close(
            under_ev,
            expected_under_ev,
        )


def test_model_probabilities_are_price_independent() -> None:
    model_home_runs = 4.8
    model_away_runs = 3.7
    home_line = -1.5
    away_line = 1.5

    prices_a = {
        "home": 1.80,
        "away": 2.10,
    }

    prices_b = {
        "home": 2.25,
        "away": 1.70,
    }

    probs_a = PROBS.calibrated_run_line_probabilities(
        model_home_runs,
        model_away_runs,
        home_line,
        away_line,
    )

    probs_b = PROBS.calibrated_run_line_probabilities(
        model_home_runs,
        model_away_runs,
        home_line,
        away_line,
    )

    _assert_close(
        probs_a[0],
        probs_b[0],
    )

    _assert_close(
        probs_a[1],
        probs_b[1],
    )

    home_prob = probs_a[0]

    break_even_a = (
        1.0 / prices_a["home"]
    )

    break_even_b = (
        1.0 / prices_b["home"]
    )

    edge_a = (
        home_prob - break_even_a
    )

    edge_b = (
        home_prob - break_even_b
    )

    ev_a = _binary_ev(
        home_prob,
        prices_a["home"],
    )

    ev_b = _binary_ev(
        home_prob,
        prices_b["home"],
    )

    kelly_a = _binary_kelly_raw(
        home_prob,
        prices_a["home"],
    )

    kelly_b = _binary_kelly_raw(
        home_prob,
        prices_b["home"],
    )

    assert not math.isclose(
        break_even_a,
        break_even_b,
        abs_tol=ABS_TOL,
    )

    assert not math.isclose(
        edge_a,
        edge_b,
        abs_tol=ABS_TOL,
    )

    assert not math.isclose(
        ev_a,
        ev_b,
        abs_tol=ABS_TOL,
    )

    assert not math.isclose(
        kelly_a,
        kelly_b,
        abs_tol=ABS_TOL,
    )


def test_binary_ev_and_raw_kelly_sign_consistency_grid() -> None:
    probabilities = [
        0.05,
        0.15,
        0.25,
        0.40,
        0.50,
        0.60,
        0.75,
        0.90,
        0.95,
    ]

    decimal_prices = [
        1.20,
        1.40,
        1.60,
        1.80,
        2.00,
        2.25,
        2.75,
        3.50,
        5.00,
    ]

    for probability in probabilities:
        for decimal_odds in decimal_prices:
            ev = _binary_ev(
                probability,
                decimal_odds,
            )

            raw_kelly = _binary_kelly_raw(
                probability,
                decimal_odds,
            )

            assert _sign(ev) == _sign(raw_kelly), (
                f"Binary sign mismatch: "
                f"p={probability} "
                f"decimal={decimal_odds} "
                f"ev={ev} "
                f"kelly={raw_kelly}"
            )

            if ev > SIGN_TOL:
                assert raw_kelly > SIGN_TOL

            elif ev < -SIGN_TOL:
                assert raw_kelly < -SIGN_TOL

            else:
                assert abs(raw_kelly) <= SIGN_TOL


def test_push_aware_total_ev_and_raw_kelly_sign_consistency_grid() -> None:
    total_cases = [
        PROBS.totals_probabilities(
            4.0,
            3.5,
            7.0,
        ),
        PROBS.totals_probabilities(
            4.8,
            4.1,
            9.0,
        ),
        PROBS.totals_probabilities(
            5.3,
            3.9,
            8.0,
        ),
        PROBS.totals_probabilities(
            3.2,
            3.1,
            6.0,
        ),
    ]

    decimal_prices = [
        1.50,
        1.75,
        1.91,
        2.00,
        2.20,
        2.50,
        3.00,
    ]

    for (
        p_over,
        p_under,
        p_push,
    ) in total_cases:
        _assert_close(
            p_over + p_under + p_push,
            1.0,
        )

        for p_win, p_loss in [
            (p_over, p_under),
            (p_under, p_over),
        ]:
            resolved_mass = (
                p_win + p_loss
            )

            assert resolved_mass > 0

            for decimal_odds in decimal_prices:
                ev = _total_ev(
                    p_win,
                    p_loss,
                    decimal_odds,
                )

                raw_kelly = _total_kelly_raw(
                    p_win,
                    p_loss,
                    decimal_odds,
                )

                assert _sign(ev) == _sign(raw_kelly), (
                    f"Total sign mismatch: "
                    f"p_win={p_win} "
                    f"p_loss={p_loss} "
                    f"p_push={p_push} "
                    f"decimal={decimal_odds} "
                    f"ev={ev} "
                    f"kelly={raw_kelly}"
                )

                if ev > SIGN_TOL:
                    assert raw_kelly > SIGN_TOL

                elif ev < -SIGN_TOL:
                    assert raw_kelly < -SIGN_TOL

                else:
                    assert abs(raw_kelly) <= SIGN_TOL