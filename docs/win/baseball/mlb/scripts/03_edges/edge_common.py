#!/usr/bin/env python3
# Shared constants and file classification for MLB edge stages.

from pathlib import Path


MONEYLINE_REQUIRED_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_model_prob_moneyline",
    "away_model_prob_moneyline",
    "home_dk_decimal_moneyline",
    "away_dk_decimal_moneyline",
]

RUN_LINE_REQUIRED_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "home_model_prob_run_line",
    "away_model_prob_run_line",
    "home_dk_run_line_decimal",
    "away_dk_run_line_decimal",
]

TOTAL_REQUIRED_COLUMNS = [
    "game_id",
    "sport",
    "league",
    "game_date",
    "game_time",
    "home_team",
    "away_team",
    "over_model_prob_total_win",
    "over_model_prob_total_loss",
    "under_model_prob_total_win",
    "under_model_prob_total_loss",
    "total_model_prob_push",
    "dk_total_over_decimal",
    "dk_total_under_decimal",
]


def detect_market(file_name: str) -> str | None:
    lowered = file_name.lower()
    for token, market in (
        ("moneyline", "moneyline"),
        ("run_line", "run_line"),
        ("total", "total"),
    ):
        if token in lowered:
            return market
    return None


def record_unrecognized_file(
    input_file: Path,
    pf: dict,
    summary: dict,
    per_file: list,
    log,
) -> None:
    log(f"SKIP unrecognized file: {input_file.name}")
    pf["status"] = "skipped"
    summary["skipped"] += 1
    per_file.append(pf)
