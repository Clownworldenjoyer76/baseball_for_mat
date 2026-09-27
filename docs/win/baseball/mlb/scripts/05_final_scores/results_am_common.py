#!/usr/bin/env python3
# Shared loader and path configuration for MLB morning result stages.

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


def load_sibling(file_name: str, module_name: str):
    path = Path(__file__).with_name(file_name)
    spec = spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load results stage: {path}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def configure_morning_grade(module) -> None:
    base = Path("docs/win/baseball/mlb/05_final_scores")
    module.SELECT_DIR = Path("docs/win/baseball/mlb/04_select/morning")
    module.SCORE_DIR = base / "results/final_scores"
    module.OUTPUT_DIR = base / "morning/results/graded"
    module.DAILY_DIR = module.OUTPUT_DIR / "daily"
    module.UNMATCHED_DIR = base / "morning/results/unmatched"
    module.AUDIT_DIR = base / "morning/results/audit"
    module.ERROR_DIR = base / "morning/errors"

    for directory in (
        module.ERROR_DIR,
        module.OUTPUT_DIR,
        module.DAILY_DIR,
        module.UNMATCHED_DIR,
        module.AUDIT_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    module.GRADE_ERROR_LOG = module.ERROR_DIR / "mlb_results_grade_errors.txt"
    module.GRADE_SUMMARY_LOG = module.ERROR_DIR / "mlb_results_grade_summary.txt"
    module.UNMATCHED_SELECTED_FILE = module.UNMATCHED_DIR / "MLB_unmatched_selected_bets.csv"
    module.NOT_FINAL_SELECTED_FILE = module.UNMATCHED_DIR / "MLB_not_final_selected_bets.csv"
    module.POSTPONED_CANCELED_FILE = module.UNMATCHED_DIR / "MLB_postponed_canceled_games.csv"
    module.BLANK_SCORE_GAME_ID_FILE = module.UNMATCHED_DIR / "blank_final_score_game_ids_MLB.csv"
    module.RECONCILIATION_AUDIT_FILE = module.AUDIT_DIR / "selected_vs_graded_reconciliation.csv"
    module.DUPLICATE_AUDIT_FILE = module.AUDIT_DIR / "grading_duplicate_audit.csv"
    module.VALIDATION_AUDIT_FILE = module.AUDIT_DIR / "graded_output_validation_audit.csv"
    module.RESULT_COUNTS_FILE = module.AUDIT_DIR / "grading_result_counts.csv"
    module.SPOT_CHECK_FILE = module.AUDIT_DIR / "grading_spot_check.csv"


def configure_morning_analyze(module) -> None:
    base = Path("docs/win/baseball/mlb/05_final_scores/morning")
    module.MLB_INPUT = base / "results/graded/MLB_final.csv"
    module.OUTPUT_DIR = base / "intermediate"
    module.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def configure_morning_reports(module) -> None:
    base = Path("docs/win/baseball/mlb/05_final_scores/morning")
    module.INPUT_FILE = base / "intermediate/work_mlb.csv"
    module.SUMMARY_DIR = base
    module.REPORTS_DIR = base / "reports"
    module.OVERVIEW_DIR = module.REPORTS_DIR / "overview"
    module.ML_DIR = module.REPORTS_DIR / "moneyline"
    module.RL_DIR = module.REPORTS_DIR / "run_line"
    module.TOT_DIR = module.REPORTS_DIR / "totals"

    for directory in (
        module.SUMMARY_DIR,
        module.OVERVIEW_DIR,
        module.ML_DIR,
        module.RL_DIR,
        module.TOT_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)
