#!/usr/bin/env python3
"""Morning MLB selector wrapper around the shared selector implementation."""

import runpy
from pathlib import Path

import yaml


SELECTOR_PATH = Path(__file__).with_name("baseball_select_bets.py")
selector = runpy.run_path(
    str(SELECTOR_PATH),
    run_name="mlb_selector_shared",
)

output_dir = Path("docs/win/baseball/mlb/04_select/morning")
config_path = Path("docs/win/baseball/mlb/config/markets_AM.yaml")
error_dir = Path("docs/win/baseball/mlb/errors/04_select")

selector["OUTPUT_DIR"] = output_dir
selector["CONFIG_PATH"] = config_path
selector["AUDIT_DIR"] = output_dir / "audit"
selector["LOCKED_DIR"] = output_dir / "locked"
selector["ERROR_DIR"] = error_dir
selector["LOG_FILE"] = error_dir / "select_bets_AM.txt"

for directory in (
    selector["OUTPUT_DIR"],
    selector["AUDIT_DIR"],
    selector["LOCKED_DIR"],
    selector["ERROR_DIR"],
):
    directory.mkdir(parents=True, exist_ok=True)

with config_path.open("r", encoding="utf-8") as config_file:
    config = yaml.safe_load(config_file)["markets"]["mlb"]

selector["CONFIG"] = config
selector["FILTERS"] = config


if __name__ == "__main__":
    selector["main"]()
