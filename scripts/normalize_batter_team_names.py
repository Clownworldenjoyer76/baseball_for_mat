#!/usr/bin/env python3
# Inject team_id and normalize team names for batters_today.csv
# Uses data/manual/team_directory.csv (manual source of truth).

import pandas as pd
import logging
from pathlib import Path

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BATTERS_FILE = "data/cleaned/batters_today.csv"
TEAM_DIRECTORY_FILE = "data/manual/team_directory.csv"  # replaced obsolete team_name_master
OUTPUT_FILE = "data/cleaned/batters_today.csv"
SUMMARY_FILE = Path("summaries/summary.txt")

REQUIRED_TEAMDIR_COLS = {
    "team_id", "team_code", "canonical_team", "team_name",
    "clean_team_name", "all_codes", "all_names"
}

def write_summary_line(success: bool, rows: int):
    status = "PASS" if success else "FAIL"
    line = f"[normalize_batter_team_names] {status} - {rows} rows processed\n"
    SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SUMMARY_FILE, "a") as f:
        f.write(line)

def _build_alias_maps(teamdir: pd.DataFrame):
    """
    Build two maps from team_directory.csv:
      alias_to_id   : any alias -> team_id (string)
      alias_to_name : any alias -> official team_name (string)
    Aliases include team_code, canonical_team, team_name, clean_team_name,
    and pipe-delimited entries in all_codes and all_names.
    """
    alias_to_id = {}
    alias_to_name = {}

    def _put(alias: str, tid: str, name: str):
        k = (alias or "").strip().upper()
        if not k:
            return
        if k not in alias_to_id:
            alias_to_id[k] = tid
        if k not in alias_to_name:
            alias_to_name[k] = name

    for _, r in teamdir.iterrows():
        tid = str(r.get("team_id", "")).strip()
        code = (r.get("team_code", "") or "").strip()
        name = (r.get("team_name", "") or "").strip()
        canon = (r.get("canonical_team", "") or "").strip()
        clean = (r.get("clean_team_name", "") or "").strip()

        # primary fields
        for alias in (code, name, canon, clean):
            _put(alias, tid, name)

        # lists
        for alias in (r.get("all_codes", "") or "").split("|"):
            _put(alias, tid, name)
        for alias in (r.get("all_names", "") or "").split("|"):
            _put(alias, tid, name)

    return alias_to_id, alias_to_name

def main():
    logger.info("📥 Loading batters_today and team_directory...")

    try:
        batters = pd.read_csv(BATTERS_FILE, dtype=str).fillna("")
        teamdir = pd.read_csv(TEAM_DIRECTORY_FILE, dtype=str).fillna("")

        if "team" not in batters.columns:
            raise ValueError("Missing 'team' column in batters_today.csv.")

        missing = REQUIRED_TEAMDIR_COLS - set(teamdir.columns)
        if missing:
            raise ValueError(
                f"{TEAM_DIRECTORY_FILE} missing required columns: {', '.join(sorted(missing))}"
            )

        # Build alias maps
        alias_to_id, alias_to_name = _build_alias_maps(teamdir)

        # Preserve original team strings for ID resolution
        original_team = batters["team"].astype(str)

        # Compute team_id via alias map (nullable Int64)
        team_id_series = original_team.map(
            lambda v: alias_to_id.get((v or "").strip().upper())
        )
        # Cast to nullable integer
        team_id_series = pd.to_numeric(team_id_series, errors="coerce").astype("Int64")

        # Normalize display team name via alias map; fall back to original
        normalized_team_name = original_team.map(
            lambda v: alias_to_name.get((v or "").strip().upper(), v)
        )

        # Report unmapped share (informational only)
        unmapped = team_id_series.isna().sum()
        if unmapped:
            pct = (unmapped / len(batters)) * 100
            logger.warning(f"ℹ️ {unmapped} rows ({pct:.2f}%) have no team_id mapping in {TEAM_DIRECTORY_FILE}")

        # Inject columns and save
        batters["team_id"] = team_id_series
        batters["team"] = normalized_team_name  # keep existing behavior: official team names

        logger.info(f"✅ Normalized {len(batters)} rows and injected team_id. Saving to {OUTPUT_FILE}...")
        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        batters.to_csv(OUTPUT_FILE, index=False)
        write_summary_line(True, len(batters))

    except Exception as e:
        logger.error(f"❌ Error during normalization: {e}")
        write_summary_line(False, 0)

if __name__ == "__main__":
    main()
