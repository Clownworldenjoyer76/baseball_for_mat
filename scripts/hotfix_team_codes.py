# scripts/hotfix_team_codes.py
import sys
import logging
from pathlib import Path
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
LOG = logging.getLogger("hotfix_team_codes")

INPUT_PATH = Path("data/raw/todaysgames_normalized.csv")

# Only fix what we know is breaking downstream
MAP = {
    # CHW variations → WhiteSox (your downstream keys expect this format)
    "CWS": "WhiteSox", "Cws": "WhiteSox", "cws": "WhiteSox",
    # ARI variations → Diamondbacks
    "AZ": "Diamondbacks", "Az": "Diamondbacks", "az": "Diamondbacks",
}

COLUMNS = ("home_team", "away_team")

def main():
    if not INPUT_PATH.exists():
        LOG.error("Missing file: %s", INPUT_PATH)
        sys.exit(0)  # don't fail the whole pipeline

    df = pd.read_csv(INPUT_PATH)
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        LOG.error("Missing expected column(s) in %s: %s", INPUT_PATH, missing)
        sys.exit(0)

    # Normalize to strings and strip (idempotent)
    for c in COLUMNS:
        df[c] = df[c].astype(str).str.strip()

    # Count before/after for quick sanity
    before = {k: ((df[COLUMNS[0]] == k) | (df[COLUMNS[1]] == k)).sum() for k in MAP.keys()}

    for c in COLUMNS:
        df[c] = df[c].replace(MAP)

    after = {v: ((df[COLUMNS[0]] == v) | (df[COLUMNS[1]] == v)).sum() for v in set(MAP.values())}

    df.to_csv(INPUT_PATH, index=False)
    LOG.info("Hotfix applied to %s", INPUT_PATH)
    if any(before.values()):
        LOG.info("Replaced counts (source codes): %s", before)
        LOG.info("Counts of normalized targets: %s", after)
    else:
        LOG.info("No known bad codes found; file already clean.")

if __name__ == "__main__":
    main()
