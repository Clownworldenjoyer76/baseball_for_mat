import pandas as pd
from pathlib import Path
from pydantic import BaseModel, ValidationError
import logging

# ─── Configuration ───────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

SUMMARY_FILE = Path("summaries/summary.txt")
BATTERS_FILE = Path("data/tagged/batters_normalized.csv")
PITCHERS_FILE = Path("data/tagged/pitchers_normalized.csv")

# ─── Schemas ─────────────────────────────────────────────────
class Player(BaseModel):
    name: str
    team: str
    type: str

# ─── Main ───────────────────────────────────────────────────
def validate_file(path: Path, label: str):
    logging.info(f"🔍 Validating: {path}")
    if not path.exists():
        logging.error(f"{label} file not found: {path}")
        return False

    df = pd.read_csv(path)
    passed = True

    for _, row in df.iterrows():
        try:
            Player(**row)
        except ValidationError as e:
            logging.error(f"{label} validation failed: {e}")
            passed = False

    status = "PASSED" if passed else "FAILED"
    logging.info(f"✅ All {label} rows passed schema validation." if passed else f"❌ {label} schema validation failed.")

    with open(SUMMARY_FILE, "a") as f:
        f.write(f"{label} validation: {status} ({len(df)} rows)\n")

    return passed

if __name__ == "__main__":
    validate_file(BATTERS_FILE, "batters")
    validate_file(PITCHERS_FILE, "pitchers")
