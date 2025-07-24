# scripts/clean_and_preprocess_pitchers.py

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import sys

# Setup logging
log_dir = Path("summaries")
log_dir.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = log_dir / f"clean_and_preprocess_pitchers_{timestamp}.log"

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)
logging.getLogger().setLevel(logging.INFO)

# Input/output paths
INPUT_DIR = Path("data/end_chain")
OUTPUT_DIR = INPUT_DIR / "cleaned"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "pitchers_home": INPUT_DIR / "pitchers_home_weather_park.csv",
    "pitchers_away": INPUT_DIR / "pitchers_away_weather_park.csv"
}

REQUIRED_COLUMNS = ["name", "team", "innings_pitched", "strikeouts", "walks", "earned_runs"]

def clean_pitcher_data(df: pd.DataFrame, label: str) -> pd.DataFrame:
    logging.info(f"🧼 Cleaning data for {label}...")

    # Ensure required columns exist
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {label}: {missing}")

    # Convert stat columns to numeric
    stat_cols = [col for col in REQUIRED_COLUMNS if col not in ["name", "team"]]
    for col in stat_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill NaNs with 0
    df[stat_cols] = df[stat_cols].fillna(0)

    # Clean team name
    df["team"] = df["team"].astype(str).str.strip().str.title()

    # Feature engineering: ERA = (Earned Runs / Innings Pitched) * 9
    if "ERA" not in df.columns:
        df["ERA"] = df.apply(
            lambda row: (row["earned_runs"] / row["innings_pitched"] * 9)
            if row["innings_pitched"] > 0 else 0,
            axis=1
        ).round(2)

    logging.info(f"✅ Cleaned {label}. Rows: {len(df)}")
    return df

def main():
    try:
        for label, path in FILES.items():
            if not path.exists():
                logging.error(f"❌ Missing file: {path}")
                continue

            df = pd.read_csv(path)
            cleaned = clean_pitcher_data(df, label)
            out_file = OUTPUT_DIR / f"{label}_cleaned.csv"
            cleaned.to_csv(out_file, index=False)
            logging.info(f"💾 Saved cleaned data to {out_file}")

        logging.info("🏁 Pitcher data cleaning complete.")

    except Exception as e:
        logging.error(f"❌ Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
