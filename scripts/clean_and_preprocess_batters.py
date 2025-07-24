# scripts/clean_and_preprocess_batters.py

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import sys

# Setup logging
log_dir = Path("summaries")
log_dir.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = log_dir / f"clean_and_preprocess_batters_{timestamp}.log"

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

# Input paths
INPUT_DIR = Path("data/end_chain")
OUTPUT_DIR = INPUT_DIR / "cleaned"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "batters_home": INPUT_DIR / "batters_home_weather_park.csv",
    "batters_away": INPUT_DIR / "batters_away_weather_park.csv"
}

REQUIRED_COLUMNS = ["name", "team", "ab", "pa", "hit", "home_run", "walk", "strikeout"]

def clean_batter_data(df: pd.DataFrame, label: str) -> pd.DataFrame:
    logging.info(f"🔧 Cleaning data for {label}...")

    # Drop columns ending in _y
    drop_cols = [col for col in df.columns if col.endswith("_y")]
    if drop_cols:
        logging.info(f"🗑️ Dropping columns: {drop_cols}")
        df = df.drop(columns=drop_cols)

    # Rename columns ending in _x
    rename_cols = {col: col[:-2] for col in df.columns if col.endswith("_x")}
    if rename_cols:
        logging.info(f"🔄 Renaming columns: {rename_cols}")
        df = df.rename(columns=rename_cols)

    # Drop columns with no header (unnamed) AND no data
    unnamed_blank_cols = [
        col for col in df.columns
        if col.startswith("Unnamed") and df[col].isna().all()
    ]
    if unnamed_blank_cols:
        logging.info(f"🧹 Removing unnamed empty columns: {unnamed_blank_cols}")
        df = df.drop(columns=unnamed_blank_cols)

    # Ensure required columns are present
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {label} data: {missing_cols}")

    # Convert stat columns to numeric
    stat_cols = [col for col in df.columns if col in REQUIRED_COLUMNS and col not in ['name', 'team']]
    for col in stat_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill NaNs with 0s in stat columns
    df[stat_cols] = df[stat_cols].fillna(0)

    # Strip and normalize team column
    df["team"] = df["team"].astype(str).str.strip().str.title()

    # Feature engineering: batting average if not already present
    if "batting_avg" not in df.columns:
        df["batting_avg"] = df["hit"] / df["ab"].replace({0: pd.NA})
        df["batting_avg"] = df["batting_avg"].fillna(0).round(3)

    logging.info(f"✅ Finished cleaning {label}. Rows: {len(df)}")
    return df

def main():
    try:
        for label, file_path in FILES.items():
            if not file_path.exists():
                logging.error(f"❌ Missing input file: {file_path}")
                continue

            df = pd.read_csv(file_path)
            cleaned_df = clean_batter_data(df, label)
            output_file = OUTPUT_DIR / f"{label}_cleaned.csv"
            cleaned_df.to_csv(output_file, index=False)
            logging.info(f"💾 Saved cleaned data to {output_file}")
        
        logging.info("🏁 Batter data cleaning complete.")

    except Exception as e:
        logging.error(f"❌ Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
