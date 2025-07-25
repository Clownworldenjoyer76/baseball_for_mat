import pandas as pd
from pathlib import Path
import unicodedata
import re
import logging
from datetime import datetime
import sys

# Define paths
INPUT_FILE = Path("data/end_chain/cleaned/games_cleaned.csv")
REF_FILE = Path("data/cleaned/pitchers_normalized_cleaned.csv")
OUTPUT_FILE = Path("data/end_chain/cleaned/pitchers_xtra_updated.csv")

# Columns for output file
OUTPUT_COLUMNS = [
    "pitcher_home", "pitcher_away", "team", "name",
    "innings_pitched", "strikeouts", "walks", "earned_runs"
]

# --- Logging Setup ---
LOG_DIR = Path("summaries")
LOG_DIR.mkdir(parents=True, exist_ok=True) # Ensure the log directory exists

# Define unique log file names with timestamps
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
INFO_LOG_FILE = LOG_DIR / f"pitchers_xtra_update_info_{timestamp}.log"
ERROR_LOG_FILE = LOG_DIR / f"pitchers_xtra_update_error_{timestamp}.log"

# Get the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Set default level for the logger

# Clear existing handlers to prevent duplicate output if script is run multiple times in same session
if logger.hasHandlers():
    logger.handlers.clear()

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO) # Console shows INFO and above
console_formatter = logging.Formatter("%(levelname)s: %(message)s")
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Info file handler (for INFO and higher messages)
info_file_handler = logging.FileHandler(INFO_LOG_FILE)
info_file_handler.setLevel(logging.INFO)
info_file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
info_file_handler.setFormatter(info_file_formatter)
logger.addHandler(info_file_handler)

# Error file handler (for ERROR and higher messages)
error_file_handler = logging.FileHandler(ERROR_LOG_FILE)
error_file_handler.setLevel(logging.ERROR) # Only ERROR and CRITICAL messages go here
error_file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
error_file_handler.setFormatter(error_file_formatter)
logger.addHandler(error_file_handler)
# --- End Logging Setup ---


def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def capitalize_mc_names(text):
    return re.sub(r'\b(mc)([a-z])([a-z]*)\b',
                  lambda m: m.group(1).capitalize() + m.group(2).upper() + m.group(3).lower(),
                  text, flags=re.IGNORECASE)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip().rstrip(',')
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = capitalize_mc_names(name)

    suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
    tokens = name.split()

    if len(tokens) >= 2:
        first = tokens[0]
        possible_suffix = tokens[-1].lower().strip(".")
        if possible_suffix in suffixes and len(tokens) > 2:
            last = " ".join(tokens[1:-1])
            suffix = tokens[-1]
            return f"{last} {suffix}, {first}"
        else:
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
    return name.title()

def main():
    logger.info(f"Starting script: {Path(__file__).name}")
    logger.info(f"Input file: {INPUT_FILE}")
    logger.info(f"Reference file: {REF_FILE}")
    logger.info(f"Output file: {OUTPUT_FILE}")

    if not INPUT_FILE.exists():
        logger.error(f"❌ Input file not found: {INPUT_FILE}")
        sys.exit(1) # Exit with an error code
    if not REF_FILE.exists():
        logger.error(f"❌ Reference file not found: {REF_FILE}")
        sys.exit(1) # Exit with an error code

    try:
        df = pd.read_csv(INPUT_FILE)
        ref = pd.read_csv(REF_FILE)
    except FileNotFoundError as e: # Catch if files disappear after existence check
        logger.error(f"Error loading files: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred while reading CSV files: {e}")
        sys.exit(1)


    required_cols = {"pitcher_home", "pitcher_away"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        logger.error(f"❌ Input file missing required columns: {missing}")
        sys.exit(1)

    # Create output DataFrame with blanks
    output_df = pd.DataFrame(columns=OUTPUT_COLUMNS)
    output_df["pitcher_home"] = df["pitcher_home"]
    output_df["pitcher_away"] = df["pitcher_away"]

    for col in ["innings_pitched", "strikeouts", "walks", "earned_runs"]:
        output_df[col] = ""

    # Normalize name from pitcher_home
    output_df["name"] = output_df["pitcher_home"].astype(str).apply(normalize_name)

    # Normalize name in reference file
    # Check if 'last_name, first_name' exists in ref, otherwise assume 'name'
    if 'last_name, first_name' in ref.columns:
        ref["name"] = ref["last_name, first_name"].astype(str).apply(normalize_name)
    elif 'name' in ref.columns:
        logger.info("Reference file already has a 'name' column. Normalizing it.")
        ref["name"] = ref["name"].astype(str).apply(normalize_name)
    else:
        logger.error("❌ Reference file must contain either 'last_name, first_name' or 'name' column for normalization.")
        sys.exit(1)

    ref_team_lookup = ref[["name", "team"]]

    logger.info("Merging team data into output DataFrame...")
    # Merge team into output_df
    output_df = output_df.merge(ref_team_lookup, on="name", how="left")

    # Debug info if team is missing
    missing_teams = output_df["team"].isna().sum()
    if missing_teams > 0:
        logger.warning(f"🔎 Missing team for {missing_teams} rows in the merged data.")
        # Optionally, log the actual names that are missing teams
        # logger.warning(f"Names with missing teams: {output_df[output_df['team'].isna()]['name'].unique().tolist()}")
    else:
        logger.info("✅ All teams successfully merged.")

    # Save final file
    try:
        output_df = output_df[OUTPUT_COLUMNS]  # enforce correct column order
        output_dir_parent = OUTPUT_FILE.parent
        output_dir_parent.mkdir(parents=True, exist_ok=True) # Ensure output directory exists before saving
        output_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"✅ Created {OUTPUT_FILE} with shape {output_df.shape}")
    except Exception as e:
        logger.error(f"❌ Failed to save output file {OUTPUT_FILE}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"A critical unhandled error occurred: {e}", exc_info=True)
        sys.exit(1)

