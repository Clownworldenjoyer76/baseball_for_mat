# scripts/lineupcleaned.py

import pandas as pd
import unicodedata
import re
from pathlib import Path
import logging
import sys

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- File Paths ---
LINEUPS_IN = Path("data/raw/lineups.csv")
MASTER_IN = Path("data/processed/player_team_master.csv")
# Recommended output path for cleaned data
LINEUPS_OUT = Path("data/cleaned/lineups_cleaned.csv")

# --- Column Names as they exist in your files ---
MASTER_NAME_COL_RAW = "name"
LINEUPS_FULL_NAME_COL_RAW = "last_name, first_name" # The literal column name

# New internal column for normalized full name for matching
NORMALIZED_MATCH_COL = "normalized_full_name_for_match"

# --- Regex Patterns ---
RE_NON_ALPHANUM_OR_SPACE_OR_COMMA = re.compile(r"[^\w\s,]")
RE_MULTI_SPACE = re.compile(r"\s+")

# --- Normalization Helpers ---
def strip_accents(text: str) -> str:
    """Removes accents from a string."""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_string_for_comparison(text: str) -> str:
    """Applies general text normalization for comparison (strips accents, special chars, standardizes spaces)."""
    text = str(text).strip() # Ensure it's a string and strip initial whitespace
    text = strip_accents(text)
    text = text.replace("’", "").replace("`", "")
    text = RE_NON_ALPHANUM_OR_SPACE_OR_COMMA.sub("", text) # Removes non-alphanum, non-space, non-comma
    text = RE_MULTI_SPACE.sub(" ", text).strip()
    return text

def convert_first_last_to_last_first(full_name: str) -> str:
    """Converts a 'First Last' string to 'Last, First' format."""
    name_parts = str(full_name).strip().split(' ')
    if len(name_parts) >= 2:
        # Assuming the last part is the last name, and rest is first name
        last_name = name_parts[-1]
        first_name = ' '.join(name_parts[:-1])
        return f"{last_name}, {first_name}"
    else:
        # If only one part or unusual format, return as is (might be just a last name)
        return full_name

# --- Main Logic ---
def main():
    logger.info("📥 Loading lineups and master file...")
    try:
        df = pd.read_csv(LINEUPS_IN)
        master = pd.read_csv(MASTER_IN)
    except FileNotFoundError as e:
        logger.critical(f"❌ Required input file not found: {e.filename}")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        logger.critical("❌ One of the input files is empty.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ Error loading CSV files: {e}")
        sys.exit(1)

    # Normalize columns (strip spaces from names)
    df.columns = df.columns.str.strip()
    master.columns = master.columns.str.strip()
    logger.info(f"Lineups DF raw columns: {df.columns.tolist()}")
    logger.info(f"Master DF raw columns: {master.columns.tolist()}")

    # --- Validate required name columns ---
    if MASTER_NAME_COL_RAW not in master.columns:
        logger.critical(f"❌ Missing required name column '{MASTER_NAME_COL_RAW}' in player_team_master.csv.")
        sys.exit(1)
    if LINEUPS_FULL_NAME_COL_RAW not in df.columns:
        logger.critical(f"❌ Missing required name column '{LINEUPS_FULL_NAME_COL_RAW}' in lineups.csv.")
        sys.exit(1)

    # --- Process Master file names (already 'Last, First') ---
    master[NORMALIZED_MATCH_COL] = master[MASTER_NAME_COL_RAW].apply(normalize_string_for_comparison)
    logger.info(f"Processed master names into '{NORMALIZED_MATCH_COL}'.")

    # --- Process Lineups file names ('First Last' to 'Last, First') ---
    # First, convert 'First Last' to 'Last, First' for consistency
    df['temp_last_first_format'] = df[LINEUPS_FULL_NAME_COL_RAW].apply(convert_first_last_to_last_first)
    # Then, apply general normalization for comparison
    df[NORMALIZED_MATCH_COL] = df['temp_last_first_format'].apply(normalize_string_for_comparison)
    df.drop(columns=['temp_last_first_format'], inplace=True) # Clean up temp column
    logger.info(f"Processed lineup names into '{NORMALIZED_MATCH_COL}'.")

    # Keep only rows with matching normalized names
    allowed_names = set(master[NORMALIZED_MATCH_COL])
    df_clean = df[df[NORMALIZED_MATCH_COL].isin(allowed_names)].copy()
    logger.info(f"Filtered lineups. Kept {len(df_clean)} rows matching master names.")

    # Save cleaned output
    LINEUPS_OUT.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(LINEUPS_OUT, index=False)
    logger.info(f"✅ Saved cleaned lineups to {LINEUPS_OUT} with {len(df_clean)} rows")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)
