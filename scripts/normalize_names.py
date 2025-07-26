# scripts/normalize_names.py

import pandas as pd
import unicodedata
import re
from pathlib import Path
import os # Import os for path existence check and error handling

# --- Configuration ---
BATT_FILE = Path("data/normalized/batters_normalized.csv")
PITCH_FILE = Path("data/normalized/pitchers_normalized.csv")
TARGET_COLUMN = "last_name, first_name"

# Common suffixes to strip from last names
# Using a regex pattern for suffixes for more efficient removal in vectorized operations
# Word boundary \b ensures it only matches full words like "jr", not "jrsomething"
# Added \.? to optionally match trailing dot. Regex is case-insensitive.
SUFFIXES_PATTERN = r"\b(jr|sr|ii|iii|iv|v)\b\.?"

# Pre-compile regex patterns for efficiency
RE_NON_ALPHANUM_OR_SPACE_OR_COMMA = re.compile(r"[^\w\s,]")
RE_MULTI_SPACE = re.compile(r"\s+")
RE_SUFFIX_REMOVE = re.compile(SUFFIXES_PATTERN, re.IGNORECASE)

# --- Helper Functions ---

def strip_accents(text: str) -> str:
    """Removes diacritical marks (accents) from a string."""
    if not isinstance(text, str):
        return ""
    # NFD: Canonical Decomposition, Mn: Mark, Nonspacing
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def process_name_series(name_series: pd.Series) -> pd.Series:
    """
    Normalizes a pandas Series of names.
    Applies cleaning, splits into last/first, and reorders to "Last, First".
    """
    # Convert to string and fill potential NaNs with empty strings for consistent processing
    processed_series = name_series.astype(str).fillna("")

    # 1. Basic Cleaning and Normalization (vectorized)
    processed_series = processed_series.apply(strip_accents) # Still best via apply for unicodedata
    processed_series = processed_series.str.replace("’", "", regex=False)
    processed_series = processed_series.str.replace("`", "", regex=False)
    processed_series = processed_series.str.replace(RE_NON_ALPHANUM_OR_SPACE_OR_COMMA, "", regex=True)
    processed_series = processed_series.str.replace(RE_MULTI_SPACE, " ", regex=True)
    processed_series = processed_series.str.strip().str.lower()

    # 2. Split into last and first names based on presence of a comma
    # Create temporary columns for last_name and first_name
    temp_df = pd.DataFrame(index=processed_series.index)
    temp_df['full_name_clean'] = processed_series

    # Names with commas: assumed 'last, first'
    has_comma_mask = temp_df['full_name_clean'].str.contains(",", na=False)
    commas_split = temp_df.loc[has_comma_mask, 'full_name_clean'].str.split(",", n=1, expand=True)
    
    # Assign to temp_df, handling cases where there might be no commas_split results
    if not commas_split.empty:
        temp_df.loc[has_comma_mask, 'last_name_temp'] = commas_split[0].str.strip()
        temp_df.loc[has_comma_mask, 'first_name_temp'] = commas_split[1].str.strip()

    # Names without commas: assumed 'first last' or 'first middle last'
    no_comma_series = temp_df.loc[~has_comma_mask, 'full_name_clean']
    
    # Split by first space: first part is assumed last, rest is first
    space_split = no_comma_series.str.split(" ", n=1, expand=True)

    # For names like "John Doe", "Doe" is last, "John" is first
    # For names like "Madonna", "Madonna" is full name, no split
    
    # If there's a second part (i.e., it's "First Last")
    valid_split_mask = space_split[1].notna()
    temp_df.loc[~has_comma_mask & valid_split_mask, 'last_name_temp'] = space_split[0].str.strip()
    temp_df.loc[~has_comma_mask & valid_split_mask, 'first_name_temp'] = space_split[1].str.strip()

    # If there's no second part (i.e., it's a single name like "Madonna")
    single_name_mask = space_split[1].isna()
    temp_df.loc[~has_comma_mask & single_name_mask, 'last_name_temp'] = space_split[0].str.strip()
    temp_df.loc[~has_comma_mask & single_name_mask, 'first_name_temp'] = "" # No first name

    # 3. Apply title case and remove suffixes from last name
    # Apply title case to both parts
    temp_df['last_name_temp'] = temp_df['last_name_temp'].str.title()
    temp_df['first_name_temp'] = temp_df['first_name_temp'].str.title()

    # Remove suffixes from last name (still needs per-element logic, but using compiled regex)
    temp_df['last_name_temp'] = temp_df['last_name_temp'].apply(lambda x: RE_SUFFIX_REMOVE.sub("", x).strip())
    
    # 4. Reconstruct the "Last, First" format
    # Handle cases where first_name might be empty (single name like "Madonna")
    normalized_series = temp_df.apply(
        lambda row: f"{row['last_name_temp']}" if not row['first_name_temp'] else f"{row['last_name_temp']}, {row['first_name_temp']}",
        axis=1
    )

    return normalized_series

# --- Main Execution ---

if __name__ == "__main__":
    files_to_process = {
        "batters": BATT_FILE,
        "pitchers": PITCH_FILE
    }

    for label, file_path in files_to_process.items():
        print(f"--- Processing {label.capitalize()} file: {file_path} ---")

        if not os.path.exists(file_path):
            print(f"❌ Error: File not found at {file_path}. Skipping.")
            continue

        try:
            df = pd.read_csv(file_path)

            if TARGET_COLUMN in df.columns:
                original_rows = len(df)
                df[TARGET_COLUMN] = process_name_series(df[TARGET_COLUMN])
                
                # Optional: Remove duplicates after normalization if that's desired.
                # The prompt did not explicitly ask for deduplication in this script,
                # but if names are meant to be unique *after* normalization, add this:
                # df.drop_duplicates(subset=[TARGET_COLUMN], inplace=True)
                # print(f"🔍 Removed {original_rows - len(df)} duplicate rows after normalization.")
                
                df.to_csv(file_path, index=False)
                print(f"✅ Successfully cleaned and saved {label} data to {file_path}.")
            else:
                print(f"⚠️ Warning: Column '{TARGET_COLUMN}' not found in {file_path}. Skipping normalization for this file.")

        except pd.errors.EmptyDataError:
            print(f"⚠️ Warning: {file_path} is empty. No data to process.")
        except Exception as e:
            print(f"❌ An unexpected error occurred while processing {file_path}: {e}")

