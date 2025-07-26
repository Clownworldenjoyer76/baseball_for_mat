import pandas as pd
import unicodedata
import re
from pathlib import Path
from datetime import datetime
import logging # Using logging for better output control

# ─── Configuration ───────────────────────────────────────────

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# File Paths
MASTER_FILE = Path("data/processed/player_team_master.csv")
BATTER_FILE = Path("data/normalized/batters_normalized.csv")
PITCHER_FILE = Path("data/normalized/pitchers_normalized.csv")
OUTPUT_FOLDER = Path("data/tagged")
OUTPUT_TOTALS_FILE = Path("data/output/player_totals.txt")

# Ensure output directories exist
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
OUTPUT_TOTALS_FILE.parent.mkdir(parents=True, exist_ok=True)

# Common suffixes to strip from last names
# Pre-compile regex for suffix removal for efficiency, case-insensitive.
# Word boundary \b ensures it matches full words like "jr", not "jrsomething".
# Optional trailing dot \.?
RE_SUFFIX_REMOVE = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?", re.IGNORECASE)

# Pre-compile regex for non-word/space/comma characters
RE_NON_ALPHANUM_OR_SPACE_OR_COMMA = re.compile(r"[^\w\s,]")
RE_MULTI_SPACE = re.compile(r"\s+")

# ─── Name Normalization Utilities ────────────────────────────

def strip_accents(text: str) -> str:
    """Removes diacritical marks (accents) from a string."""
    # Already relatively efficient, but ensures type check.
    if not isinstance(text, str):
        return ""
    # NFD: Canonical Decomposition, Mn: Mark, Nonspacing
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name_series(names_series: pd.Series) -> pd.Series:
    """
    Normalizes a pandas Series of names using vectorized operations where possible.
    Converts to "Last, First" format, handles accents, suffixes, and cleanup.
    """
    # Ensure all values are strings for consistent processing, fill NaNs with empty string
    normalized_series = names_series.astype(str).fillna("")

    # Step 1: Basic cleaning and accent stripping
    normalized_series = normalized_series.apply(strip_accents) # Still best for unicodedata
    normalized_series = normalized_series.str.replace("’", "", regex=False)
    normalized_series = normalized_series.str.replace("`", "", regex=False)
    normalized_series = normalized_series.str.replace(RE_NON_ALPHANUM_OR_SPACE_OR_COMMA, "", regex=True)
    normalized_series = normalized_series.str.replace(RE_MULTI_SPACE, " ", regex=True)
    normalized_series = normalized_series.str.strip() # Don't lower here, do it selectively for splitting

    # Step 2: Split names into Last and First components
    # Using a temporary DataFrame to hold split parts
    name_parts_df = pd.DataFrame(index=normalized_series.index)
    name_parts_df['raw_name'] = normalized_series

    # Identify names with commas
    has_comma_mask = name_parts_df['raw_name'].str.contains(",", na=False)

    # Process names with commas (assume 'LAST, FIRST')
    commas_split = name_parts_df.loc[has_comma_mask, 'raw_name'].str.split(",", n=1, expand=True)
    if not commas_split.empty:
        name_parts_df.loc[has_comma_mask, 'last'] = commas_split[0].str.strip()
        name_parts_df.loc[has_comma_mask, 'first'] = commas_split[1].str.strip()

    # Process names without commas (assume 'FIRST LAST' or 'FIRST MIDDLE LAST')
    no_comma_names = name_parts_df.loc[~has_comma_mask, 'raw_name']
    space_split = no_comma_names.str.split(" ", n=1, expand=True) # Split only on first space

    # Assign parts: first token is last name, rest is first name
    name_parts_df.loc[~has_comma_mask, 'last'] = space_split[0].str.strip()
    name_parts_df.loc[~has_comma_mask, 'first'] = space_split[1].fillna('').str.strip() # Handle single name case

    # Step 3: Apply Title Case and Suffix Removal
    name_parts_df['last'] = name_parts_df['last'].str.title()
    name_parts_df['first'] = name_parts_df['first'].str.title()

    # Remove suffixes from the last name. Using apply here is necessary as RE_SUFFIX_REMOVE.sub is not vectorized directly for pandas series.
    # However, it's efficient due to pre-compiled regex.
    name_parts_df['last'] = name_parts_df['last'].apply(lambda x: RE_SUFFIX_REMOVE.sub("", x).strip())

    # Step 4: Reconstruct the "Last, First" format
    # Handle cases where 'first' might be empty (e.g., single-name players)
    final_names = name_parts_df.apply(
        lambda row: f"{row['last']}" if not row['first'] else f"{row['last']}, {row['first']}",
        axis=1
    )
    return final_names

# ─── Data Processing Functions ───────────────────────────

def load_csv_safely(file_path: Path, column_to_check: str = None) -> pd.DataFrame:
    """Loads a CSV file safely, with error handling and optional column check."""
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        return pd.DataFrame() # Return empty DataFrame on missing file

    try:
        df = pd.read_csv(file_path)
        if column_to_check and column_to_check not in df.columns:
            logging.warning(f"Column '{column_to_check}' not found in {file_path}.")
            return pd.DataFrame() # Return empty DataFrame if critical column is missing
        return df
    except pd.errors.EmptyDataError:
        logging.warning(f"File is empty: {file_path}. Returning empty DataFrame.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error loading {file_path}: {e}")
        return pd.DataFrame()

def tag_and_save_players(input_file_path: Path, player_type: str, master_df: pd.DataFrame) -> pd.DataFrame:
    """
    Tags players in the input file by merging with the master DataFrame
    and saves the result to the output folder.
    """
    logging.info(f"Tagging {player_type} data from {input_file_path}...")

    df_to_tag = load_csv_safely(input_file_path, "last_name, first_name")
    if df_to_tag.empty:
        logging.warning(f"Skipping {player_type} due to issues loading or missing column in {input_file_path}.")
        return pd.DataFrame()

    # Normalize the name column in the current DataFrame
    df_to_tag["last_name, first_name"] = normalize_name_series(df_to_tag["last_name, first_name"])

    # Perform the merge operation
    merged_df = df_to_tag.merge(
        master_df,
        how="left",
        left_on="last_name, first_name",
        right_on="name",
        suffixes=("", "_master")
    )

    # Log and drop unmatched rows
    unmatched = merged_df[merged_df["team"].isna() | merged_df["type"].isna()]
    if not unmatched.empty:
        unique_unmatched_names = unmatched["last_name, first_name"].drop_duplicates()
        logging.warning(
            f"{len(unmatched)} {player_type} rows ({len(unique_unmatched_names)} unique names) "
            f"had no team/type match and will be dropped. Examples:\n"
            f"{unique_unmatched_names.to_string(index=False)}"
        )
    
    # Drop rows that couldn't be matched to master data
    merged_clean = merged_df.dropna(subset=["team", "type"]).copy() # .copy() to prevent SettingWithCopyWarning

    if merged_clean.empty:
        logging.warning(f"No {player_type} rows matched after tagging and dropping unmatched. Output file will be empty.")
        return pd.DataFrame()

    # Reorder columns: Move key columns to the front for better visibility
    key_cols_to_front = ["name", "player_id", "team", "type"]
    # Ensure all key_cols_to_front actually exist in merged_clean before reordering
    existing_key_cols = [col for col in key_cols_to_front if col in merged_clean.columns]
    other_cols = [col for col in merged_clean.columns if col not in existing_key_cols]
    merged_clean = merged_clean[existing_key_cols + other_cols]

    # Save the processed DataFrame
    output_file_path = OUTPUT_FOLDER / input_file_path.name # Use Path object for joining
    try:
        merged_clean.to_csv(output_file_path, index=False)
        logging.info(f"✅ Tagged {player_type} saved to: {output_file_path} ({len(merged_clean)} rows)")
    except Exception as e:
        logging.error(f"Error saving tagged {player_type} data to {output_file_path}: {e}")
        return pd.DataFrame() # Return empty if save fails

    return merged_clean

# ─── Main Execution ───────────────────────────────────────

if __name__ == "__main__":
    logging.info("Starting player tagging process...")

    # Load and normalize master file
    master_players_df = load_csv_safely(MASTER_FILE, "name")
    if master_players_df.empty:
        logging.critical("Master player file could not be loaded or is empty. Exiting.")
        exit(1) # Exit if master data is critical and missing

    master_players_df["name"] = normalize_name_series(master_players_df["name"])
    # Optional: Deduplicate master_df after normalization if names might not be unique
    # master_players_df.drop_duplicates(subset=["name"], inplace=True)
    # logging.info(f"Master file normalized and deduplicated: {len(master_players_df)} unique names.")


    # Process batter and pitcher files
    all_tagged_dfs = {}
    
    # Using a list of tuples to process files in a loop, reducing repetition
    files_to_tag = [
        (BATTER_FILE, "batters"),
        (PITCHER_FILE, "pitchers")
    ]

    for file_path, player_type_label in files_to_tag:
        tagged_df = tag_and_save_players(file_path, player_type_label, master_players_df)
        all_tagged_dfs[player_type_label] = tagged_df

    # Write totals summary
    try:
        with open(OUTPUT_TOTALS_FILE, "w") as f:
            f.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            for player_type_label, df in all_tagged_dfs.items():
                f.write(f"Tagged {player_type_label.capitalize()}: {len(df)}\n")
        logging.info(f"Summary totals written to {OUTPUT_TOTALS_FILE}")
    except Exception as e:
        logging.error(f"Error writing totals summary to {OUTPUT_TOTALS_FILE}: {e}")

    logging.info("Player tagging process completed.")

