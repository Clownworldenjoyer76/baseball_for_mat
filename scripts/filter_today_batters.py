import pandas as pd
from pathlib import Path
import unicodedata
import re

LINEUPS_FILE = "data/cleaned/lineups_cleaned.csv"
BATTERS_FILE = "data/cleaned/batters_normalized_cleaned.csv"
PITCHERS_FILE = "data/cleaned/pitchers_normalized_cleaned.csv" # Path to the pitchers file
OUTPUT_FILE = "data/cleaned/batters_today.csv"
UNMATCHED_FILE = "data/cleaned/unmatched_batters.txt"
PITCHERS_IN_LINEUPS_FILE = "data/cleaned/pitchers_in_lineups.txt" # File to list identified pitchers

# --- Utility Functions ---
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)

    # Specific handling for initials with periods (e.g., J.P. -> JP, J.T. -> JT)
    # This converts "J.P. Crawford" to "JP Crawford" and "J.T. Realmuto" to "JT Realmuto"
    name = re.sub(r'\bJ\.P\.', 'JP', name, flags=re.IGNORECASE)
    name = re.sub(r'\bJ\.T\.', 'JT', name, flags=re.IGNORECASE)

    # Remove any characters that are not word characters, whitespace, comma, or period.
    name = re.sub(r"[^\w\s,\.]", "", name)

    # Replace multiple spaces with a single space and strip leading/trailing whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # Define a mapping for full first names to their initial forms (for these specific cases)
    # Keys should be lowercased for case-insensitive matching
    full_to_initials_mapping = {
        "john paul": "JP",
        "jacob": "JT",
        # Add other specific full name to initial mappings here if needed in the future
    }

    # Process the name to "Last, First" format first
    last_name = ""
    first_name = ""

    if "," in name:
        parts = name.split(",")
        if len(parts) == 2:
            last_name = parts[0].strip()
            first_name = parts[1].strip()
    else: # Assume "First Last" format
        tokens = name.split()
        if len(tokens) >= 2:
            first_name = tokens[0]
            last_name = " ".join(tokens[1:])
        else: # Handle single word names or unexpected formats
            return name.title() # Return as is, title cased

    # Apply the full_to_initials_mapping to the extracted first name
    # Convert first_name to lowercase for lookup in the mapping
    mapped_first_name = full_to_initials_mapping.get(first_name.lower(), first_name)

    # If the mapped name is initials (e.g., 'JP', 'JT'), keep it uppercase.
    # Otherwise, title case the first and last names.
    final_first_name = mapped_first_name.upper() if mapped_first_name in ["JP", "JT"] else mapped_first_name.title()
    final_last_name = last_name.title()

    return f"{final_last_name}, {final_first_name}"


# --- Manual Overrides for Known Variants (Applied to raw names before initial normalization, if applicable) ---
# Note: For `normalized_full_name_for_match`, these are less relevant as that column should already be normalized.
NAME_OVERRIDES = {
    "Tatis Jr., Fernando": "Tatis, Fernando",
    "Witt Jr., Bobby": "Witt, Bobby",
    "Crawford, John Paul": "Crawford, Jp",
    "Realmuto, Jacob": "Realmuto, Jt",
    "P Muncy, Max": "Muncy, Max",
    "V Garcia, Luis": "Garcia, Luis",
    "O'Hearn, Ryan": "Ohearn, Ryan",
    "O'Hoppe, Logan": "Ohoppe, Logan",
    "Crow-Armstrong, Pete": "Crowarmstrong, Pete",
    "Encarnacion-Strand, Christian": "Encarnacionstrand, Christian",
    "Kiner-Falefa, Isiah": "Kinerfalefa, Isiah",
    "De La Cruz, Elly": "De La Cruz, Elly",  # already proper if exists
    # Add any other RAW name overrides if necessary
}

# --- Main ---
def main():
    print("📥 Loading lineups, batters, and pitchers...")
    try:
        lineups_df = pd.read_csv(LINEUPS_FILE)
        batters_df = pd.read_csv(BATTERS_FILE)
        pitchers_df = pd.read_csv(PITCHERS_FILE)
    except Exception as e:
        raise RuntimeError(f"❌ Failed to load input files: {e}")

    lineups_match_column = 'normalized_full_name_for_match'

    if lineups_match_column not in lineups_df.columns or \
       'name' not in batters_df.columns or \
       'name' not in pitchers_df.columns:
        raise ValueError(
            f"❌ Missing required columns. Ensure '{lineups_match_column}' is in lineups file, "
            f"'name' in batters file, and 'name' in pitchers file."
        )

    # Normalize batter and pitcher names
    batters_df['normalized_name'] = batters_df['name'].astype(str).apply(normalize_name)
    pitchers_df['normalized_name'] = pitchers_df['name'].astype(str).apply(normalize_name)

    # Normalize the names from the lineups file using the same normalize_name function
    lineups_df['final_match_name'] = lineups_df[lineups_match_column].astype(str).apply(normalize_name)

    # Get sets of normalized names for efficient lookup
    expected_names_from_lineups = set(lineups_df['final_match_name'])
    pitcher_names_set = set(pitchers_df['normalized_name'])

    # Identify pitchers within the lineup names and separate them
    pitchers_in_lineups = expected_names_from_lineups.intersection(pitcher_names_set)
    batters_for_matching = expected_names_from_lineups - pitcher_names_set # Subtract pitchers from the lineup names

    print(f"✅ Identified {len(pitchers_in_lineups)} pitchers in the lineup (based on {PITCHERS_FILE}).")
    if pitchers_in_lineups:
        Path(PITCHERS_IN_LINEUPS_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(PITCHERS_IN_LINEUPS_FILE).write_text("\n".join(sorted(pitchers_in_lineups)))
        print(f"📝 Saved identified pitchers to {PITCHERS_IN_LINEUPS_FILE}")

    # Filter batters_df using only the identified batters from the lineup
    filtered = batters_df[batters_df['normalized_name'].isin(batters_for_matching)].copy()
    unmatched_batters = sorted(batters_for_matching - set(filtered['normalized_name']))

    print(f"✅ Filtered down to {len(filtered)} batters (excluding identified pitchers)")
    if unmatched_batters:
        print(f"⚠️ {len(unmatched_batters)} unmatched batters found (after removing pitchers). Writing to {UNMATCHED_FILE}")
        Path(UNMATCHED_FILE).parent.mkdir(parents=True, exist_ok=True)
        Path(UNMATCHED_FILE).write_text("\n".join(unmatched_batters))
    else:
        print("✅ All intended lineup batters matched successfully.")

    filtered.drop(columns=['normalized_name'], errors='ignore', inplace=True)
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved filtered batters to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
