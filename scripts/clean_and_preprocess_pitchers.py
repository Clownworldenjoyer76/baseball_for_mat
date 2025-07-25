import pandas as pd
import unicodedata
import re
from pathlib import Path

# Paths
HOME_FILE = Path("data/end_chain/pitchers_home_weather_park.csv")
AWAY_FILE = Path("data/end_chain/pitchers_away_weather_park.csv")

# Normalization logic
def strip_accents(text):
    """Removes accent marks from characters in a string."""
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def capitalize_mc_names(text):
    """Capitalizes 'Mc' prefixes correctly (e.g., 'mcgregor' -> 'McGregor')."""
    def repl(match):
        return match.group(1).capitalize() + match.group(2).upper() + match.group(3).lower()
    return re.sub(r'\b(mc)([a-z])([a-z]*)\b', repl, text, flags=re.IGNORECASE)

def normalize_name(name):
    """
    Normalizes a pitcher's name to 'Last, First' format,
    handling various input formats and cleaning.
    """
    if not isinstance(name, str):
        return ""

    # Initial cleaning: replace various apostrophes, strip whitespace
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    
    # Remove characters that are not word characters, spaces, commas, or periods
    name = re.sub(r"[^\w\s,.]", "", name)
    
    # Replace multiple spaces with a single space
    name = re.sub(r"\s+", " ", name).strip()
    name = capitalize_mc_names(name)

    # Determine current format and convert to 'Last, First'
    if "," in name:
        # Already contains a comma, assume 'Last, First' or 'First, Last'
        parts = [p.strip().title() for p in name.split(",", 1)] # Split only on the first comma
        if len(parts) == 2:
            # Ensure it's Last, First. If input was 'First,Last', this reorders.
            # If input was 'Last,First', this maintains.
            # Check if the first part looks like a typical first name (e.g., shorter, common first name list)
            # For simplicity, we assume if a comma exists, the intent is 'Last, First'
            # Or, if it's 'First, Last', we reorder.
            # A more robust check might involve comparing parts or using a lookup list.
            # For now, if "First, Last" is the input, it will become "Last, First" after this.
            # If 'Smith, John', it remains 'Smith, John'
            # If 'John, Smith', it becomes 'Smith, John' (assuming typical First, Last input)
            # Let's explicitly aim for 'Last, First' if a comma exists.
            
            # If the input is "First, Last", split will give [First, Last]. We want [Last, First].
            # If the input is "Last, First", split will give [Last, First]. We want [Last, First].
            # The previous scripts implied "First Last" was converted to "Last, First".
            # The current ask is "names in First, Last format. I want them in Last, First format."
            # So, if name is "John, Smith", we want "Smith, John".
            
            # To achieve "Last, First" from "First, Last" (comma-separated):
            return f"{parts[1]}, {parts[0]}"
        return ' '.join(parts).title() # Fallback for malformed comma names
    else:
        # No comma, assume 'First Last' format
        tokens = [t.title() for t in name.split()]
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:]) # Handles multi-word last names
            return f"{last}, {first}"
        return name.title() # For single names

def normalize_name_column(df, column):
    """Applies name normalization to a DataFrame column."""
    df[column] = df[column].astype(str).apply(normalize_name)
    df[column] = df[column].str.rstrip(", ").str.strip() # Clean up any trailing comma/space
    return df

def process_file(file_path):
    """
    Processes a single CSV file to normalize pitcher names.
    Renames 'last_name, first_name' to 'name' if present.
    """
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    df = pd.read_csv(file_path)

    # Rename 'last_name, first_name' → 'name' for consistency
    if "last_name, first_name" in df.columns:
        df.rename(columns={"last_name, first_name": "name"}, inplace=True)
    # Ensure a 'name' column exists for processing, otherwise skip
    elif "name" not in df.columns:
        print(f"⚠️ Neither 'last_name, first_name' nor 'name' column found in {file_path.name}. Skipping name normalization.")
        return # Exit if no name column to process

    # Normalize name column
    df = normalize_name_column(df, "name")

    # Save cleaned file
    df.to_csv(file_path, index=False)
    print(f"✅ Cleaned and updated: {file_path.name}")

def main():
    """Main function to process home and away pitcher files."""
    print("Starting name normalization for pitcher files...")
    process_file(HOME_FILE)
    process_file(AWAY_FILE)
    print("Name normalization complete.")

if __name__ == "__main__":
    main()
