import pandas as pd
import unicodedata
import re
from pathlib import Path

# Paths
HOME_FILE = Path("data/end_chain/pitchers_home_weather_park.csv")
AWAY_FILE = Path("data/end_chain/pitchers_away_weather_park.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

MERGE_COLS = ["innings_pitched", "strikeouts", "walks", "earned_runs"]

# Normalization logic
def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def capitalize_mc_names(text):
    def repl(match):
        return match.group(1).capitalize() + match.group(2).upper() + match.group(3).lower()
    return re.sub(r'\b(mc)([a-z])([a-z]*)\b', repl, text, flags=re.IGNORECASE)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = capitalize_mc_names(name)
    tokens = name.split()
    if len(tokens) >= 2:
        return f"{tokens[1]}, {tokens[0]}"
    return name.title()

def normalize_name_column(df, column):
    df[column] = df[column].astype(str).apply(normalize_name)
    df[column] = df[column].str.rstrip(", ").str.strip()
    return df

def process_file(file_path, xtra_df):
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    df = pd.read_csv(file_path)
    
    # Rename 'last_name, first_name' → 'name'
    if "last_name, first_name" in df.columns:
        df.rename(columns={"last_name, first_name": "name"}, inplace=True)

    # Normalize name column
    df = normalize_name_column(df, "name")
    xtra_df = normalize_name_column(xtra_df.copy(), "name")

    # Merge
    merged = df.merge(xtra_df[["name"] + MERGE_COLS], on="name", how="left")

    # Report unmatched
    unmatched = merged[merged[MERGE_COLS[0]].isnull()]
    if not unmatched.empty:
        print(f"⚠️ {len(unmatched)} unmatched pitchers in {file_path.name}:")
        print(unmatched["name"].unique())

    # Save updated file
    merged.to_csv(file_path, index=False)
    print(f"✅ Updated: {file_path.name}")

def main():
    if not XTRA_FILE.exists():
        print(f"❌ Missing input: {XTRA_FILE}")
        return

    xtra_df = pd.read_csv(XTRA_FILE)

    # Ensure required columns
    required_cols = ["name"] + MERGE_COLS
    if not all(col in xtra_df.columns for col in required_cols):
        print("❌ Missing required columns in pitchers_xtra_normalized.csv")
        return

    process_file(HOME_FILE, xtra_df)
    process_file(AWAY_FILE, xtra_df)

if __name__ == "__main__":
    main()
