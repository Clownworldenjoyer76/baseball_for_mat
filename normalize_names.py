import os
import pandas as pd
import unicodedata

# Define file paths
batters_path = "data/Data/batters.csv"
pitchers_path = "data/Data/pitchers.csv"
output_batters_path = "data/Data/batters_normalized.csv"
output_pitchers_path = "data/Data/pitchers_normalized.csv"

# Create output directory if it doesn't exist
os.makedirs("data/Data", exist_ok=True)

# Function to normalize and format names
def strip_accents(text):
    normalized = unicodedata.normalize("NFKD", text)
    return ''.join([c for c in normalized if not unicodedata.combining(c)])

def clean_and_format_name(name):
    if not isinstance(name, str):
        return ""
    name = strip_accents(name)
    name = name.replace('.', '').replace("'", '').strip().lower()
    parts = [p.capitalize() for p in name.split(',')]
    return ', '.join(p.strip() for p in parts)

# Process batters
batters_df = pd.read_csv(batters_path)
if "last_name, first_name" in batters_df.columns:
    batters_df["last_name, first_name"] = batters_df["last_name, first_name"].apply(clean_and_format_name)
batters_df.to_csv(output_batters_path, index=False)

# Process pitchers
pitchers_df = pd.read_csv(pitchers_path)
if "last_name, first_name" in pitchers_df.columns:
    pitchers_df["last_name, first_name"] = pitchers_df["last_name, first_name"].apply(clean_and_format_name)
pitchers_df.to_csv(output_pitchers_path, index=False)