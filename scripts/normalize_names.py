import pandas as pd
import os
import unicodedata
import re

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in text if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""

    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)  # Keep alphanumerics, comma, period
    name = re.sub(r"\s+", " ", name).strip()

    # Handle names in "First Middle Last" or "First Last Suffix" formats
    if "," not in name:
        tokens = name.split()
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return name.title()

    # Handle names already in "Last, First Suffix" format
    parts = name.split(",")
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = parts[1].strip().title()
        return f"{last}, {first}"

    return name.title()

def process_file(input_file, output_file):
    print(f"📥 Processing {input_file}...")
    df = pd.read_csv(input_file)
    if "last_name, first_name" not in df.columns:
        raise ValueError(f"'last_name, first_name' column not found in {input_file}")
    df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
    df.to_csv(output_file, index=False)
    print(f"✅ Saved normalized file to {output_file}")

def main():
    os.makedirs("data/normalized", exist_ok=True)
    try:
        process_file("data/Data/batters.csv", "data/normalized/batters_normalized.csv")
        process_file("data/Data/pitchers.csv", "data/normalized/pitchers_normalized.csv")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
