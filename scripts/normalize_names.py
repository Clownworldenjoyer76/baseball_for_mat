# scripts/normalize_names.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

BATT_FILE = Path("data/normalized/batters_normalized.csv")
PITCH_FILE = Path("data/normalized/pitchers_normalized.csv")

# Common suffixes to strip from last names
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""

    # Normalize and clean
    name = strip_accents(name)
    name = name.replace("’", "").replace("`", "")
    name = re.sub(r"[^\w\s,]", "", name)
    name = re.sub(r"\s+", " ", name).strip().lower()

    # Split and clean
    if "," in name:
        last, first = [part.strip().title() for part in name.split(",", 1)]
    else:
        parts = name.split()
        if len(parts) >= 2:
            last, first = parts[0].title(), " ".join(parts[1:]).title()
        else:
            return name.title()
    
    # Remove suffixes from last name
    last_parts = last.split()
    if last_parts[-1].lower().strip(".") in SUFFIXES:
        last = " ".join(last_parts[:-1])

    return f"{last}, {first}"

# Apply to both batter and pitcher files
for file in [BATT_FILE, PITCH_FILE]:
    df = pd.read_csv(file)
    if "last_name, first_name" in df.columns:
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
        df.to_csv(file, index=False)
