import pandas as pd
import unicodedata
import re
from pathlib import Path

BATT_FILE = Path("data/normalized/batters_normalized.csv")
PITCH_FILE = Path("data/normalized/pitchers_normalized.csv")

# Common suffixes (lowercased and stripped of punctuation)
SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    tokens = name.replace(",", "").split()
    if len(tokens) >= 2:
        if tokens[-1].lower().strip(".") in SUFFIXES and len(tokens) >= 3:
            last = f"{tokens[-2]} {tokens[-1]}"
            first = " ".join(tokens[:-2])
        else:
            last = tokens[-1]
            first = " ".join(tokens[:-1])
        return f"{last.strip().title()}, {first.strip().title()}"
    return name.title()

# Normalize files
for file in [BATT_FILE, PITCH_FILE]:
    df = pd.read_csv(file)
    if "last_name, first_name" in df.columns:
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
        df.to_csv(file, index=False)
