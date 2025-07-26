import pandas as pd
import unicodedata
import re
from pathlib import Path

BATT_FILE = Path("data/normalized/batters_normalized.csv")
PITCH_FILE = Path("data/normalized/pitchers_normalized.csv")

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,]", "", name)  # preserve comma
    name = re.sub(r"\s+", " ", name).strip()
    return name.title()

for file in [BATT_FILE, PITCH_FILE]:
    df = pd.read_csv(file)
    if "last_name, first_name" in df.columns:
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
        df.to_csv(file, index=False)
