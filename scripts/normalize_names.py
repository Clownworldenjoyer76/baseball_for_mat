# scripts/normalize_names.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

# ─── File Paths ───────────────────────────────────────────

BATT_FILE = Path("data/normalized/batters_normalized.csv")
PITCH_FILE = Path("data/normalized/pitchers_normalized.csv")

# ─── Text Normalization Utilities ─────────────────────────

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)  # allow letters, digits, spaces, commas, periods
    name = re.sub(r"\s+", " ", name).strip()
    
    # Handle names with a comma
    if "," in name:
        parts = [p.strip() for p in name.split(",")]
        if len(parts) == 2:
            last, first = parts
        else:
            return name.title()  # fallback
    else:
        tokens = name.split()
        if len(tokens) < 2:
            return name.title()
        last = tokens[-1]
        first = " ".join(tokens[:-1])
    
    # Detect and shift suffix (Jr., Sr., II, etc.) from last to first name
    suffixes = {"Jr", "Jr.", "Sr", "Sr.", "II", "III", "IV"}
    last_tokens = last.split()
    
    if last_tokens[-1] in suffixes and len(last_tokens) >= 2:
        suffix = last_tokens[-1]
        core_last = " ".join(last_tokens[:-1])
        first = f"{first} {suffix}"
        last = core_last

    # Clean initials
    first = re.sub(r"\b([A-Z])\.", r"\1", first, flags=re.IGNORECASE)
    
    return f"{last.title()}, {first.title()}"

# ─── Normalize Both Files ─────────────────────────────────

for file in [BATT_FILE, PITCH_FILE]:
    df = pd.read_csv(file)
    if "last_name, first_name" in df.columns:
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
        df.to_csv(file, index=False)
