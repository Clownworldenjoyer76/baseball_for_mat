# scripts/normalize_pitchers_xtra.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

# Input and output paths
INPUT_FILE = Path("data/end_chain/pitchers_xtra.csv")
NORMALIZED_REF = Path("data/cleaned/pitchers_normalized_cleaned.csv")
OUTPUT_DIR = Path("data/end_chain/cleaned")
OUTPUT_FILE = OUTPUT_DIR / "pitchers_xtra_normalized.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def strip_accents(text):
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def capitalize_mc_names(text):
    return re.sub(r'\b(mc)([a-z])([a-z]*)\b',
                  lambda m: m.group(1).capitalize() + m.group(2).upper() + m.group(3).lower(),
                  text, flags=re.IGNORECASE)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip().rstrip(',')
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = capitalize_mc_names(name)

    suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
    tokens = name.split()

    if len(tokens) >= 2:
        first = tokens[0]
        possible_suffix = tokens[-1].lower().strip(".")
        if possible_suffix in suffixes and len(tokens) > 2:
            last = " ".join(tokens[1:-1])
            suffix = tokens[-1]
            return f"{last} {suffix}, {first}"
        else:
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
    return name.title()

def main():
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        return
    if not NORMALIZED_REF.exists():
        print(f"❌ Reference file not found: {NORMALIZED_REF}")
        return

    df = pd.read_csv(INPUT_FILE)
    ref = pd.read_csv(NORMALIZED_REF)

    old_name_col = "last_name, first_name"
    if old_name_col not in df.columns:
        print(f"❌ '{old_name_col}' column not found in input.")
        return

    print(f"🔄 Normalizing names in {INPUT_FILE.name}...")

    # Normalize names
    df[old_name_col] = df[old_name_col].astype(str).apply(normalize_name)

    # Rename and clean
    df = df.rename(columns={
        old_name_col: "name",
        "p_formatted_ip": "innings_pitched",
        "strikeout": "strikeouts",
        "walk": "walks",
        "p_earned_run": "earned_runs"
    })

    # Remove trailing commas in all string columns
    df = df.applymap(lambda x: x.rstrip(',') if isinstance(x, str) else x)

    # Match team from ref by name
    ref = ref[['last_name, first_name', 'team']].rename(columns={'last_name, first_name': 'name'})
    df = df.merge(ref, on='name', how='left')

    if 'team' not in df.columns:
        print("❌ Failed to merge 'team' column.")
    else:
        print(f"✅ Merged 'team' for {df['team'].notna().sum()} rows.")

    # Log missing names
    if df['name'].str.strip().eq("").any():
        print("⚠️ Warning: Some names could not be normalized.")

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
