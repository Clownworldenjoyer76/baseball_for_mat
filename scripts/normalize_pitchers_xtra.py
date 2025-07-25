# scripts/normalize_pitchers_xtra.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

# Input and output paths
INPUT_FILE = Path("data/end_chain/pitchers_xtra.csv")
OUTPUT_DIR = Path("data/end_chain/cleaned")
OUTPUT_FILE = OUTPUT_DIR / "pitchers_xtra_normalized.csv"

# Create output dir
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
    
    if "," in name:
        parts = [p.strip().title() for p in name.split(",")]
        if len(parts) >= 2:
            return f"{parts[0]}, {parts[1]}"
        return ' '.join(parts).title()
    else:
        tokens = [t.title() for t in name.split()]
        if len(tokens) >= 2:
            first = tokens[0]
            last = " ".join(tokens[1:])
            return f"{last}, {first}"
        return ' '.join(tokens).title()

def main():
    if not INPUT_FILE.exists():
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)

    old_name_col = "last_name, first_name"
    if old_name_col not in df.columns:
        print(f"❌ '{old_name_col}' column not found in input.")
        return

    print(f"🔄 Normalizing names in {INPUT_FILE.name}...")

    df[old_name_col] = df[old_name_col].astype(str).apply(normalize_name)

    # Rename columns
    df = df.rename(columns={
        old_name_col: "name",
        "p_formatted_ip": "innings_pitched",
        "strikeout": "strikeouts",
        "walk": "walks",
        "p_earned_run": "earned_runs"
    })

    # Log missing names
    missing = df[df["name"].str.strip() == ""]
    if not missing.empty:
        print("\n⚠️ Missing or empty normalized names:")
        print(missing)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Normalized and renamed columns written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
