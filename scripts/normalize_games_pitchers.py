# scripts/normalize_games_pitchers.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

# Input/output paths
FILE_PATH = Path("data/end_chain/cleaned/games_cleaned.csv")

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
    if not FILE_PATH.exists():
        print(f"❌ File not found: {FILE_PATH}")
        return

    df = pd.read_csv(FILE_PATH)

    for col in ["pitcher_home", "pitcher_away"]:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(normalize_name)
        else:
            print(f"⚠️ Column not found: {col}")

    df.to_csv(FILE_PATH, index=False)
    print(f"✅ Normalized pitcher names written to {FILE_PATH}")

if __name__ == "__main__":
    main()
