# scripts/normalize_names.py

import pandas as pd
import unicodedata
import re
from pathlib import Path

# --- File Paths ---
BATT_IN = Path("data/Data/batters.csv")
PITCH_IN = Path("data/Data/pitchers.csv")
BATT_OUT = Path("data/normalized/batters_normalized.csv")
PITCH_OUT = Path("data/normalized/pitchers_normalized.csv")
SUMMARY_FILE = Path("summaries/summary.txt")
TARGET_COLUMN = "last_name, first_name"

# --- Regex Patterns ---
SUFFIXES_PATTERN = r"\b(jr|sr|ii|iii|iv|v)\b\.?"
RE_NON_ALPHANUM_OR_SPACE_OR_COMMA = re.compile(r"[^\w\s,]")
RE_MULTI_SPACE = re.compile(r"\s+")
RE_SUFFIX_REMOVE = re.compile(SUFFIXES_PATTERN, re.IGNORECASE)

def strip_accents(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def normalize_series(name_series: pd.Series) -> pd.Series:
    name_series = name_series.astype(str).fillna("")
    name_series = name_series.apply(strip_accents)
    name_series = name_series.str.replace("’", "", regex=False)
    name_series = name_series.str.replace("`", "", regex=False)
    name_series = name_series.str.replace(RE_NON_ALPHANUM_OR_SPACE_OR_COMMA, "", regex=True)
    name_series = name_series.str.replace(RE_MULTI_SPACE, " ", regex=True)
    name_series = name_series.str.strip()
    return name_series

def process(label: str, input_path: Path, output_path: Path) -> tuple[int, int]:
    if not input_path.exists():
        print(f"❌ Missing: {input_path}")
        return 0, 0
    df = pd.read_csv(input_path)
    if TARGET_COLUMN not in df.columns:
        print(f"⚠️ Missing column '{TARGET_COLUMN}' in {input_path}")
        return 0, 0
    original_count = len(df)
    df[TARGET_COLUMN] = normalize_series(df[TARGET_COLUMN])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ {label} normalized: {original_count} → {output_path}")
    return original_count, len(df)

def main():
    batt_src, batt_out = process("Batters", BATT_IN, BATT_OUT)
    pitch_src, pitch_out = process("Pitchers", PITCH_IN, PITCH_OUT)

    with open(SUMMARY_FILE, "a") as f:
        f.write("🔡 Normalize Names Summary\n")
        f.write(f"Batters in source:    {batt_src}\n")
        f.write(f"Batters normalized:   {batt_out}\n")
        f.write(f"Pitchers in source:   {pitch_src}\n")
        f.write(f"Pitchers normalized:  {pitch_out}\n\n")

if __name__ == "__main__":
    main()
