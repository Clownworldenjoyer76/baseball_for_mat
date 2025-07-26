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

def clean_name(name: str) -> str:
    name = strip_accents(name)
    name = name.replace("’", "").replace("`", "")
    name = RE_NON_ALPHANUM_OR_SPACE_OR_COMMA.sub("", name)
    name = RE_MULTI_SPACE.sub(" ", name).strip()
    name = name.rstrip(",")  # Remove trailing commas

    # Optionally remove suffixes from last name portion
    if "," in name:
        parts = name.split(",", 1)
        last = RE_SUFFIX_REMOVE.sub("", parts[0]).strip().title()
        first = parts[1].strip().title()
        return f"{last}, {first}"
    else:
        return name.title()

def normalize_series(series: pd.Series) -> pd.Series:
    print("\n🔍 Preview of original names:")
    print(series.head(10).to_string(index=False))
    normalized = series.astype(str).fillna("").apply(clean_name)
    print("\n✅ Preview of normalized names:")
    print(normalized.head(10).to_string(index=False))
    print(f"\n✅ Total names normalized: {len(normalized)}")
    return normalized

def process_file(label: str, input_path: Path, output_path: Path):
    print(f"\n🔄 Processing {label} from {input_path} → {output_path}")
    if not input_path.exists():
        print(f"❌ Missing input file: {input_path}")
        return

    try:
        df = pd.read_csv(input_path)
        if TARGET_COLUMN in df.columns:
            df[TARGET_COLUMN] = normalize_series(df[TARGET_COLUMN])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            print(f"✅ Saved normalized {label} to: {output_path}")
        else:
            print(f"⚠️ Column '{TARGET_COLUMN}' not found in {input_path}")
    except Exception as e:
        print(f"❌ Error processing {label}: {e}")

if __name__ == "__main__":
    process_file("batters", BATT_IN, BATT_OUT)
    process_file("pitchers", PITCH_IN, PITCH_OUT)
