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

def normalize_to_last_first(name_series: pd.Series) -> pd.Series:
    print("\n🔍 Preview of original names:")
    print(name_series.head(10).to_string(index=False))

    name_series = name_series.astype(str).fillna("")
    name_series = name_series.apply(strip_accents)
    name_series = name_series.str.replace("’", "", regex=False)
    name_series = name_series.str.replace("`", "", regex=False)
    name_series = name_series.str.replace(RE_NON_ALPHANUM_OR_SPACE_OR_COMMA, "", regex=True)
    name_series = name_series.str.replace(RE_MULTI_SPACE, " ", regex=True)
    name_series = name_series.str.strip()

    temp_df = pd.DataFrame(index=name_series.index)
    temp_df["raw"] = name_series

    has_comma = temp_df["raw"].str.contains(",", na=False)
    with_comma = temp_df.loc[has_comma, "raw"].str.split(",", n=1, expand=True)
    if not with_comma.empty:
        temp_df.loc[has_comma, "first"] = with_comma[0].str.strip()
        temp_df.loc[has_comma, "last"] = with_comma[1].str.strip()

    without_comma = temp_df.loc[~has_comma, "raw"].str.split(" ", n=1, expand=True)
    temp_df.loc[~has_comma, "first"] = without_comma[0].str.strip()
    temp_df.loc[~has_comma, "last"] = without_comma[1].fillna("").str.strip()

    temp_df["first"] = temp_df["first"].str.title()
    temp_df["last"] = temp_df["last"].str.title()
    temp_df["last"] = temp_df["last"].apply(lambda x: RE_SUFFIX_REMOVE.sub("", x).strip())

    normalized_series = temp_df.apply(
        lambda row: f"{row['last']}, {row['first']}" if row['first'] else row['last'],
        axis=1
    )

    print("✅ Preview of normalized names:")
    print(normalized_series.head(10).to_string(index=False))
    print(f"✅ Total names normalized: {len(normalized_series)}")

    return normalized_series

def process_file(label: str, input_path: Path, output_path: Path):
    print(f"\n🔄 Processing {label} from {input_path} → {output_path}")
    if not input_path.exists():
        print(f"❌ Missing input file: {input_path}")
        return

    try:
        df = pd.read_csv(input_path)
        if TARGET_COLUMN in df.columns:
            df[TARGET_COLUMN] = normalize_to_last_first(df[TARGET_COLUMN])
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
