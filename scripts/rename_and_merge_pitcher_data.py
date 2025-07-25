import pandas as pd
from pathlib import Path
import unicodedata
import re

# File paths
HOME_FILE = Path("data/end_chain/pitchers_home_weather_park.csv")
AWAY_FILE = Path("data/end_chain/pitchers_away_weather_park.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

MERGE_COLS = ["innings_pitched", "strikeouts", "walks", "earned_runs"]
RENAME_MAP = {"last_name, first_name": "name"}

def strip_accents(text):
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def capitalize_mc_names(text):
    return re.sub(r'\b(mc)([a-z])([a-z]*)\b',
                  lambda m: m.group(1).capitalize() + m.group(2).upper() + m.group(3).lower(),
                  text, flags=re.IGNORECASE)

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.replace("’", "'").replace("`", "'").strip()
    name = strip_accents(name)
    name = re.sub(r"[^\w\s,\.]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = capitalize_mc_names(name)

    tokens = name.split()
    if len(tokens) >= 2:
        return f"{tokens[1]}, {tokens[0]}"
    return name

def normalize_and_format_names(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    if "last_name, first_name" in df.columns:
        df = df.rename(columns=RENAME_MAP)

    if "name" in df.columns:
        df["name"] = df["name"].astype(str).apply(normalize_name)
        df["name"] = df["name"].str.rstrip(", ").str.strip()
        print(f"🔄 Normalized names to 'Last, First' in {filename}")
    else:
        print(f"⚠️ 'name' column not found in {filename}")
    return df

def merge_extra_data(df: pd.DataFrame, xtra_df: pd.DataFrame, filename: str) -> pd.DataFrame:
    df = df.merge(xtra_df[["name"] + MERGE_COLS], on="name", how="left")
    print(f"✅ Merged extra pitcher stats into {filename}")
    return df

def process_file(path: Path, xtra_df: pd.DataFrame):
    if not path.exists():
        print(f"❌ File not found: {path}")
        return

    df = pd.read_csv(path)
    df = normalize_and_format_names(df, path.name)
    df = merge_extra_data(df, xtra_df, path.name)
    df["name"] = df["name"].str.rstrip(", ").str.strip()  # Final cleanup
    df.to_csv(path, index=False)
    print(f"💾 Updated: {path.name}")

def main():
    if not XTRA_FILE.exists():
        print(f"❌ Missing xtra file: {XTRA_FILE}")
        return

    xtra_df = pd.read_csv(XTRA_FILE)
    xtra_df["name"] = xtra_df["name"].astype(str).apply(normalize_name)
    xtra_df["name"] = xtra_df["name"].str.rstrip(", ").str.strip()

    process_file(HOME_FILE, xtra_df)
    process_file(AWAY_FILE, xtra_df)

if __name__ == "__main__":
    main()
