import pandas as pd
import unicodedata
import re
from pathlib import Path

# File paths
HOME_FILE = Path("data/end_chain/pitchers_home_weather_park.csv")
AWAY_FILE = Path("data/end_chain/pitchers_away_weather_park.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

MERGE_COLS = ["innings_pitched", "strikeouts", "walks", "earned_runs"]

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

def load_and_prepare(file_path: Path, xtra_df: pd.DataFrame):
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return

    df = pd.read_csv(file_path)

    if "name" not in df.columns:
        print(f"❌ No 'name' column in {file_path.name}")
        return

    df["name"] = df["name"].astype(str).apply(normalize_name)
    df["name"] = df["name"].str.rstrip(", ").str.strip()

    xtra_names = set(xtra_df["name"])
    unmatched_names = df[~df["name"].isin(xtra_names)]["name"].unique()
    if len(unmatched_names) > 0:
        print(f"⚠️ {len(unmatched_names)} unmatched names in {file_path.name}:")
        for name in unmatched_names:
            print(" -", name)

    merged = df.merge(xtra_df[["name"] + MERGE_COLS], on="name", how="left")
    merged.to_csv(file_path, index=False)
    print(f"✅ Updated: {file_path.name}")

def main():
    if not XTRA_FILE.exists():
        print(f"❌ Missing file: {XTRA_FILE}")
        return

    xtra_df = pd.read_csv(XTRA_FILE)

    if "name" not in xtra_df.columns and "last_name, first_name" in xtra_df.columns:
        xtra_df["name"] = xtra_df["last_name, first_name"]

    if "name" not in xtra_df.columns:
        print("❌ 'name' column missing in pitchers_xtra_normalized.csv")
        return

    xtra_df["name"] = xtra_df["name"].astype(str).apply(normalize_name)
    xtra_df["name"] = xtra_df["name"].str.rstrip(", ").str.strip()

    load_and_prepare(HOME_FILE, xtra_df)
    load_and_prepare(AWAY_FILE, xtra_df)

if __name__ == "__main__":
    main()
