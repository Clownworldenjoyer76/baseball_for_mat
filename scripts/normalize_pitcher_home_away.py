import pandas as pd
from unidecode import unidecode
import os

PITCHER_REF_FILE = "data/Data/pitchers.csv"

FILES = {
    "data/adjusted/pitchers_home.csv": "pitcher_home",
    "data/adjusted/pitchers_away.csv": "pitcher_away",
}

def normalize_name(name):
    if pd.isna(name): return name
    name = unidecode(name).lower().strip()
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return name.title()

def main():
    try:
        ref_pitchers = pd.read_csv(PITCHER_REF_FILE)
        ref_pitchers["normalized_name"] = ref_pitchers["last_name, first_name"].apply(normalize_name)
        ref_set = set(ref_pitchers["normalized_name"])

        for path, pitcher_col in FILES.items():
            if not os.path.exists(path):
                print(f"❌ Missing file: {path}")
                continue

            df = pd.read_csv(path)

            # Drop old 'name' column if exists
            if "name" in df.columns:
                df = df.drop(columns=["name"])

            # Normalize 'last_name, first_name' if present
            if "last_name, first_name" in df.columns:
                df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
                df = df[df["last_name, first_name"].isin(ref_set)]

            # Normalize pitcher_* if present
            if pitcher_col in df.columns:
                df[pitcher_col] = df[pitcher_col].apply(normalize_name)
                df = df[df[pitcher_col].isin(ref_set)]

            df.to_csv(path, index=False)
            print(f"✅ Normalized and saved: {path}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
