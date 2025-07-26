# scripts/normalize_pitcher_and_batter_names.py

import pandas as pd
from pathlib import Path
from unidecode import unidecode

# Input and Output Paths
BATTERS_IN = Path("data/Data/batters.csv")
PITCHERS_IN = Path("data/Data/pitchers.csv")
BATTERS_OUT = Path("data/normalized/batters_normalized.csv")
PITCHERS_OUT = Path("data/normalized/pitchers_normalized.csv")

def normalize_name(name):
    if pd.isna(name):
        return name
    name = unidecode(str(name)).strip()
    name = ' '.join(name.split())  # Collapse multiple spaces
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1].title()}, {' '.join(parts[:-1]).title()}"
    return name.title()

def normalize_file(input_path: Path, output_path: Path):
    df = pd.read_csv(input_path)
    if "last_name, first_name" in df.columns:
        df["last_name, first_name"] = df["last_name, first_name"].apply(normalize_name)
    else:
        print(f"⚠️ 'last_name, first_name' column not found in {input_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Normalized file saved: {output_path}")

def main():
    normalize_file(BATTERS_IN, BATTERS_OUT)
    normalize_file(PITCHERS_IN, PITCHERS_OUT)

if __name__ == "__main__":
    main()
