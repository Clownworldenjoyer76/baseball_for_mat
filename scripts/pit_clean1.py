# scripts/pit_clean1.py

import pandas as pd
from pathlib import Path

# Input file paths
HOME_PATH = Path("data/end_chain/pitchers_home_weather_park.csv")
AWAY_PATH = Path("data/end_chain/pitchers_away_weather_park.csv")

def flip_name_format(name):
    try:
        parts = [part.strip() for part in name.split(",")]
        if len(parts) == 2:
            return f"{parts[1]}, {parts[0]}"
        return name
    except Exception:
        return name

def process_file(path: Path):
    if not path.exists():
        print(f"❌ File not found: {path}")
        return
    df = pd.read_csv(path)
    if "name" not in df.columns:
        print(f"❌ No 'name' column in: {path}")
        return
    df["name"] = df["name"].astype(str).apply(flip_name_format)
    df.to_csv(path, index=False)
    print(f"✅ Updated names in {path.name}")

def main():
    process_file(HOME_PATH)
    process_file(AWAY_PATH)

if __name__ == "__main__":
    main()
