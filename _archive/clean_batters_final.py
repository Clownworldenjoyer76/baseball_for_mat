import pandas as pd
from pathlib import Path

CLEANED_DIR = Path("data/end_chain/cleaned")
FILES = {
    "batters_home": CLEANED_DIR / "batters_home_cleaned.csv",
    "batters_away": CLEANED_DIR / "batters_away_cleaned.csv"
}

DROP_COLUMNS = [
    "location", "stadium", "precipitation", "notes",  # bad weather columns
    "venue", "city", "state", "timezone", "lat", "lon", "is_dome",  # stadium metadata
    "away_team", "pitcher_home", "pitcher_away"  # non-batter fields
]

def clean_file(file_path: Path):
    df = pd.read_csv(file_path)

    # Drop only columns that exist in this file
    existing_to_drop = [col for col in DROP_COLUMNS if col in df.columns]
    df = df.drop(columns=existing_to_drop, errors="ignore")

    # Reorder alphabetically for consistency (optional)
    df = df[sorted(df.columns)]

    df.to_csv(file_path, index=False)
    print(f"✅ Cleaned: {file_path.name} ({df.shape[0]} rows, {df.shape[1]} cols)")

def main():
    for label, path in FILES.items():
        if path.exists():
            clean_file(path)
        else:
            print(f"⚠️ File not found: {path}")

if __name__ == "__main__":
    main()
