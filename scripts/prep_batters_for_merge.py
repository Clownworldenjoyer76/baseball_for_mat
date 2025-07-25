import pandas as pd
from pathlib import Path

# Paths
INPUT_DIR = Path("data/end_chain/cleaned")
FILES = {
    "batters_home": INPUT_DIR / "batters_home_cleaned.csv",
    "batters_away": INPUT_DIR / "batters_away_cleaned.csv"
}

# Columns to drop (non-batter data or merge-conflicting fields)
DROP_COLUMNS = [
    "location", "stadium", "precipitation", "notes",  # weather
    "venue", "city", "state", "timezone", "lat", "lon", "is_dome",  # stadium
    "away_team", "pitcher_home", "pitcher_away"  # game-level
]

# Required column
REQUIRED = "team"

def prep_file(file_path: Path):
    df = pd.read_csv(file_path)

    if REQUIRED not in df.columns:
        raise ValueError(f"Missing required column: {REQUIRED} in {file_path.name}")

    # Drop only if they exist
    to_drop = [col for col in DROP_COLUMNS if col in df.columns]
    df = df.drop(columns=to_drop)

    # Drop duplicate team rows (keep first)
    df = df.drop_duplicates(subset=[REQUIRED])

    # Strip and title-case team values
    df[REQUIRED] = df[REQUIRED].astype(str).str.strip().str.title()

    df.to_csv(file_path, index=False)
    print(f"✅ Prepped: {file_path.name} ({df.shape[0]} rows, {df.shape[1]} cols)")

def main():
    for label, path in FILES.items():
        if path.exists():
            prep_file(path)
        else:
            print(f"⚠️ File not found: {path}")

if __name__ == "__main__":
    main()
