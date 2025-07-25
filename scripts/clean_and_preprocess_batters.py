import pandas as pd
from pathlib import Path
import sys

# Input paths
INPUT_DIR = Path("data/end_chain")
OUTPUT_DIR = INPUT_DIR / "cleaned"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FILES = {
    "batters_home": INPUT_DIR / "batters_home_weather_park.csv",
    "batters_away": INPUT_DIR / "batters_away_weather_park.csv"
}

REQUIRED_COLUMNS = ["name", "team", "ab", "pa", "hit", "home_run", "walk", "strikeout"]

def clean_batter_data(df: pd.DataFrame, label: str) -> pd.DataFrame:
    # Drop columns ending in _y
    drop_cols = [col for col in df.columns if col.endswith("_y")]
    df = df.drop(columns=drop_cols, errors='ignore')

    # Rename columns ending in _x
    rename_cols = {col: col[:-2] for col in df.columns if col.endswith("_x")}
    df = df.rename(columns=rename_cols)

    # Drop unnamed empty columns
    unnamed_blank_cols = [
        col for col in df.columns
        if col.startswith("Unnamed") and df[col].isna().all()
    ]
    df = df.drop(columns=unnamed_blank_cols)

    # Ensure required columns exist
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {label} data: {missing_cols}")

    # Convert numeric stats
    stat_cols = [col for col in REQUIRED_COLUMNS if col not in ['name', 'team']]
    for col in stat_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df[stat_cols] = df[stat_cols].fillna(0)

    # Normalize team name
    df["team"] = df["team"].astype(str).str.strip().str.title()

    # Add batting_avg if missing
    if "batting_avg" not in df.columns:
        df["batting_avg"] = df["hit"] / df["ab"].replace({0: pd.NA})
        df["batting_avg"] = df["batting_avg"].fillna(0).round(3)

    return df

def main():
    try:
        for label, file_path in FILES.items():
            if not file_path.exists():
                continue

            df = pd.read_csv(file_path)
            cleaned_df = clean_batter_data(df, label)
            output_file = OUTPUT_DIR / f"{label}_cleaned.csv"
            cleaned_df.to_csv(output_file, index=False)

    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
