import pandas as pd
from pathlib import Path

# File paths
HOME_FILE = Path("data/end_chain/pitchers_home_weather_park.csv")
AWAY_FILE = Path("data/end_chain/pitchers_away_weather_park.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

MERGE_COLS = ["innings_pitched", "strikeouts", "walks", "earned_runs"]
RENAME_MAP = {"last_name, first_name": "name"}

def rename_name_column(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    if "last_name, first_name" in df.columns:
        df = df.rename(columns=RENAME_MAP)
        print(f"✅ Renamed column in {filename}")
    else:
        print(f"⚠️ 'last_name, first_name' not found in {filename}")
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
    df = rename_name_column(df, path.name)
    df = merge_extra_data(df, xtra_df, path.name)
    df.to_csv(path, index=False)
    print(f"💾 Updated: {path.name}")

def main():
    if not XTRA_FILE.exists():
        print(f"❌ Missing xtra file: {XTRA_FILE}")
        return

    xtra_df = pd.read_csv(XTRA_FILE)

    process_file(HOME_FILE, xtra_df)
    process_file(AWAY_FILE, xtra_df)

if __name__ == "__main__":
    main()
