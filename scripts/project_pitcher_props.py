import pandas as pd
from pathlib import Path

# ✅ Ensure we load the correct input file
INPUT = Path("data/end_chain/pitchers_xtra.csv")
OUTPUT = Path("data/_projections/pitcher_props_projected.csv")

def main():
    df = pd.read_csv(INPUT)

    # ✅ Clean column names for safety
    df.columns = df.columns.str.strip().str.lower()

    # ✅ Check required columns exist
    if "p_earned_run" not in df.columns or "innings_pitched" not in df.columns:
        raise ValueError("Required columns 'p_earned_run' and/or 'innings_pitched' not found in input CSV.")

    # ✅ Calculate ERA
    df["era"] = (df["p_earned_run"] / df["innings_pitched"]) * 9

    # ✅ Output to projections directory
    df.to_csv(OUTPUT, index=False)

if __name__ == "__main__":
    main()
