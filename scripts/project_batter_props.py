
import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections
from utils import safe_col

# File paths
FINAL_FILE = Path("data/end_chain/final/batter_props_input.csv")
OUTPUT_FILE = Path("data/_projections/batter_props_projected.csv")

def main():
    print("🔄 Loading batter input file...")
    df = pd.read_csv(FINAL_FILE)

    print("🧮 Running projection formulas...")
    df = calculate_all_projections(df)

    print("💾 Saving to:", OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
