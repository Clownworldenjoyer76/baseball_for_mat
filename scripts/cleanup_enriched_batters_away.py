import pandas as pd
from pathlib import Path

# Paths
INPUT_FILE = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
OUTPUT_FILE = INPUT_FILE  # Overwrite same file

# Load data
df = pd.read_csv(INPUT_FILE)

# Step 1: Drop all *_y columns
y_cols = [col for col in df.columns if col.endswith('_y')]
df.drop(columns=y_cols, inplace=True)

# Step 2: Rename *_x columns to original names
rename_map = {col: col[:-2] for col in df.columns if col.endswith('_x')}
df.rename(columns=rename_map, inplace=True)

# Step 3: Drop duplicate columns, if any (safe fallback)
df = df.loc[:, ~df.columns.duplicated()]

# Save cleaned file
df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Cleaned and saved: {OUTPUT_FILE}")
