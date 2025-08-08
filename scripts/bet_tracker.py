import pandas as pd
from pathlib import Path

# Load input file
INPUT_FILE = Path("data/your_input_file.csv")  # Replace with your actual path
OUTPUT_FILE = Path("data/player_best_props.csv")

# Load CSV
df = pd.read_csv(INPUT_FILE)

# Clean column names
df.columns = df.columns.str.strip().str.lower()

# Ensure required columns exist
required_cols = ["player_name", "prop_type", "over_probability"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# Keep only the most probable prop per player
df_best = df.sort_values("over_probability", ascending=False).drop_duplicates(subset=["player_name"])

# Optional: sort by over_probability descending
df_best = df_best.sort_values("over_probability", ascending=False).reset_index(drop=True)

# Save
df_best.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Saved to: {OUTPUT_FILE}")
