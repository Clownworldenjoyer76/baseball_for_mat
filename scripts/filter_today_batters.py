import pandas as pd
from pathlib import Path

LINEUPS_FILE = "data/raw/lineups.csv"
BATTERS_FILE = "data/cleaned/batters_normalized_cleaned.csv"
OUTPUT_FILE = "data/cleaned/batters_today.csv"

def main():
    print("📥 Loading lineups and batters...")
    try:
        lineups_df = pd.read_csv(LINEUPS_FILE)
        batters_df = pd.read_csv(BATTERS_FILE)
    except Exception as e:
        raise RuntimeError(f"Failed to load input files: {e}")

    if 'last_name, first_name' not in lineups_df.columns or 'name' not in batters_df.columns:
        raise ValueError("Missing required columns in either lineups or batters file.")

    lineups_df['last_name, first_name'] = lineups_df['last_name, first_name'].astype(str).str.strip()
    formatted_names = lineups_df['last_name, first_name'].apply(
        lambda name: ", ".join(name.split()[::-1]) if len(name.split()) == 2 else name
    )
    expected_names = formatted_names.unique()

    filtered = batters_df[batters_df['name'].astype(str).str.strip().isin(expected_names)]

    print(f"✅ Filtered down to {len(filtered)} batters")

    if filtered.empty:
        print("⚠️ No batters matched today's lineups. Check name formatting.")

    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
