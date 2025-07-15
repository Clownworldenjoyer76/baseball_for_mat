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

    print("📊 Lineups columns:", lineups_df.columns.tolist())
    print("📊 Batters columns:", batters_df.columns.tolist())

    if 'last_name, first_name' not in lineups_df.columns or 'last_name, first_name' not in batters_df.columns:
        raise ValueError("Missing required 'last_name, first_name' column in lineups or batters.")

    print("🔍 Sample values from lineups:")
    print(lineups_df['last_name, first_name'].dropna().head(5))

    print("🔍 Sample values from batters:")
    print(batters_df['last_name, first_name'].dropna().head(5))

    print("🔍 Stripping and matching names...")
    expected_names = lineups_df['last_name, first_name'].astype(str).str.strip().unique()
    batters_df['last_name, first_name'] = batters_df['last_name, first_name'].astype(str).str.strip()

    print(f"🔢 {len(expected_names)} unique names in today's lineups")
    print(f"🔢 {len(batters_df)} total batters in cleaned file")

    filtered = batters_df[batters_df['last_name, first_name'].isin(expected_names)]
    print(f"✅ Filtered down to {len(filtered)} batters")

    if filtered.empty:
        print("⚠️ No batters matched today's lineups. Check name formatting.")

    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
