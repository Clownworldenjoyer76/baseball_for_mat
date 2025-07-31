import pandas as pd
from pathlib import Path
from projection_formulas import project_final_score

# File paths
BATTER_PROPS_FILE = Path("data/_projections/batter_props_projected.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    print("\n--- DEBUG START ---")
    print(f"DEBUG: Script started at {pd.Timestamp.now()}")
    print(f"DEBUG: Input file path: {BATTER_PROPS_FILE}")
    print(f"DEBUG: Output file path: {OUTPUT_FILE}")

    print("\n🔄 Loading projected batter data...")
    try:
        df = pd.read_csv(BATTER_PROPS_FILE)
        print(f"DEBUG: Successfully loaded '{BATTER_PROPS_FILE}'.")
        print(f"DEBUG: DataFrame head after loading:\n{df.head()}")
        print(f"DEBUG: DataFrame shape after loading: {df.shape}")
        if df.empty:
            print("DEBUG: WARNING: Loaded DataFrame is empty!")
    except FileNotFoundError:
        print(f"❌ Error: File not found: {BATTER_PROPS_FILE}")
        return
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return

    # Detect valid team column
    team_col = None
    for col in ["home_team", "away_team", "team"]:
        if col in df.columns:
            team_col = col
            break

    if team_col is None:
        print("❌ Error: No team column found in batter props file.")
        return

    df[team_col] = df[team_col].str.upper().str.strip()

    print("📊 Running final score projections...")
    result = project_final_score(df)

    print(f"💾 Saving results to {OUTPUT_FILE}")
    result.to_csv(OUTPUT_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()