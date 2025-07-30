import pandas as pd
from pathlib import Path

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

def main():
    if not XTRA_FILE.exists():
        raise FileNotFoundError(f"❌ Missing input file: {XTRA_FILE}")

    # Load files
    df_final = pd.read_csv(FINAL_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    # Rename and normalize column in XTRA
    if "last_name_first_name" in df_xtra.columns:
        df_xtra = df_xtra.rename(columns={"last_name_first_name": "last_name, first_name"})
        df_xtra.to_csv(XTRA_FILE, index=False)  # ✅ Persist the rename

    # Normalize name casing and spacing
    df_final["last_name, first_name"] = df_final["last_name, first_name"].str.strip().str.title()
    df_xtra["last_name, first_name"] = df_xtra["last_name, first_name"].str.strip().str.title()

    # Drop old stat columns if they exist
    df_final = df_final.drop(columns=["innings_pitched", "strikeouts", "walks", "earned_runs"], errors="ignore")

    # Merge by name
    df_final = df_final.merge(
        df_xtra[["last_name, first_name", "innings_pitched", "strikeouts", "walks", "earned_runs"]],
        on="last_name, first_name",
        how="left"
    )

    # Force change to ensure Git commits
    df_final["debug_timestamp"] = pd.Timestamp.now()

    # Save result
    df_final.to_csv(FINAL_FILE, index=False)
    print(f"✅ Updated pitcher stats and saved to: {FINAL_FILE}")

if __name__ == "__main__":
    main()
