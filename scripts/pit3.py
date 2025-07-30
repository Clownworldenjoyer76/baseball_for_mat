import pandas as pd
from pathlib import Path

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

def to_last_first(name):
    parts = str(name).strip().split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

def main():
    if not XTRA_FILE.exists():
        raise FileNotFoundError(f"❌ Missing input file: {XTRA_FILE}")

    # Load files
    df_final = pd.read_csv(FINAL_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    # Convert and rename the key name column
    df_xtra["last_name, first_name"] = df_xtra["last_name_first_name"].apply(to_last_first).str.strip().str.title()
    df_xtra.drop(columns=["last_name_first_name"], inplace=True)
    df_xtra.to_csv(XTRA_FILE, index=False)  # ✅ Save the updated CSV with renamed column

    # Normalize name casing in final file
    df_final["last_name, first_name"] = df_final["last_name, first_name"].str.strip().str.title()

    # Merge and inject stat columns
    df_final = df_final.drop(columns=["innings_pitched", "strikeouts", "walks", "earned_runs"], errors="ignore")
    df_final = df_final.merge(
        df_xtra[["last_name, first_name", "innings_pitched", "strikeouts", "walks", "earned_runs"]],
        on="last_name, first_name",
        how="left"
    )

    # Force GitHub commit
    df_final["debug_timestamp"] = pd.Timestamp.now()

    df_final.to_csv(FINAL_FILE, index=False)
    print(f"✅ Overwrote: {FINAL_FILE}")
    print(f"✅ Renamed column and updated: {XTRA_FILE}")

if __name__ == "__main__":
    main()
