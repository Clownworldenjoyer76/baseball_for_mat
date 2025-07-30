import pandas as pd
from pathlib import Path

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

def main():
    df_final = pd.read_csv(FINAL_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    # Format name to 'Last, First'
    df_xtra["last_name, first_name"] = df_xtra["last_name_first_name"].str.title().str.replace(" ", ", ", n=1)

    # Merge and inject stat columns
    df_final = df_final.drop(columns=["innings_pitched", "strikeouts", "walks", "earned_runs"], errors="ignore")
    df_final = df_final.merge(
        df_xtra[["last_name, first_name", "innings_pitched", "strikeouts", "walks", "earned_runs"]],
        on="last_name, first_name",
        how="left"
    )

    df_final.to_csv(FINAL_FILE, index=False)
    print(f"✅ Overwrote: {FINAL_FILE}")

if __name__ == "__main__":
    main()
