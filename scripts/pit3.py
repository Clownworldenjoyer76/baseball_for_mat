import pandas as pd
from pathlib import Path

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")

def to_last_first(name):
    parts = str(name).strip().split()
    return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) > 1 else name

def main():
    if not XTRA_FILE.exists():
        raise FileNotFoundError(f"❌ Missing input file: {XTRA_FILE}")

    df_final = pd.read_csv(FINAL_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    df_xtra["last_name, first_name"] = df_xtra["last_name_first_name"].apply(to_last_first)

    df_final = df_final.drop(columns=["innings_pitched", "strikeouts", "walks", "earned_runs"], errors="ignore")
    df_final = df_final.merge(
        df_xtra[["last_name, first_name", "innings_pitched", "strikeouts", "walks", "earned_runs"]],
        on="last_name, first_name",
        how="left"
    )

    # ✅ Force change so Git always commits
    df_final["debug_timestamp"] = pd.Timestamp.now()

    df_final.to_csv(FINAL_FILE, index=False)
    print(f"✅ Overwrote: {FINAL_FILE}")

if __name__ == "__main__":
    main()
