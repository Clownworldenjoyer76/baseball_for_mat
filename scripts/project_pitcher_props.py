import pandas as pd
from pathlib import Path
from projection_formulas import calculate_all_projections
from utils import safe_col

# File paths
FINAL_FILE = Path("data/end_chain/final/startingpitchers_final.csv")
XTRA_FILE = Path("data/end_chain/cleaned/pitchers_xtra_normalized.csv")
OUTPUT_FILE = Path("data/end_chain/complete/pitcher_props_projected.csv")

def main():
    print("🔄 Loading pitcher files...")
    df_final = pd.read_csv(FINAL_FILE)
    df_xtra = pd.read_csv(XTRA_FILE)

    print("🧼 Cleaning & aligning columns...")
    df_final["last_name, first_name"] = df_final["last_name, first_name"].astype(str).str.strip().str.title()
    df_xtra["last_name, first_name"] = df_xtra["last_name, first_name"].astype(str).str.strip().str.title()

    # Merge extra stats into final
    df = df_final.merge(
        df_xtra,
        on="last_name, first_name",
        how="left"
    )

    # Fill missing numeric fields with 0
    for col in ["innings_pitched", "k_percent", "bb_percent", "era", "hits_per_9"]:
        df[col] = safe_col(df, col, 0)

    # Apply full projection formula logic
    df = calculate_all_projections(df)

    # Optional: also include basic traditional props
    df["projected_strikeouts"] = (df["k_percent"] / 100 * (df["innings_pitched"] / 3)).round(2)
    df["projected_walks"] = (df["bb_percent"] / 100 * (df["innings_pitched"] / 3)).round(2)
    df["projected_outs"] = (df["innings_pitched"] * 3).round(2)
    df["projected_hits_allowed"] = (df["hits_per_9"] * df["innings_pitched"] / 9).round(2)
    df["projected_earned_runs"] = (df["era"] * df["innings_pitched"] / 9).round(2)

    # Final columns to output
    output_cols = [
        "last_name, first_name",
        "innings_pitched",
        "projected_total_bases",
        "projected_hits",
        "projected_walks",
        "projected_singles",
        "projected_rbi",
        "projected_home_runs",
        "projected_strikeouts",
        "projected_outs",
        "projected_hits_allowed",
        "projected_earned_runs",
    ]
    if "team" in df.columns:
        output_cols.insert(1, "team")  # Place team after name

    df[output_cols].to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved pitcher projections to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
