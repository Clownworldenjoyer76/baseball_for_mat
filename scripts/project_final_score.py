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
            # Depending on desired behavior, you might want to exit here
            # return
    except FileNotFoundError:
        print(f"DEBUG: ERROR: Input file not found: {BATTER_PROPS_FILE}")
        print("--- DEBUG END (FAILURE) ---")
        return # Exit if the input file doesn't exist
    except pd.errors.EmptyDataError:
        print(f"DEBUG: ERROR: Input file is empty: {BATTER_PROPS_FILE}")
        print("--- DEBUG END (FAILURE) ---")
        return # Exit if the input file is empty
    except Exception as e:
        print(f"DEBUG: ERROR: An unexpected error occurred while loading {BATTER_PROPS_FILE}: {e}")
        print("--- DEBUG END (FAILURE) ---")
        return

    print("\n🧠 Projecting final scores...")
    try:
        # Before calling, check if df is still valid if previous steps allowed continuation
        if df.empty:
            print("DEBUG: Skipping project_final_score as input DataFrame is empty.")
            final_df = pd.DataFrame() # Ensure final_df is defined as empty if input is empty
        else:
            final_df = project_final_score(df)
            print(f"DEBUG: Successfully called 'project_final_score'.")
            print(f"DEBUG: Projected DataFrame head:\n{final_df.head()}")
            print(f"DEBUG: Projected DataFrame shape: {final_df.shape}")
            if final_df.empty:
                print("DEBUG: WARNING: 'project_final_score' returned an empty DataFrame!")
    except Exception as e:
        print(f"DEBUG: ERROR: An error occurred during score projection in 'project_final_score()': {e}")
        print("--- DEBUG END (FAILURE) ---")
        return # Exit if the projection fails

    print("\n💾 Saving to:", OUTPUT_FILE)
    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        print(f"DEBUG: Directory '{OUTPUT_FILE.parent}' ensured to exist.")
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"DEBUG: Successfully saved data to '{OUTPUT_FILE}'.")
        print("✅ Done.")
    except Exception as e:
        print(f"DEBUG: ERROR: An error occurred saving output file {OUTPUT_FILE}: {e}")
        print("--- DEBUG END (FAILURE) ---")
        return

    print("\n--- DEBUG END (SUCCESS) ---")

if __name__ == "__main__":
    main()
