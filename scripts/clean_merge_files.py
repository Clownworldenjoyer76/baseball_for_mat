import pandas as pd
import os

def clean_merge_files():
    """
    Identifies, removes, and logs duplicate rows across specified input CSV files.

    Input files:
    - data/end_chain/first/pit_hwp.csv
    - data/end_chain/first/pit_awp.csv
    - data/end_chain/first/raw/bat_awp_dirty.csv
    - data/end_chain/first/raw/bat_hwp_dirty.csv

    Output:
    - Overwrites each file with duplicates removed
    - Logs summary to: data/end_chain/duplicates.txt
    """

    input_files = [
        'data/end_chain/first/pit_hwp.csv',
        'data/end_chain/first/pit_awp.csv',
        'data/end_chain/first/raw/bat_awp_dirty.csv',
        'data/end_chain/first/raw/bat_hwp_dirty.csv'
    ]

    output_summary_file = 'data/end_chain/duplicates.txt'

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_summary_file), exist_ok=True)

    with open(output_summary_file, 'w') as f:
        f.write("--- Duplicate Row Analysis and Cleaning ---\n\n")

        for file_path in input_files:
            if not os.path.exists(file_path):
                f.write(f"File not found: {file_path}\n")
                print(f"Warning: {file_path} not found. Skipping.")
                continue

            try:
                df = pd.read_csv(file_path)
                initial_rows = len(df)
                pre_dedup = df.duplicated()
                num_duplicates = pre_dedup.sum()

                f.write(f"Analysis for: {file_path}\n")
                f.write(f"  Total rows before: {initial_rows}\n")
                f.write(f"  Duplicate rows found: {num_duplicates}\n")

                if num_duplicates > 0:
                    f.write("  Sample duplicates (first 5):\n")
                    f.write(df[pre_dedup].head().to_string() + "\n")

                # Remove duplicates
                df_cleaned = df.drop_duplicates()
                df_cleaned.to_csv(file_path, index=False)

                # Recheck for remaining duplicates
                remaining_duplicates = df_cleaned.duplicated().sum()
                f.write(f"  Rows after deduplication: {len(df_cleaned)}\n")
                f.write(f"  Remaining duplicates after cleanup: {remaining_duplicates}\n\n")

                print(f"Cleaned {file_path} → removed {num_duplicates}, remaining: {remaining_duplicates}")

            except Exception as e:
                f.write(f"Error processing {file_path}: {e}\n\n")
                print(f"Error processing {file_path}: {e}")

    print(f"\n✅ Duplicate cleanup complete. Summary written to {output_summary_file}")

if __name__ == "__main__":
    clean_merge_files()
