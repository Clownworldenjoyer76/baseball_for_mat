import pandas as pd
import os

def clean_merge_files():
    """
    Identifies and summarizes duplicate rows across specified input CSV files.

    Input files:
    - data/end_chain/first/pit_hwp.csv
    - data/end_chain/first/pit_awp.csv
    - data/end_chain/first/raw/bat_awp_dirty.csv
    - data/end_chain/first/raw/bat_hwp_dirty.csv

    Output file:
    - data/end_chain/duplicates.txt (summary of findings)
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
        f.write("--- Duplicate Row Analysis ---\n\n")

        for file_path in input_files:
            if not os.path.exists(file_path):
                f.write(f"File not found: {file_path}\n")
                print(f"Warning: {file_path} not found. Skipping.")
                continue

            try:
                df = pd.read_csv(file_path)
                initial_rows = len(df)
                duplicates = df[df.duplicated()]
                num_duplicates = len(duplicates)

                f.write(f"Analysis for: {file_path}\n")
                f.write(f"  Total rows: {initial_rows}\n")
                f.write(f"  Number of duplicate rows found: {num_duplicates}\n")

                if num_duplicates > 0:
                    f.write("  Sample of duplicate rows (first 5):\n")
                    f.write(duplicates.head().to_string() + "\n")
                else:
                    f.write("  No duplicate rows found.\n")
                f.write("\n")
                print(f"Processed {file_path}. Found {num_duplicates} duplicates.")

            except Exception as e:
                f.write(f"Error processing {file_path}: {e}\n\n")
                print(f"Error processing {file_path}: {e}")

    print(f"\nDuplicate row analysis summary saved to {output_summary_file}")

if __name__ == "__main__":
    clean_merge_files()
