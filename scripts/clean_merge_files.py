import pandas as pd
import os
import tempfile
import shutil

def clean_merge_files():
    """
    Deduplicate rows in-place for:
      - data/end_chain/first/pit_hwp.csv
      - data/end_chain/first/pit_awp.csv
      - data/end_chain/first/raw/bat_awp_dirty.csv
      - data/end_chain/first/raw/bat_hwp_dirty.csv

    Writes summary to data/end_chain/duplicates.txt
    """
    input_files = [
        'data/end_chain/first/pit_hwp.csv',
        'data/end_chain/first/pit_awp.csv',
        'data/end_chain/first/raw/bat_awp_dirty.csv',
        'data/end_chain/first/raw/bat_hwp_dirty.csv'
    ]
    summary_path = 'data/end_chain/duplicates.txt'
    os.makedirs(os.path.dirname(summary_path), exist_ok=True)

    def dedup_subset_for(df: pd.DataFrame) -> list | None:
        # Prefer id-based dedup if available; otherwise full-row
        keys = []
        for k in ('player_id', 'game_id'):
            if k in df.columns:
                keys.append(k)
        return keys or None

    with open(summary_path, 'w') as f:
        f.write("--- Duplicate Row Analysis and Cleaning ---\n\n")
        for file_path in input_files:
            if not os.path.exists(file_path):
                msg = f"File not found: {file_path}\n"
                f.write(msg)
                print(f"Warning: {msg.strip()} Skipping.")
                continue

            try:
                df = pd.read_csv(file_path, low_memory=False)
                initial_rows = len(df)
                subset = dedup_subset_for(df)
                pre_dupe_mask = df.duplicated(subset=subset, keep='first')
                num_dupes = int(pre_dupe_mask.sum())

                f.write(f"Analysis for: {file_path}\n")
                f.write(f"  Total rows before: {initial_rows}\n")
                f.write(f"  Dedup subset: {subset if subset else 'FULL ROW'}\n")
                f.write(f"  Duplicate rows found: {num_dupes}\n")
                if num_dupes > 0:
                    f.write("  Sample duplicates (first 5):\n")
                    f.write(df[pre_dupe_mask].head().to_string() + "\n")

                df_clean = df.drop_duplicates(subset=subset, keep='first')
                remaining_dupes = int(df_clean.duplicated(subset=subset, keep='first').sum())
                rows_after = len(df_clean)

                # Atomic write: tmp → replace
                d = os.path.dirname(file_path) or "."
                with tempfile.NamedTemporaryFile('w', delete=False, dir=d, suffix='.csv') as tmp:
                    df_clean.to_csv(tmp.name, index=False)
                    tmp_path = tmp.name
                shutil.move(tmp_path, file_path)

                f.write(f"  Rows after deduplication: {rows_after}\n")
                f.write(f"  Remaining duplicates after cleanup: {remaining_dupes}\n\n")

                # Clear, accurate console message
                print(
                    f"Cleaned {file_path} → removed_dupes: {num_dupes}, "
                    f"rows_after: {rows_after}, dupes_remaining: {remaining_dupes}"
                )

            except Exception as e:
                f.write(f"Error processing {file_path}: {e}\n\n")
                print(f"Error processing {file_path}: {e}")

    print(f"\n✅ Duplicate cleanup complete. Summary written to {summary_path}")

if __name__ == "__main__":
    clean_merge_files()
