# chain_setup_inputs.py

import os
import shutil

def setup_chain_inputs():
    """
    Creates the data/end_chain directory and copies necessary input files into it.
    """
    # --- Configuration ---
    # The destination directory for the copied files.
    destination_dir = 'data/end_chain'

    # The list of source files to copy.
    # Please modify this list to include the actual paths to your input files.
    # Paths can be relative to the script's location or absolute.
    files_to_copy = [
        'data/system.inpcrd',
        'data/system.prmtop',
        'data/md_config.in'
    ]

    print(f"🚀 Setting up inputs for the next chain step...")
    print(f"Destination directory: '{destination_dir}'\n")

    # --- Create Destination Directory ---
    try:
        os.makedirs(destination_dir, exist_ok=True)
        print(f"✅ Directory '{destination_dir}' is ready.")
    except OSError as e:
        print(f"❌ Error creating directory {destination_dir}: {e}")
        return # Exit if directory creation fails

    # --- Copy Files ---
    files_copied_count = 0
    for file_path in files_to_copy:
        # Check if the source file exists before attempting to copy
        if os.path.exists(file_path):
            try:
                # Construct the full destination path
                dest_file_path = os.path.join(destination_dir, os.path.basename(file_path))
                
                # Copy the file, preserving metadata
                shutil.copy2(file_path, dest_file_path)
                print(f"  -> Copied '{file_path}' to '{dest_file_path}'")
                files_copied_count += 1
            except Exception as e:
                print(f"  -> ❌ Error copying '{file_path}': {e}")
        else:
            print(f"  -> ⚠️ Warning: Source file not found at '{file_path}'. Skipping.")

    print(f"\n✨ Setup complete. Copied {files_copied_count} of {len(files_to_copy)} files.")

if __name__ == "__main__":
    setup_chain_inputs()

