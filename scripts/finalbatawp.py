import pandas as pd
import os
import subprocess

def final_bat_awp():
    """
    Processes baseball data by merging various input files to create a final
    batting average with runners in scoring position (AWP) dataset.

    Input files:
    - data/end_chain/cleaned/games_today_cleaned.csv
    - data/weather_adjustments.csv
    - data/weather_input.csv
    - data/end_chain/cleaned/bat_awp_cleaned.csv

    Output file:
    - data/end_chain/final/finalbatawp.csv
    """

    # Load input files
    try:
        games_today_df = pd.read_csv('data/end_chain/cleaned/games_today_cleaned.csv')
        weather_adjustments_df = pd.read_csv('data/weather_adjustments.csv')
        weather_input_df = pd.read_csv('data/weather_input.csv')
        bat_awp_df = pd.read_csv('data/end_chain/cleaned/bat_awp_cleaned.csv')
    except FileNotFoundError as e:
        print(f"Error: One of the input files was not found. Please ensure all files are in the correct directory.")
        print(f"Missing file: {e.filename}")
        return

    # Merge with games_today_cleaned.csv to get home_team and game_time
    final_df = pd.merge(
        bat_awp_df,
        games_today_df[['away_team', 'home_team', 'game_time']],
        on='away_team',
        how='left'
    )

    # Merge with weather_adjustments.csv
    weather_cols_to_merge = [
        'away_team', 'venue', 'location', 'temperature', 'wind_speed',
        'wind_direction', 'humidity', 'precipitation', 'condition', 'notes'
    ]
    final_df = pd.merge(
        final_df,
        weather_adjustments_df[weather_cols_to_merge],
        on='away_team',
        how='left'
    )

    # Merge with weather_input.csv to get Park Factor
    final_df = pd.merge(
        final_df,
        weather_input_df[['away_team', 'Park Factor']],
        on='away_team',
        how='left'
    )

    # Define the output path and filename for finalbatawp.csv
    output_directory = 'data/end_chain/final'
    output_filename = 'finalbatawp.csv'
    output_filepath = os.path.join(output_directory, output_filename)

    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Save the final DataFrame to a CSV file
    final_df.to_csv(output_filepath, index=False)
    print(f"✅ Successfully created '{output_filepath}'")

    # --- New Section for bat_hwp_cleaned.csv normalization ---
    hwp_input_filepath = 'data/end_chain/cleaned/prep/bat_hwp_cleaned.csv'
    hwp_output_filepath = 'data/end_chain/cleaned/bat_awp_clean2.csv'

    try:
        hwp_df = pd.read_csv(hwp_input_filepath)

        # Normalize 'team' column: capitalize first letter, remove trailing commas/whitespace
        if 'team' in hwp_df.columns:
            hwp_df['team'] = hwp_df['team'].astype(str).str.capitalize().str.replace(r',$', '', regex=True).str.strip()
            print(f"✅ Normalized 'team' column in '{hwp_input_filepath}'.")
        else:
            print(f"⚠️ Warning: 'team' column not found in '{hwp_input_filepath}'. Skipping normalization.")

        # Ensure output directory for bat_awp_clean2.csv exists
        os.makedirs(os.path.dirname(hwp_output_filepath), exist_ok=True)

        # Output updated file
        hwp_df.to_csv(hwp_output_filepath, index=False)
        print(f"✅ Successfully created '{hwp_output_filepath}' with normalized data.")

    except FileNotFoundError:
        print(f"❌ Error: Input file for normalization not found: '{hwp_input_filepath}'.")
    except Exception as e:
        print(f"❌ Error during bat_hwp_cleaned.csv normalization: {e}")
    # --- End of New Section ---

    # Git commit and push for both files
    try:
        # Add finalbatawp.csv
        subprocess.run(["git", "add", output_filepath], check=True)
        # Add bat_awp_clean2.csv
        subprocess.run(["git", "add", hwp_output_filepath], check=True)

        commit_message = f"📊 Auto-update {output_filename} and normalize bat_hwp_cleaned.csv"
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✅ Pushed to repository.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git push failed: {e}")
        # Detailed error output for debugging
        if e.stderr:
            print("Git stderr:", e.stderr.decode())
        if e.stdout:
            print("Git stdout:", e.stdout.decode())

if __name__ == "__main__":
    final_bat_awp()
