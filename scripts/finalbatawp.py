import pandas as pd
import os

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
    # Merge on away_team from bat_awp_df to get home_team and game_time from games_today_df
    final_df = pd.merge(
        bat_awp_df,
        games_today_df[['away_team', 'home_team', 'game_time']],
        on='away_team',
        how='left'
    )

    # Merge with weather_adjustments.csv to get weather-related information
    # Merge on away_team from bat_awp_df to get weather details from weather_adjustments_df
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
    # Merge on away_team (present in both final_df and weather_input_df) to get 'Park Factor'
    final_df = pd.merge(
        final_df,
        # Select 'away_team' and 'Park Factor' from weather_input_df
        weather_input_df[['away_team', 'Park Factor']],
        on='away_team', # Merge directly on 'away_team' as it exists in both
        how='left'
    )

    # Define the output path and filename
    output_directory = 'data/end_chain/final'
    output_filename = 'finalbatawp.csv'
    output_filepath = os.path.join(output_directory, output_filename)

    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Save the final DataFrame to a CSV file
    final_df.to_csv(output_filepath, index=False)
    print(f"Successfully created '{output_filepath}'")

if __name__ == "__main__":
    final_bat_awp()
