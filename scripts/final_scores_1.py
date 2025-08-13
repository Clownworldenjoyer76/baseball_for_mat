# final_scores_1.py

import pandas as pd
import numpy as np
import os

# --- Configuration ---
# This section defines the file paths and model parameters.

# Input files
SCHEDULE_PATH = 'data/bets/mlb_sched.csv'
BATTER_STRENGTH_PATH = 'data/bets/prep/batter_props_final.csv'
PITCHER_STRENGTH_PATH = 'data/bets/prep/pitcher_props_bets.csv'

# Output file
OUTPUT_PATH = 'data/bets/game_props_history.csv'

# Model Parameters for Score Projection
# This is a baseline average for runs scored by a team in a game.
AVG_RUNS_PER_GAME = 4.5
# This factor adjusts how much the z-scores (strength metrics) influence the run projection.
Z_SCORE_SCALING_FACTOR = 1.0


def aggregate_team_strength(batter_df, pitcher_df):
    """
    Aggregates individual player data to create average team strength metrics.
    - 'batter_z' is assumed to be the metric for batting strength.
    - 'pitcher_z' is assumed to be the metric for pitching strength.
    """
    # Group by team and calculate the mean strength. Higher is better.
    team_batting = batter_df.groupby('team_abbr')['batter_z'].mean().reset_index()
    team_batting = team_batting.rename(columns={'batter_z': 'team_batter_z'})

    team_pitching = pitcher_df.groupby('team_abbr')['pitcher_z'].mean().reset_index()
    team_pitching = team_pitching.rename(columns={'pitcher_z': 'team_pitcher_z'})

    # Merge batting and pitching strengths into a single DataFrame.
    team_stats = pd.merge(team_batting, team_pitching, on='team_abbr', how='outer')
    return team_stats


def main():
    """
    Main function to execute the script logic.
    """
    print("Starting script...")

    # --- 1. Load Data ---
    # Start with the main schedule file. This is our base.
    try:
        games_df = pd.read_csv(SCHEDULE_PATH)
        batter_df = pd.read_csv(BATTER_STRENGTH_PATH)
        pitcher_df = pd.read_csv(PITCHER_STRENGTH_PATH)
    except FileNotFoundError as e:
        print(f"Error: Input file not found. {e}")
        return

    # --- 2. Aggregate and Merge Strength Data ---
    print("Aggregating team strengths...")
    team_stats = aggregate_team_strength(batter_df, pitcher_df)

    print("Merging strengths into games list...")
    # Merge stats for the HOME team
    games_df = pd.merge(games_df, team_stats, left_on='home_team', right_on='team_abbr', how='left')
    games_df = games_df.rename(columns={'team_batter_z': 'home_batter_z', 'team_pitcher_z': 'home_pitcher_z'})
    games_df = games_df.drop('team_abbr', axis=1)

    # Merge stats for the AWAY team
    games_df = pd.merge(games_df, team_stats, left_on='away_team', right_on='team_abbr', how='left')
    games_df = games_df.rename(columns={'team_batter_z': 'away_batter_z', 'team_pitcher_z': 'away_pitcher_z'})
    games_df = games_df.drop('team_abbr', axis=1)

    # --- 3. Compute Projected Scores and Totals ---
    print("Calculating projected scores...")
    # Formula: Avg Runs + (Team's Offense Strength - Opponent's Pitching Strength) * Scale Factor
    games_df['home_score_proj'] = AVG_RUNS_PER_GAME + (games_df['home_batter_z'] - games_df['away_pitcher_z']) * Z_SCORE_SCALING_FACTOR
    games_df['away_score_proj'] = AVG_RUNS_PER_GAME + (games_df['away_batter_z'] - games_df['home_pitcher_z']) * Z_SCORE_SCALING_FACTOR

    # Sum the projected scores for a total projected run count.
    games_df['projected_real_run_total'] = games_df['home_score_proj'] + games_df['away_score_proj']

    # --- 4. Determine Projected Favorite ---
    # Use numpy.where to efficiently check which team has a higher projected score.
    games_df['favorite_projected'] = np.where(
        games_df['home_score_proj'] > games_df['away_score_proj'],
        games_df['home_team'],
        games_df['away_team']
    )
    # Handle the rare case of a projected tie by defaulting to the away team.
    games_df['favorite_projected'] = np.where(
        games_df['home_score_proj'] == games_df['away_score_proj'],
        'TIE',
        games_df['favorite_projected']
    )

    # --- 5. Add Final Score Columns and Evaluate Favorite ---
    print("Adding final score columns and evaluating projections...")
    # These columns are for the actual, final results of the game.
    # We rename existing score columns if they exist, otherwise create empty ones.
    if 'home_score' in games_df.columns and 'away_score' in games_df.columns:
        games_df = games_df.rename(columns={'home_score': 'home_score_final', 'away_score': 'away_score_final'})
    else:
        # Create placeholder columns if no scores are in the source file
        games_df['home_score_final'] = np.nan
        games_df['away_score_final'] = np.nan

    # Calculate the total actual runs from the final scores.
    games_df['real_run_total'] = games_df['home_score_final'] + games_df['away_score_final']

    # Determine the actual winner of the game.
    conditions = [
        (games_df['home_score_final'] > games_df['away_score_final']),
        (games_df['away_score_final'] > games_df['home_score_final'])
    ]
    choices = [games_df['home_team'], games_df['away_team']]
    actual_winner = np.select(conditions, choices, default=None)

    # Check if the projected favorite was the actual winner.
    # Result is True, False, or None (if game hasn't been played).
    games_df['favorite_correct'] = np.where(
        pd.notna(actual_winner),
        games_df['favorite_projected'] == actual_winner,
        None
    ).astype('boolean') # Use nullable boolean type


    # --- 6. Save Output ---
    # Ensure the output directory exists before saving.
    output_dir = os.path.dirname(OUTPUT_PATH)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Reorder and select final columns for clarity in the output file
    final_cols = [
        'game_pk', 'game_date', 'status',
        'home_team', 'away_team',
        'home_score_proj', 'away_score_proj', 'projected_real_run_total',
        'home_score_final', 'away_score_final', 'real_run_total',
        'favorite_projected', 'favorite_correct',
        'home_batter_z', 'home_pitcher_z', 'away_batter_z', 'away_pitcher_z'
    ]
    # Filter for columns that actually exist in the dataframe to prevent errors
    cols_to_keep = [col for col in final_cols if col in games_df.columns]
    
    games_df[cols_to_keep].to_csv(OUTPUT_PATH, index=False)
    print(f"Script finished. Output saved to {OUTPUT_PATH}")


if __name__ == '__main__':
    main()
