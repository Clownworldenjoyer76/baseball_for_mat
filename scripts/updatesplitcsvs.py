import pandas as pd
from pathlib import Path

# Input files
HOME_FILE = Path("data/adjusted/batters_home.csv")
AWAY_FILE = Path("data/adjusted/batters_away.csv")
GAMES_FILE = Path("data/raw/todaysgames_normalized.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    # Load data
    batters_home = load_csv(HOME_FILE)
    batters_away = load_csv(AWAY_FILE)
    games = load_csv(GAMES_FILE)

    # Prepare the games DataFrame for merging
    # We need the home_team and away_team for each game
    # Ensure no duplicates if games file might have them (though for today's games, it should be unique matchups)
    games_for_merge = games[['home_team', 'away_team']].drop_duplicates()

    # --- Correctly adding columns using pd.merge() ---

    # For batters_home: Merge based on 'team' in batters_home and 'home_team' in games
    # This will add 'away_team' and 'home_team' columns from 'games_for_merge' to 'batters_home'
    # We use a 'left' merge to keep all rows from batters_home
    batters_home = pd.merge(batters_home, games_for_merge,
                            left_on='team',    # Column in batters_home
                            right_on='home_team', # Column in games_for_merge
                            how='left',
                            suffixes=('_batter', '')) # Suffixes to distinguish columns if names clash (e.g., if batters_home also had 'home_team')
                                                    # '' for the right DataFrame means no suffix for its columns

    # For batters_away: Merge based on 'team' in batters_away and 'away_team' in games
    # This will add 'home_team' and 'away_team' columns from 'games_for_merge' to 'batters_away'
    batters_away = pd.merge(batters_away, games_for_merge,
                            left_on='team',    # Column in batters_away
                            right_on='away_team', # Column in games_for_merge
                            how='left',
                            suffixes=('_batter', ''))


    # --- Important Consideration: Duplicates from Merge ---
    # If a 'team' in batters_home/away plays multiple games today (e.g., a doubleheader scenario,
    # or if your 'games' file lists different game types for the same teams),
    # the merge might create duplicate rows in batters_home/away if there are multiple matches
    # in the 'games_for_merge' DataFrame.
    # You might need to add a .drop_duplicates() on relevant columns or refine your merge key
    # if this is not the desired behavior.
    # For a typical "today's games" file, each team should only have one home/away game.


    # Save updated files
    batters_home.to_csv(HOME_FILE, index=False)
    batters_away.to_csv(AWAY_FILE, index=False)

    print("✅ Corrected home_team and away_team values added to both files.")

if __name__ == "__main__":
    main()
