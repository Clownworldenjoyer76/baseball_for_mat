import pandas as pd


def split_batters_by_team_location(
    batters_today_path, todaysgames_normalized_path, batters_home_path, batters_away_path
):
    """
    Splits batters data into home and away teams based on today's games.

    Args:
        batters_today_path (str): Path to the batters_today.csv file.
        todaysgames_normalized_path (str): Path to the todaysgames_normalized.csv file.
        batters_home_path (str): Path to save the batters_home.csv file.
        batters_away_path (str): Path to save the batters_away.csv file.
    """
    try:
        batters_df = pd.read_csv(batters_today_path)
        games_df = pd.read_csv(todaysgames_normalized_path)
    except FileNotFoundError as e:
        print(f"Error: One of the input files not found: {e}")
        return
    except pd.errors.EmptyDataError as e:
        print(f"Error: One of the input files is empty: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred while reading CSV files: {e}")
        return

    home_teams = games_df["home_team"].unique()
    away_teams = games_df["away_team"].unique()

    batters_home_df = batters_df[batters_df["team"].isin(home_teams)].copy()
    batters_away_df = batters_df[batters_df["team"].isin(away_teams)].copy()

    try:
        batters_home_df.to_csv(batters_home_path, index=False)
        batters_away_df.to_csv(batters_away_path, index=False)
    except Exception as e:
        print(f"Error: Could not write output CSV files: {e}")


if __name__ == "__main__":
    split_batters_by_team_location(
        "data/cleaned/batters_today.csv",
        "data/raw/todaysgames_normalized.csv",
        "data/adjusted/batters_home.csv",
        "data/adjusted/batters_away.csv",
    )
