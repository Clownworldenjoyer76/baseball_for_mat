import pandas as pd

def generate_game_time_updates():
    stadium_df = pd.read_csv("data/Data/stadium_metadata.csv")
    pitchers_df = pd.read_csv("data/daily/todays_pitchers.csv")

    # Clear the game_time column
    if "game_time" not in stadium_df.columns:
        stadium_df["game_time"] = ""
    else:
        stadium_df["game_time"] = ""

    # Strip spaces to ensure proper matching
    stadium_df["team"] = stadium_df["team"].str.strip()
    pitchers_df["home_team"] = pitchers_df["home_team"].str.strip()

    # Map home_team game_time to matching team in stadium_df
    for _, row in pitchers_df.iterrows():
        home_team = row["home_team"]
        game_time = row["game_time"]

        stadium_df.loc[stadium_df["team"] == home_team, "game_time"] = game_time

    # Print result for debugging
    print("Updated stadium_metadata.csv:")
    print(stadium_df[["team", "game_time"]])

    stadium_df.to_csv("data/Data/stadium_metadata.csv", index=False)

if __name__ == "__main__":
    generate_game_time_updates()
