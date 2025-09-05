import pandas as pd

def project_prep():
    PATHS = {
        "bat_today_final": "data/end_chain/final/bat_today_final.csv",
        "batter_away_final": "data/end_chain/final/batter_away_final.csv",
        "batter_home_final": "data/end_chain/final/batter_home_final.csv",
        "startingpitchers": "data/end_chain/final/startingpitchers.csv",
        "pitchers_normalized_cleaned": "data/cleaned/pitchers_normalized_cleaned.csv",
        "todays_games": "data/raw/todaysgames_normalized.csv",
        "stadium_metadata": "data/manual/stadium_master.csv",
        "final_scores_projected": "data/_projections/final_scores_projected.csv"
    }

    try:
        df_bat_today = pd.read_csv(PATHS["bat_today_final"])
        df_batter_away = pd.read_csv(PATHS["batter_away_final"])
        df_batter_home = pd.read_csv(PATHS["batter_home_final"])
        df_startingpitchers = pd.read_csv(PATHS["startingpitchers"])
        df_pitchers_normalized = pd.read_csv(PATHS["pitchers_normalized_cleaned"])
        df_todays_games = pd.read_csv(PATHS["todays_games"])
        df_stadium_metadata = pd.read_csv(PATHS["stadium_metadata"])
        try:
            df_final_scores_projected = pd.read_csv(PATHS["final_scores_projected"])
        except FileNotFoundError:
            df_final_scores_projected = pd.DataFrame()
    except Exception as e:
        print(f"Load error: {e}")
        return

    # ---- Ensure player_id exists ----
    if "player_id" not in df_startingpitchers.columns:
        if "last_name, first_name" in df_startingpitchers.columns and "name" in df_pitchers_normalized.columns:
            df_startingpitchers = df_startingpitchers.merge(
                df_pitchers_normalized[["name", "player_id"]],
                left_on="last_name, first_name",
                right_on="name",
                how="left"
            )
            df_startingpitchers["player_id"] = df_startingpitchers["player_id"].fillna("UNKNOWN")
            df_startingpitchers.drop(columns=["name"], inplace=True)

    # ---- Inject game_id ----
    games_long = pd.concat([
        df_todays_games[["game_id", "home_team", "pitcher_home"]].rename(
            columns={"home_team": "team", "pitcher_home": "last_name, first_name"}
        ),
        df_todays_games[["game_id", "away_team", "pitcher_away"]].rename(
            columns={"away_team": "team", "pitcher_away": "last_name, first_name"}
        )
    ], ignore_index=True)

    df_startingpitchers = df_startingpitchers.merge(
        games_long[["game_id", "last_name, first_name"]],
        on="last_name, first_name",
        how="left"
    )
    df_startingpitchers["game_id"] = df_startingpitchers["game_id"].fillna("UNKNOWN")

    # ---- Merge stadium metadata ----
    if "team_id" in df_todays_games.columns:
        df_startingpitchers = df_startingpitchers.merge(
            df_todays_games[["game_id", "home_team_id"]].rename(columns={"home_team_id": "team_id"}),
            on="game_id",
            how="left"
        )
        df_startingpitchers = df_startingpitchers.merge(
            df_stadium_metadata[["team_id", "city", "state", "timezone", "is_dome"]],
            on="team_id",
            how="left"
        )

    # ---- Save updated outputs ----
    df_batter_away.to_csv(PATHS["batter_away_final"], index=False)
    df_batter_home.to_csv(PATHS["batter_home_final"], index=False)
    df_startingpitchers.to_csv(PATHS["startingpitchers"], index=False)
    df_final_scores_projected.to_csv(PATHS["final_scores_projected"], index=False)

if __name__ == "__main__":
    project_prep()
