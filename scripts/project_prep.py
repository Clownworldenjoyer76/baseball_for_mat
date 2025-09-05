# scripts/project_prep.py
import pandas as pd

def project_prep():
    PATHS = {
        "bat_today_final": "data/end_chain/final/bat_today_final.csv",
        "batter_away_final": "data/end_chain/final/batter_away_final.csv",
        "batter_home_final": "data/end_chain/final/batter_home_final.csv",
        "startingpitchers": "data/end_chain/final/startingpitchers.csv",
        "pitchers_normalized_cleaned": "data/cleaned/pitchers_normalized_cleaned.csv",
        "todays_games": "data/raw/todaysgames_normalized.csv",
        "weather_input": "data/weather_input.csv",
        "weather_adjustments": "data/weather_adjustments.csv",
        "stadium_master": "data/manual/stadium_master.csv",
        "final_scores_projected": "data/_projections/final_scores_projected.csv"
    }

    try:
        df_bat_today = pd.read_csv(PATHS["bat_today_final"])
        df_batter_away = pd.read_csv(PATHS["batter_away_final"])
        df_batter_home = pd.read_csv(PATHS["batter_home_final"])
        df_startingpitchers = pd.read_csv(PATHS["startingpitchers"])
        df_pitchers_normalized = pd.read_csv(PATHS["pitchers_normalized_cleaned"])
        df_todays_games = pd.read_csv(PATHS["todays_games"])
        df_weather_input = pd.read_csv(PATHS["weather_input"])
        df_weather_adjustments = pd.read_csv(PATHS["weather_adjustments"])
        df_stadium_master = pd.read_csv(PATHS["stadium_master"])
        try:
            df_final_scores_projected = pd.read_csv(PATHS["final_scores_projected"])
        except FileNotFoundError:
            df_final_scores_projected = pd.DataFrame()
    except Exception as e:
        print(f"Load error: {e}")
        return

    # ---- Ensure player_id exists ----
    if "player_id" not in df_startingpitchers.columns:
        print("❌ startingpitchers.csv missing player_id")
        return

    # ---- Ensure game_id exists ----
    if "game_id" not in df_startingpitchers.columns:
        if "pitcher_home_id" in df_todays_games.columns:
            games_long = pd.concat([
                df_todays_games[["game_id", "home_team_id", "pitcher_home_id"]].rename(
                    columns={"home_team_id": "team_id", "pitcher_home_id": "player_id"}
                ),
                df_todays_games[["game_id", "away_team_id", "pitcher_away_id"]].rename(
                    columns={"away_team_id": "team_id", "pitcher_away_id": "player_id"}
                )
            ], ignore_index=True)
            df_startingpitchers = df_startingpitchers.merge(
                games_long, on="player_id", how="left"
            )
        else:
            df_startingpitchers["game_id"] = "UNKNOWN"

    # ---- Add stadium info from stadium_master ----
    if "team_id" in df_startingpitchers.columns:
        df_startingpitchers = df_startingpitchers.merge(
            df_stadium_master[["team_id", "city", "state", "timezone", "is_dome"]],
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
