
import pandas as pd

WEATHER_FILE = "data/weather_adjustments.csv"
TEAM_FILE = "data/weather_teams.csv"
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
OUTPUT_HOME_FILE = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY_FILE = "data/adjusted/pitchers_away_weather.csv"

def apply_weather_adjustment(pitchers_df, weather_df, team_col, output_file):
    merged_df = pd.merge(pitchers_df, weather_df, left_on=team_col, right_on=team_col, how='left')
    if 'temperature' in merged_df.columns:
        merged_df['adj_woba_weather'] = merged_df['woba'] * (1 + (merged_df['temperature'] - 70) * 0.005)
    else:
        merged_df['adj_woba_weather'] = merged_df['woba']
    merged_df.to_csv(output_file, index=False)

def main():
    weather_df = pd.read_csv(WEATHER_FILE)
    team_df = pd.read_csv(TEAM_FILE)

    if 'stadium' not in weather_df.columns:
        raise ValueError("Missing 'stadium' column in weather_adjustments.csv")

    if 'home_team' not in team_df.columns or 'away_team' not in team_df.columns:
        raise ValueError("weather_teams.csv missing home_team or away_team column")

    # Merge team info into weather dataframe
    weather_df = pd.merge(weather_df, team_df, on='stadium', how='left')

    home_pitchers = pd.read_csv(PITCHERS_HOME_FILE)
    away_pitchers = pd.read_csv(PITCHERS_AWAY_FILE)

    apply_weather_adjustment(home_pitchers, weather_df, 'home_team', OUTPUT_HOME_FILE)
    apply_weather_adjustment(away_pitchers, weather_df, 'away_team', OUTPUT_AWAY_FILE)

if __name__ == "__main__":
    main()
