import pandas as pd

# File paths
BATTERS_HOME_FILE = "data/adjusted/batters_home.csv"
BATTERS_AWAY_FILE = "data/adjusted/batters_away.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
WEATHER_FILE = "data/weather_adjustments.csv"
OUTPUT_HOME = "data/adjusted/batters_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/batters_away_weather.csv"
LOG_HOME = "log_weather_home.txt"
LOG_AWAY = "log_weather_away.txt"

def apply_adjustment(df, team_col, stadium_map, weather_df):
    df = df.merge(stadium_map, left_on=team_col, right_on='team', how='left')
    df = df.merge(weather_df, on='stadium', how='left')
    df['adj_woba_weather'] = df['woba'] * df['temperature'].apply(
        lambda x: 1.02 if x > 85 else 0.98 if x < 60 else 1.0
    )
    return df

def main():
    # Load data
    batters_home = pd.read_csv(BATTERS_HOME_FILE)
    batters_away = pd.read_csv(BATTERS_AWAY_FILE)
    stadiums = pd.read_csv(STADIUM_FILE)
    weather = pd.read_csv(WEATHER_FILE)

    # Build home team → stadium map
    home_stadium_map = stadiums[['home_team', 'stadium']].rename(columns={'home_team': 'team'})
    away_stadium_map = stadiums[['away_team', 'stadium']].rename(columns={'away_team': 'team'})

    # Apply adjustments
    adjusted_home = apply_adjustment(batters_home, 'team', home_stadium_map, weather)
    adjusted_away = apply_adjustment(batters_away, 'team', away_stadium_map, weather)

    # Save outputs
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)

    # Log top 5 for each
    top5_home = adjusted_home.sort_values('adj_woba_weather', ascending=False).head(5)
    top5_away = adjusted_away.sort_values('adj_woba_weather', ascending=False).head(5)

    with open(LOG_HOME, "w") as f:
        f.write("Top 5 Home Batters by adj_woba_weather:\n")
        f.write(top5_home[["name", "team", "woba", "temperature", "adj_woba_weather"]].to_string(index=False))

    with open(LOG_AWAY, "w") as f:
        f.write("Top 5 Away Batters by adj_woba_weather:\n")
