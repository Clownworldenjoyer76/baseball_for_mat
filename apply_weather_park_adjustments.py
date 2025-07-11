import pandas as pd

# File paths
BATTER_FILE = "data/cleaned/batters_normalized_cleaned.csv"
PITCHER_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
PARK_DAY_FILE = "data/Data/park_factors_day.csv"
PARK_NIGHT_FILE = "data/Data/park_factors_night.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

# Output paths
BATTER_OUT = "data/adjusted/batters_adjusted.csv"
PITCHER_OUT = "data/adjusted/pitchers_adjusted.csv"

def load_data():
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    park_day = pd.read_csv(PARK_DAY_FILE)
    park_night = pd.read_csv(PARK_NIGHT_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    return batters, pitchers, park_day, park_night, weather

def determine_day_night(local_game_time):
    # crude split: before 6pm is day
    return "day" if pd.to_datetime(local_game_time).hour < 18 else "night"

def apply_adjustments(df, weather, park_day, park_night):
    # Map team to stadium
    team_to_stadium = dict(zip(weather['home_team'], weather['stadium']))
    df['stadium'] = df['team'].map(team_to_stadium)

    # Merge weather by stadium
    df = df.merge(weather, on='stadium', how='left')

    # Day/night
    df['game_type'] = df['local_game_time'].apply(determine_day_night)

    # Merge correct park factors
    df = df.merge(park_day, on='stadium', how='left', suffixes=('', '_day'))
    df = df.merge(park_night, on='stadium', how='left', suffixes=('', '_night'))

    # Apply weighted park factors
    for stat in ['hr', 'single', 'double', 'triple']:
        day_val = df[f'{stat}_factor']
        night_val = df[f'{stat}_factor_night']
        df[f'{stat}_adj'] = df.apply(
            lambda row: day_val[row.name] if row['game_type'] == 'day' else night_val[row.name],
            axis=1
        )

    # Example weather factor: temperature adjustment (arbitrary multiplier)
    if 'temperature' in df.columns:
        df['temp_mult'] = df['temperature'].apply(lambda x: 1 + ((x - 70) * 0.005))
    else:
        df['temp_mult'] = 1

    return df

def main():
    print("Loading data...")
    batters, pitchers, park_day, park_night, weather = load_data()
    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")

    print("Applying adjustments to batters...")
    batters_adj = apply_adjustments(batters, weather, park_day, park_night)

    print("Applying adjustments to pitchers...")
    pitchers_adj = apply_adjustments(pitchers, weather, park_day, park_night)

    print("Saving adjusted files...")
    batters_adj.to_csv(BATTER_OUT, index=False)
    pitchers_adj.to_csv(PITCHER_OUT, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
