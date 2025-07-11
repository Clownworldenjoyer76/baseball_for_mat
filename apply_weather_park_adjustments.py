import pandas as pd

# Input files
BATTER_FILE = "data/cleaned/batters_normalized_cleaned.csv"
PITCHER_FILE = "data/cleaned/pitchers_normalized_cleaned.csv"
WEATHER_FILE = "data/Data/weather_adjustments.csv"
PARK_DAY_FILE = "data/Data/park_factors_day.csv"
PARK_NIGHT_FILE = "data/Data/park_factors_night.csv"

# Output file
OUTPUT_FILE = "data/adjusted_projections.csv"

def determine_day_night(game_time):
    try:
        hour = int(game_time.split(":")[0])
        if "PM" in game_time and hour >= 6:
            return "night"
        elif "AM" in game_time:
            return "day"
        elif "PM" in game_time and hour < 6:
            return "day"
    except:
        return "day"
    return "day"

def apply_adjustments(df, weather_df, park_day_df, park_night_df):
    df['day_night'] = df['game_time'].apply(determine_day_night)
    df = df.merge(weather_df, on='home_team', how='left')

    # Choose park factors by time
    df = df.merge(park_day_df, on='home_team', how='left', suffixes=('', '_park_day'))
    df = df.merge(park_night_df, on='home_team', how='left', suffixes=('', '_park_night'))

    for col in ['HR', '1B', '2B', '3B', 'K', 'BB']:
        df[f'park_factor_{col.lower()}'] = df.apply(
            lambda row: row.get(f"{col}_x", row.get(f"{col}_park_day")) 
            if row['day_night'] == 'day' 
            else row.get(f"{col}_y", row.get(f"{col}_park_night")), axis=1)

    # Sample logic to adjust HR (can expand to others)
    if 'HR' in df.columns and 'park_factor_hr' in df.columns:
        df['HR_adjusted'] = df['HR'] * df['park_factor_hr'] / 100

    return df

def main():
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    park_day = pd.read_csv(PARK_DAY_FILE)
    park_night = pd.read_csv(PARK_NIGHT_FILE)

    # De-duplicate weather records
    weather = weather.drop_duplicates(subset='home_team', keep='first')

    batters_adj = apply_adjustments(batters, weather, park_day, park_night)
    pitchers_adj = apply_adjustments(pitchers, weather, park_day, park_night)

    # Combine and output
    final = pd.concat([batters_adj, pitchers_adj], ignore_index=True)
    final.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Created {OUTPUT_FILE} with {len(final)} rows.")

if __name__ == "__main__":
    main()
