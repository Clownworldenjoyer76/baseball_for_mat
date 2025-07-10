import pandas as pd

PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"

def generate_weather_input():
    pitchers_df = pd.read_csv(PITCHERS_FILE)
    stadium_df = pd.read_csv(STADIUM_FILE)

    # Clean and match home_team
    pitchers_df['home_team'] = pitchers_df['home_team'].str.strip().str.lower()
    stadium_df['home_team'] = stadium_df['home_team'].str.strip().str.lower()

    merged = pd.merge(
        pitchers_df[['home_team', 'game_time']],
        stadium_df[['home_team', 'venue', 'city', 'state', 'timezone', 'is_dome', 'latitude', 'longitude']],
        on='home_team',
        how='inner'
    )

    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ weather_input.csv created with {len(merged)} records.")

if __name__ == "__main__":
    generate_weather_input()