import pandas as pd

TEAM_MAP_FILE = "data/Data/team_name_map.csv"
PITCHERS_FILE = "data/daily/todays_pitchers.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
OUTPUT_FILE = "data/weather_input.csv"

def load_team_map():
    df = pd.read_csv(TEAM_MAP_FILE)
    return dict(zip(df["name"].str.strip().str.lower(), df["team"].str.strip()))

def normalize_team_name(name, team_map):
    return team_map.get(name.strip().lower(), name.strip())

def generate_weather_input():
    team_map = load_team_map()

    pitchers_df = pd.read_csv(PITCHERS_FILE)
    stadium_df = pd.read_csv(STADIUM_FILE)

    # Normalize home_team using the team map
    pitchers_df['home_team'] = pitchers_df['home_team'].astype(str).apply(lambda x: normalize_team_name(x, team_map))
    stadium_df['home_team'] = stadium_df['home_team'].astype(str).apply(lambda x: normalize_team_name(x, team_map))

    # Merge only on matching home_team values
    merged_df = pd.merge(
        pitchers_df[['home_team', 'game_time']],
        stadium_df[['home_team', 'venue', 'city', 'state', 'timezone', 'is_dome', 'latitude', 'longitude']],
        on='home_team',
        how='inner'
    )

    merged_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Created {OUTPUT_FILE} with {len(merged_df)} rows")

if __name__ == "__main__":
    generate_weather_input()