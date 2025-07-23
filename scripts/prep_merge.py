import pandas as pd

# Input/Output file paths (same files)
HOME_FILE = "data/adjusted/batters_home_weather_park.csv"
AWAY_FILE = "data/adjusted/batters_away_weather_park.csv"

def normalize_team_column(file_path):
    df = pd.read_csv(file_path)

    if "team" in df.columns:
        df["team"] = df["team"].astype(str).str.title()
        df.to_csv(file_path, index=False)
        print(f"✅ Normalized 'team' column in {file_path}")
    else:
        print(f"⚠️ 'team' column not found in {file_path}")

def main():
    normalize_team_column(HOME_FILE)
    normalize_team_column(AWAY_FILE)

if __name__ == "__main__":
    main()
