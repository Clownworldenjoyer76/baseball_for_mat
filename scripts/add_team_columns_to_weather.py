import pandas as pd

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_teams.csv"

def create_weather_teams_file():
    # Load input
    df = pd.read_csv(INPUT_FILE)

    # Rename team_name_x → home_team
    df.rename(columns={"team_name_x": "home_team"}, inplace=True)

    # Select only required columns
    output_df = df[["home_team", "away_team"]]

    # Save output
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote home_team and away_team to {OUTPUT_FILE}")

def main():
    create_weather_teams_file()

if __name__ == "__main__":
    main()
