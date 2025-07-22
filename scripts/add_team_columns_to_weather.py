import pandas as pd

def main():
    weather_path = "data/weather_adjustments.csv"
    input_path = "data/weather_input.csv"

    # Load files
    weather_df = pd.read_csv(weather_path)
    input_df = pd.read_csv(input_path)

    # Add home_team column (mirror 'stadium')
    weather_df["home_team"] = weather_df["stadium"]

    # Merge to get away_team from input file
    input_df = input_df[["stadium", "away_team"]]
    merged = weather_df.merge(input_df, on="stadium", how="left")

    # Save updated weather_adjustments.csv
    merged.to_csv(weather_path, index=False)
    print(f"✅ Updated file: {weather_path} with home_team and away_team")

if __name__ == "__main__":
    main()
