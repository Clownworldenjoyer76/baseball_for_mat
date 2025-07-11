import pandas as pd
from datetime import datetime
from apply_adjustments import apply_adjustments
from parse_game_time import is_day_game, is_night_game

def main():
    print("Loading data...")

    batters = pd.read_csv("data/cleaned/batters_normalized_cleaned.csv")
    pitchers = pd.read_csv("data/cleaned/pitchers_normalized_cleaned.csv")
    weather = pd.read_csv("data/weather_adjustments.csv")
    park_day = pd.read_csv("data/Data/park_factors_day.csv")
    park_night = pd.read_csv("data/Data/park_factors_night.csv")

    print(f"Batters: {len(batters)}, Pitchers: {len(pitchers)}, Weather: {len(weather)}")
    print("Applying adjustments to batters...")

    batters_adj = apply_adjustments(batters, weather, park_day, park_night)

    output_path = Path("data/batters_adjusted.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    batters_adj.to_csv(output_path, index=False)

    print("Done. Adjusted batter file written.")

if __name__ == "__main__":
    main()
