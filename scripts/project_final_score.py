#Project Final Score 7.30.25
import pandas as pd
from pathlib import Path
from projection_formulas import project_final_score

# File paths
HOME_FILE = Path("data/end_chain/final/batter_home_final.csv")
AWAY_FILE = Path("data/end_chain/final/batter_away_final.csv")
PARK_FACTORS = Path("data/weather_input.csv")
WEATHER_ADJUSTMENTS = Path("data/weather_adjustments.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    print("🔄 Loading batter and environment data...")
    home = pd.read_csv(HOME_FILE)
    away = pd.read_csv(AWAY_FILE)
    parks = pd.read_csv(PARK_FACTORS)
    weather = pd.read_csv(WEATHER_ADJUSTMENTS)

    print("🧠 Projecting final scores...")
    df = project_final_score(home, away, parks, weather)

    print("💾 Saving to:", OUTPUT_FILE)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print("✅ Done.")

if __name__ == "__main__":
    main()
