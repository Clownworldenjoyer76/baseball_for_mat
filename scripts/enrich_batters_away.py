import pandas as pd
from pathlib import Path
import logging
import sys

# Setup logging (console only)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("📥 Loading data...")
    batters_path = Path("data/end_chain/cleaned/prep/bat_awp_cleaned.csv")
    weather_path = Path("data/weather_adjustments.csv")

    batters = pd.read_csv(batters_path)
    weather = pd.read_csv(weather_path)

    logger.info(f"✅ Loaded {len(batters)} batters and {len(weather)} weather rows")

    logger.info("📐 Normalizing columns...")
    batters = batters.rename(columns={"team": "away_team"})

    weather_cols = [
        "temperature", "wind_speed", "wind_direction", "humidity",
        "condition", "game_time", "home_team", "away_team"
    ]
    weather = weather[weather_cols]
    weather["temperature"] = weather["temperature"].round(1)

    # 🔧 Force correct data types for merge
    batters["home_team"] = batters["home_team"].astype(str)
    batters["away_team"] = batters["away_team"].astype(str)
    weather["home_team"] = weather["home_team"].astype(str)
    weather["away_team"] = weather["away_team"].astype(str)

    logger.info("🔗 Merging...")
    batters = pd.merge(batters, weather, on=["away_team", "home_team"], how="left")

    out_path = Path("data/end_chain/cleaned/bat_awp_cleaned.csv")
    batters.to_csv(out_path, index=False)

    logger.info(f"✅ Enriched file saved to: {out_path}")

if __name__ == "__main__":
    main()
