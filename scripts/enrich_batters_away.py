import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import sys

# Setup logging
log_dir = Path("summaries")
log_dir.mkdir(parents=True, exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_path = log_dir / f"enrich_batters_away_{timestamp}.log"

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)
logging.getLogger().setLevel(logging.INFO)

def main():
    logging.info("📥 Loading data...")
    batters_path = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
    weather_path = Path("data/weather_adjustments.csv")

    batters = pd.read_csv(batters_path)
    weather = pd.read_csv(weather_path)

    logging.info(f"✅ Loaded {len(batters)} batters and {len(weather)} weather rows")

    logging.info("📐 Normalizing columns...")
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

    logging.info("🔗 Merging...")
    batters = pd.merge(batters, weather, on=["away_team", "home_team"], how="left")

    out_path = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
    batters.to_csv(out_path, index=False)

    logging.info(f"✅ Enriched file saved to: {out_path}")

if __name__ == "__main__":
    main()
