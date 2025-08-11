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

def _select_weather_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and normalize expected columns from weather_adjustments.csv.
    Handles schema variations:
      - 'game_time' may be stored as 'game_time_et'
      - add guards if some columns are missing
    """
    # map desired -> candidate options in order of preference
    desired_to_candidates = {
        "temperature": ["temperature"],
        "wind_speed": ["wind_speed"],
        "wind_direction": ["wind_direction"],
        "humidity": ["humidity"],
        "condition": ["condition"],
        "game_time": ["game_time", "game_time_et"],  # <- support both
        "home_team": ["home_team"],
        "away_team": ["away_team"],
    }

    rename_map = {}
    keep_cols = []
    missing = []

    for desired, candidates in desired_to_candidates.items():
        chosen = None
        for c in candidates:
            if c in df.columns:
                chosen = c
                break
        if chosen:
            keep_cols.append(chosen)
            if chosen != desired:
                rename_map[chosen] = desired
        else:
            missing.append(desired)

    if missing:
        logger.warning(f"Weather file missing expected columns: {missing}. Proceeding with available ones.")

    out = df[keep_cols].rename(columns=rename_map)

    # standardize dtypes/formatting
    if "temperature" in out.columns:
        out["temperature"] = pd.to_numeric(out["temperature"], errors="coerce").round(1)
    for col in ["home_team", "away_team"]:
        if col in out.columns:
            out[col] = out[col].astype(str)

    return out

def main():
    logger.info("📥 Loading data...")
    batters_path = Path("data/end_chain/cleaned/prep/bat_awp_cleaned.csv")
    weather_path = Path("data/weather_adjustments.csv")

    batters = pd.read_csv(batters_path)
    weather = pd.read_csv(weather_path)

    logger.info(f"✅ Loaded {len(batters)} batters and {len(weather)} weather rows")

    logger.info("📐 Normalizing columns...")
    # incoming batters has 'team' as away team
    batters = batters.rename(columns={"team": "away_team"})

    # ensure merge key dtypes align
    for col in ["home_team", "away_team"]:
        if col in batters.columns:
            batters[col] = batters[col].astype(str)

    # normalize weather columns & handle schema variations
    weather = _select_weather_columns(weather)

    # ensure merge key dtypes align
    for col in ["home_team", "away_team"]:
        if col in weather.columns:
            weather[col] = weather[col].astype(str)

    logger.info("🔗 Merging...")
    batters = pd.merge(batters, weather, on=["away_team", "home_team"], how="left")

    out_path = Path("data/end_chain/cleaned/bat_awp_cleaned.csv")
    batters.to_csv(out_path, index=False)

    logger.info(f"✅ Enriched file saved to: {out_path}")

if __name__ == "__main__":
    main()
