import pandas as pd
from pathlib import Path

# File paths
BATT_FILE = Path("data/end_chain/cleaned/batters_away_cleaned.csv")
GAMES_FILE = Path("data/end_chain/todaysgames_normalized.csv")
WEATHER_ADJUSTMENTS_FILE = Path("data/weather_adjustments.csv")
WEATHER_INPUT_FILE = Path("data/weather_input.csv")

def load_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

def main():
    print("🔄 Loading input files...")
    try:
        batters = load_csv(BATT_FILE)
        games = load_csv(GAMES_FILE)
        weather_adjustments = load_csv(WEATHER_ADJUSTMENTS_FILE)
        weather_input = load_csv(WEATHER_INPUT_FILE)
        print("✅ Files loaded successfully.")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return
    except Exception as e:
        print(f"❌ An unexpected error occurred during file loading: {e}")
        return

    print("Merging home_team from games data...")
    games['away_team'] = games['away_team'].str.strip().str.upper()
    games['home_team'] = games['home_team'].str.strip().str.upper()
    batters['away_team'] = batters['away_team'].str.strip().str.upper()

    games_subset = games[["away_team", "home_team"]].drop_duplicates()
    batters = pd.merge(batters, games_subset, on="away_team", how="left")
    print("✅ home_team merged.")

    print("Merging weather adjustment data...")
    weather_adjustments['away_team'] = weather_adjustments['away_team'].str.strip().str.upper()
    weather_adjustments['home_team'] = weather_adjustments['home_team'].str.strip().str.upper()

    weather_adj_subset = weather_adjustments[[
        "away_team",
        "home_team",
        "temperature",
        "humidity",
        "wind_speed",
        "wind_direction",
        "precipitation",
        "condition",
        "notes",
        "game_time"
    ]].drop_duplicates()

    batters = pd.merge(
        batters,
        weather_adj_subset,
        on=["away_team", "home_team"],
        how="left"
    )
    print("✅ Weather adjustment data merged.")

    print("Merging Park Factor and time_of_day from weather_input data...")
    weather_input['away_team'] = weather_input['away_team'].str.strip().str.upper()
    weather_input['home_team'] = weather_input['home_team'].str.strip().str.upper()

    weather_input_subset = weather_input[[
        "home_team",
        "away_team",
        "Park Factor",
        "game_time",
        "notes",
        "time_of_day"
    ]].drop_duplicates()

    batters = pd.merge(
        batters,
        weather_input_subset,
        on=["away_team", "home_team"],
        how="left",
        suffixes=('', '_drop')
    )

    batters.drop(columns=[col for col in batters.columns if col.endswith('_drop') and col not in ['Park Factor', 'time_of_day']], errors='ignore', inplace=True)

    expected_cols = ["away_team", "home_team", "temperature", "humidity", "wind_speed", "wind_direction",
                     "precipitation", "condition", "notes", "game_time", "Park Factor", "time_of_day"]
    missing_cols = [col for col in expected_cols if col not in batters.columns]
    if missing_cols:
        print(f"⚠️ Warning: Missing columns after merge: {', '.join(missing_cols)}")

    batters.to_csv(BATT_FILE, index=False)
    print(f"✅ Updated {BATT_FILE.name}")

if __name__ == "__main__":
    main()
