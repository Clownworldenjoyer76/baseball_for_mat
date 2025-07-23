import pandas as pd
import requests
import time
from datetime import datetime

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
API_KEY = "45d9502513854b489c3162411251907"
BASE_URL = "http://api.weatherapi.com/v1/current.json"

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %I:%M:%S %p %Z")

def fetch_weather(lat, lon):
    url = f"{BASE_URL}?key={API_KEY}&q={lat},{lon}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception:
        return None
    return None

def main():
    print(f"{timestamp()} 🔄 Reading input file...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"{timestamp()} ❌ Failed to read input file: {e}")
        return

    print(f"{timestamp()} 🌍 Fetching weather for {len(df)} stadiums...")

    results = []

    for _, row in df.iterrows():
        venue = row.get("venue", "")
        city = row.get("city", "")
        location = f"{venue}, {city}"
        lat = row.get("latitude", "")
        lon = row.get("longitude", "")
        is_dome = row.get("is_dome", False)
        game_time = row.get("game_time", "")
        team_name = row.get("team_name", "UNKNOWN")  # this is your 'stadium' value

        attempts = 0
        data = None
        while attempts < 5 and data is None:
            data = fetch_weather(lat, lon)
            if data is None:
                attempts += 1
                time.sleep(1)

        if data is None:
            print(f"{timestamp()} ❌ Failed to fetch weather for {location} after 5 attempts.")
            continue

        current = data.get("current", {})
        condition = current.get("condition", {}).get("text", "Unknown")

        results.append({
            "stadium": team_name,
            "location": location,
            "temperature": current.get("temp_f", ""),
            "wind_speed": current.get("wind_mph", ""),
            "wind_direction": current.get("wind_dir", ""),
            "humidity": current.get("humidity", ""),
            "precipitation": current.get("precip_in", 0.0),
            "condition": condition,
            "notes": "Roof closed" if is_dome is True else "Roof open",
            "game_time": game_time
        })

    if not results:
        print(f"{timestamp()} ⚠️ No weather data collected. Exiting.")
        return

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"{timestamp()} ✅ Weather data written to {OUTPUT_FILE}")

    # === FORCE INSERT HOME_TEAM AND AWAY_TEAM ===
    try:
        weather_df = pd.read_csv(OUTPUT_FILE)
        stadium_df = pd.read_csv(STADIUM_FILE)

        # Strip whitespace and normalize
        weather_df["stadium"] = weather_df["stadium"].astype(str).str.strip()
        stadium_df["team_name"] = stadium_df["team_name"].astype(str).str.strip()

        # Merge to get away_team using stadium → team_name match
        merged = weather_df.merge(
            stadium_df[["team_name", "away_team"]],
            left_on="stadium",
            right_on="team_name",
            how="left"
        )

        merged["home_team"] = merged["stadium"]
        merged["away_team"] = merged["away_team"].fillna("UNKNOWN")

        merged.drop(columns=["team_name"], inplace=True)
        merged.to_csv(OUTPUT_FILE, index=False)

        print(f"{timestamp()} ✅ Forced insert of home_team and away_team into {OUTPUT_FILE}")

    except Exception as e:
        print(f"{timestamp()} ❌ Final write failed: {e}")

if __name__ == "__main__":
    main()
