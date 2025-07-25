import pandas as pd
import requests
import time
from datetime import datetime

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
VENUE_FILE = "data/Data/stadium_metadata.csv" # This variable is still here but no longer used
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

    print(f"{timestamp()} 🌍 Fetching weather for {len(df)} venues...")

    results = []

    for _, row in df.iterrows():
        # Find venue in the venue column of data/weather_input.csv
        venue_name = row.get("venue", "")
        # Find away_team value in away_team column of data/weather_input.csv
        away_team_val = row.get("away_team", "UNKNOWN")
        # Find home_team value in team_name_x column of data/weather_input.csv
        home_team_val = row.get("team_name_x", "UNKNOWN")

        city = row.get("city", "")
        location = f"{venue_name}, {city}"
        lat = row.get("latitude", "")
        lon = row.get("longitude", "")
        is_dome = row.get("is_dome", False)
        game_time = row.get("game_time", "")

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
            "venue": venue_name,
            "location": location,
            "temperature": current.get("temp_f", ""),
            "wind_speed": current.get("wind_mph", ""),
            "wind_direction": current.get("wind_dir", ""),
            "humidity": current.get("humidity", ""),
            "precipitation": current.get("precip_in", 0.0),
            "condition": condition,
            "notes": "Roof closed" if is_dome is True else "Roof open",
            "game_time": game_time,
            "home_team": home_team_val,
            "away_team": away_team_val
        })

    if not results:
        print(f"{timestamp()} ⚠️ No weather data collected. Exiting.")
        return

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"{timestamp()} ✅ Weather data written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
