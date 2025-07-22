import pandas as pd
import requests
import time
from datetime import datetime

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
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
        venue = row["venue"]
        city = row["city"]
        location = f"{venue}, {city}"
        lat = row["latitude"]
        lon = row["longitude"]
        is_dome = row["is_dome"]
        game_time = row["game_time"]

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
            "stadium": row["home_team"],
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
    try:
        out_df.to_csv(OUTPUT_FILE, index=False)
        print(f"{timestamp()} ✅ Weather data written to {OUTPUT_FILE}")
    except Exception as e:
        print(f"{timestamp()} ❌ Failed to write output: {e}")

if __name__ == "__main__":
    main()
