import pandas as pd
import requests
import time

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
LOG_FILE = "summaries/Activate3/get_weather_data.log"

API_KEY = "45d9502513854b489c3162411251907"
BASE_URL = "http://api.weatherapi.com/v1/current.json"

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
    with open(LOG_FILE, "w") as log:
        log.write("🔄 Reading input file...\n")
        try:
            df = pd.read_csv(INPUT_FILE)
        except Exception as e:
            log.write(f"❌ Failed to read input file: {e}\n")
            return

        log.write(f"🌍 Fetching weather for {len(df)} stadiums...\n")

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
                log.write(f"❌ Failed to fetch weather for {location} after 5 attempts.\n")
                continue

            current = data.get("current", {})
            condition = current.get("condition", {}).get("text", "Unknown")

            results.append({
                "stadium": venue,
                "location": location,
                "temperature": current.get("temp_f", ""),
                "wind_speed": current.get("wind_mph", ""),
                "wind_direction": current.get("wind_dir", ""),
                "humidity": current.get("humidity", ""),
                "precipitation": current.get("precip_in", 0.0),
                "condition": condition,
                "notes": "Roof closed" if is_dome.lower() == "yes" else "Roof open",
                "game_time": game_time
            })

        if not results:
            log.write("⚠️ No weather data collected. Exiting.\n")
            return

        out_df = pd.DataFrame(results)
        try:
            out_df.to_csv(OUTPUT_FILE, index=False)
            log.write(f"✅ Weather data written to {OUTPUT_FILE}\n")
        except Exception as e:
            log.write(f"❌ Failed to write output: {e}\n")

if __name__ == "__main__":
    main()
