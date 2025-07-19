
import pandas as pd
import requests
import time

INPUT_CSV = "data/weather_input.csv"
OUTPUT_CSV = "data/weather_adjustments.csv"
LOG_FILE = "summaries/Activate3/get_weather_data.log"
API_KEY = "45d9502513854b489c3162411251907"
BASE_URL = "http://api.weatherapi.com/v1/current.json"

MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds

def fetch_weather(lat, lon):
    params = {
        "key": API_KEY,
        "q": f"{lat},{lon}"
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            with open(LOG_FILE, "a") as log:
                log.write(f"[Retry {attempt}] Error fetching weather for {lat}, {lon}: {e}\n")
            time.sleep(RETRY_DELAY)
    return None

def main():
    try:
        df = pd.read_csv(INPUT_CSV)
        results = []
        for _, row in df.iterrows():
            lat = row["latitude"]
            lon = row["longitude"]
            team = row["home_team"]
            weather_data = fetch_weather(lat, lon)
            if weather_data:
                current = weather_data.get("current", {})
                results.append({
                    "home_team": team,
                    "temp_f": current.get("temp_f"),
                    "humidity": current.get("humidity"),
                    "wind_mph": current.get("wind_mph"),
                    "wind_dir": current.get("wind_dir")
                })
            else:
                results.append({
                    "home_team": team,
                    "temp_f": None,
                    "humidity": None,
                    "wind_mph": None,
                    "wind_dir": None
                })

        pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
        with open(LOG_FILE, "a") as log:
            log.write("✅ Weather data collection complete.\n")
    except Exception as e:
        with open(LOG_FILE, "a") as log:
            log.write(f"❌ Script failed: {e}\n")

if __name__ == "__main__":
    main()
