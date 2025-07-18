import pandas as pd
import requests
import time
from pathlib import Path

API_KEY = "b55200ce76260b2adb442b2f17b896c0"
INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"

def build_url(lat, lon):
    return f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial"

def fetch_weather(lat, lon):
    url = build_url(lat, lon)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def extract_data(row, data):
    return {
        "home_team": row["home_team"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "wind_direction": data["wind"].get("deg", 0)
    }

def main():
    df = pd.read_csv(INPUT_FILE)
    print(f"✅ Loaded {len(df)} rows from {INPUT_FILE}")

    results = []
    retry_queue = []

    for _, row in df.iterrows():
        lat = row["lat"]
        lon = row["lon"]

        try:
            data = fetch_weather(lat, lon)
            results.append(extract_data(row, data))
        except Exception as e:
            print(f"❌ API error for lat={lat}, lon={lon}: {e}")
            retry_queue.append(row)

    if retry_queue:
        print(f"🔁 Retrying {len(retry_queue)} failed locations...")
        time.sleep(5)

        for row in retry_queue:
            lat = row["lat"]
            lon = row["lon"]
            try:
                data = fetch_weather(lat, lon)
                results.append(extract_data(row, data))
            except Exception as e:
                print(f"🔥 Final failure for lat={lat}, lon={lon}: {e}")

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Weather data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
