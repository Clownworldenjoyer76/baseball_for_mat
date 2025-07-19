import pandas as pd
import requests
import time

INPUT_CSV = "data/weather_input.csv"
OUTPUT_CSV = "data/weather_adjustments.csv"
ERROR_LOG = "weather_error_log.txt"
API_KEY = "b55200ce76260b2adb442b2f17b896c0"
API_URL = "https://api.openweathermap.org/data/2.5/weather"

def fetch_weather(lat, lon, retries=5):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(API_URL, params={
                "lat": lat,
                "lon": lon,
                "appid": API_KEY,
                "units": "imperial"
            }, timeout=10)

            response.raise_for_status()
            return response.json()

        except Exception as e:
            print(f"⚠️ Attempt {attempt} failed for lat={lat}, lon={lon}: {e}")
            time.sleep(2 ** (attempt - 1))

    return None

def main():
    df = pd.read_csv(INPUT_CSV)
    results = []
    failed_rows = []

    print(f"✅ Loaded {len(df)} rows from {INPUT_CSV}")
    for i, row in df.iterrows():
        lat, lon = row["lat"], row["lon"]
        weather = fetch_weather(lat, lon)

        if weather is None:
            failed_rows.append((lat, lon))
            continue

        # Extract and assign weather data
        temp = weather.get("main", {}).get("temp")
        wind = weather.get("wind", {}).get("speed")
        humidity = weather.get("main", {}).get("humidity")
        row["temperature"] = temp
        row["wind_speed"] = wind
        row["humidity"] = humidity
        results.append(row)

    # Write successful results
    if results:
        pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False)
        print(f"📁 Weather data saved to {OUTPUT_CSV}")
        print(f"✅ Processed {len(results)} rows")

    # Log failures
    if failed_rows:
        with open(ERROR_LOG, "w") as f:
            for lat, lon in failed_rows:
                f.write(f"{lat},{lon}\n")
        print(f"❌ {len(failed_rows)} rows failed after 5 retries")
        raise RuntimeError("Weather scrape failed for some rows — check weather_error_log.txt")

if __name__ == "__main__":
    main()
