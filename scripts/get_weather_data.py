import pandas as pd
import requests
import time
import os

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
API_KEY = "45d9502513854b489c3162411251907"
BASE_URL = "http://api.weatherapi.com/v1/current.json"

def fetch_weather(lat, lon):
    try:
        params = {"key": API_KEY, "q": f"{lat},{lon}", "aqi": "no"}
        response = requests.get(BASE_URL, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Warning: Received status code {response.status_code} for lat {lat}, lon {lon}")
            return None
    except Exception as e:
        print(f"Error fetching weather for lat {lat}, lon {lon}: {e}")
        return None

def main():
    print("🔄 Reading input file...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"❌ Failed to read {INPUT_FILE}: {e}")
        return

    results = []
    print(f"🌍 Fetching weather for {len(df)} stadiums...")

    for i, row in df.iterrows():
        team = row["home_team"]
        lat = row["lat"]
        lon = row["lon"]
        roof = row["roof"]

        print(f"➡️ {i+1}/{len(df)}: Getting weather for {team} at ({lat}, {lon})")

        data = fetch_weather(lat, lon)
        if data:
            current = data.get("current", {})
            condition = current.get("condition", {}).get("text", "")
            temperature = current.get("temp_f", "")
            wind_speed = current.get("wind_mph", "")
            wind_direction = current.get("wind_dir", "")
            humidity = current.get("humidity", "")
        else:
            condition = ""
            temperature = ""
            wind_speed = ""
            wind_direction = ""
            humidity = ""

        roof_status = "Roof closed" if roof.lower() == "closed" else "Roof open"

        results.append({
            "home_team": team,
            "temperature": temperature,
            "wind_speed": wind_speed,
            "wind_direction": wind_direction,
            "humidity": humidity,
            "precipitation": 0.0,
            "condition": condition,
            "notes": roof_status
        })

        time.sleep(1.1)  # avoid rate limiting

    print("🧪 Creating DataFrame...")
    output_df = pd.DataFrame(results)

    try:
        print(f"💾 Writing to {OUTPUT_FILE}...")
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        output_df.to_csv(OUTPUT_FILE, index=False)
        print("✅ Weather data saved successfully.")
    except Exception as e:
        print(f"❌ Failed to write weather data: {e}")

if __name__ == "__main__":
    main()
