import pandas as pd
import requests
import time
from datetime import datetime

API_KEY = "45d9502513854b489c3162411251907"
INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"

DEFAULT_PRECIPITATION = 0.0

def fetch_weather(lat, lon):
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={lat},{lon}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def get_roof_note(roof_type):
    if pd.isna(roof_type):
        return ""
    if roof_type.strip().lower() == "open":
        return "Roof open"
    elif roof_type.strip().lower() == "closed":
        return "Roof closed"
    return f"Roof {roof_type}"

def main():
    print("🔄 Reading input file...")
    df = pd.read_csv(INPUT_FILE)
    results = []

    print(f"🌍 Fetching weather for {len(df)} stadiums...")
    for _, row in df.iterrows():
        home_team = row["home_team"]
        location = row["location"]
        game_time = row["game_time"]
        latitude = row["latitude"]
        longitude = row["longitude"]
        roof_type = row["roof"]
        
        retries = 5
        weather = None
        for _ in range(retries):
            weather = fetch_weather(latitude, longitude)
            if weather:
                break
            time.sleep(2)
        
        if not weather:
            raise Exception(f"❌ Failed to fetch weather for {home_team} after {retries} attempts.")

        current = weather["current"]

        results.append({
            "home_team": home_team,
            "location": location,
            "game_time": game_time,
            "temperature": current["temp_f"],
            "humidity": current["humidity"],
            "wind_speed": current["wind_mph"],
            "wind_direction": current["wind_dir"],
            "precipitation": current.get("precip_in", DEFAULT_PRECIPITATION),
            "notes": get_roof_note(roof_type)
        })

    output_df = pd.DataFrame(results)
    output_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Weather data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
