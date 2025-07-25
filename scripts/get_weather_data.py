import pandas as pd
import requests
import time
from datetime import datetime

INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
VENUE_FILE = "data/Data/stadium_metadata.csv" # Changed STADIUM_FILE to VENUE_FILE
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

    print(f"{timestamp()} 🌍 Fetching weather for {len(df)} venues...") # Changed 'stadiums' to 'venues'

    results = []

    for _, row in df.iterrows():
        venue_name = row.get("venue", "") # Renamed 'venue' to 'venue_name' to avoid conflict
        city = row.get("city", "")
        location = f"{venue_name}, {city}"
        lat = row.get("latitude", "")
        lon = row.get("longitude", "")
        is_dome = row.get("is_dome", False)
        game_time = row.get("game_time", "")
        team_name = row.get("team_name", "UNKNOWN")  # this is your 'venue' value

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
            "venue": team_name, # Changed "stadium" to "venue"
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
        venue_df = pd.read_csv(VENUE_FILE) # Changed stadium_df to venue_df

        # Strip whitespace and normalize
        weather_df["venue"] = weather_df["venue"].astype(str).str.strip() # Changed "stadium" to "venue"
        venue_df["team_name"] = venue_df["team_name"].astype(str).str.strip() # Changed stadium_df to venue_df

        # Merge to get away_team using venue → team_name match
        merged = weather_df.merge(
            venue_df[["team_name", "away_team"]], # Changed stadium_df to venue_df
            left_on="venue", # Changed "stadium" to "venue"
            right_on="team_name",
            how="left"
        )

        merged["home_team"] = merged["venue"] # Changed "stadium" to "venue"
        merged["away_team"] = merged["away_team"].fillna("UNKNOWN")

        merged.drop(columns=["team_name"], inplace=True)
        merged.to_csv(OUTPUT_FILE, index=False)

        print(f"{timestamp()} ✅ Forced insert of home_team and away_team into {OUTPUT_FILE}")

    except Exception as e:
        print(f"{timestamp()} ❌ Final write failed: {e}")

if __name__ == "__main__":
    main()
