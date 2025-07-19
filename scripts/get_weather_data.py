import pandas as pd
import requests
import time

API_KEY = "45d9502513854b489c3162411251907"
INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
MAX_RETRIES = 5

def fetch_weather(lat, lon):
    url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={lat},{lon}"
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        time.sleep(2)
    return None

def main():
    df = pd.read_csv(INPUT_FILE)
    results = []

    for _, row in df.iterrows():
        lat = row["latitude"]
        lon = row["longitude"]
        is_dome = row["is_dome"]
        venue = row["venue"]
        home_team = row["home_team"]
        away_team = row["away_team"]

        weather_data = fetch_weather(lat, lon)

        if weather_data:
            current = weather_data.get("current", {})
            result = {
                "home_team": home_team,
                "away_team": away_team,
                "venue": venue,
                "temperature": current.get("temp_f", ""),
                "wind_speed": current.get("wind_mph", ""),
                "wind_direction": current.get("wind_dir", ""),
                "humidity": current.get("humidity", ""),
                "precipitation": 0.0,
                "notes": "Roof closed" if is_dome else "Roof open"
            }
        else:
            result = {
                "home_team": home_team,
                "away_team": away_team,
                "venue": venue,
                "temperature": "",
                "wind_speed": "",
                "wind_direction": "",
                "humidity": "",
                "precipitation": 0.0,
                "notes": "Roof closed" if is_dome else "Roof open"
            }

        results.append(result)

    output_df = pd.DataFrame(results)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    output_df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
