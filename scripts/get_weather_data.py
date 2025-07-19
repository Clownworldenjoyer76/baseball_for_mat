import pandas as pd
import requests
from pathlib import Path
import subprocess

API_KEY = "b55200ce76260b2adb442b2f17b896c0"
INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"

def fetch_weather(lat, lon):
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        return {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "wind_speed": data["wind"]["speed"],
            "wind_direction": data["wind"]["deg"],
            "condition": data["weather"][0]["main"]
        }
    except Exception as e:
        print(f"❌ API error for lat={lat}, lon={lon}: {e}")
        return {
            "temperature": None,
            "humidity": None,
            "wind_speed": None,
            "wind_direction": None,
            "condition": None
        }

def main():
    df = pd.read_csv(INPUT_FILE)

    # 🔧 Rename columns to match expected format
    df = df.rename(columns={
        "latitude": "lat",
        "longitude": "lon"
    })

    print(f"✅ Loaded {len(df)} rows from {INPUT_FILE}")

    weather_data = []
    for _, row in df.iterrows():
        lat = row["lat"]
        lon = row["lon"]
        weather = fetch_weather(lat, lon)
        row_data = row.to_dict()
        row_data.update(weather)
        weather_data.append(row_data)

    out_df = pd.DataFrame(weather_data)
    out_df.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Processed {len(out_df)} rows")
    print(f"📁 Weather data saved to {OUTPUT_FILE}")

    # Git commit
    try:
        subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", OUTPUT_FILE], check=True)
        subprocess.run(["git", "commit", "-m", "Auto-commit: updated weather data"], check=True)
        subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit failed: {e}")

if __name__ == "__main__":
    main()
