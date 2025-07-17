
import pandas as pd
import requests
from datetime import datetime
import pytz
import os
import sys

API_KEY = "b55200ce76260b2adb442b2f17b896c0"
INPUT_FILE = "data/weather_input.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"
LOG_FILE = "data/weather_api_log.txt"

def get_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ API error for lat={lat}, lon={lon}: {e}")
        with open(LOG_FILE, "a") as log:
            log.write(f"API ERROR: lat={lat}, lon={lon}, error={e}\n")
        return None

def convert_to_localtime(game_time_str, tz_str):
    eastern = pytz.timezone("US/Eastern")
    local_tz = pytz.timezone(tz_str)
    game_time = datetime.strptime(game_time_str, "%I:%M %p")
    now = datetime.now().date()
    game_datetime_eastern = eastern.localize(datetime.combine(now, game_time.time()))
    return game_datetime_eastern.astimezone(local_tz).strftime("%-I:%M %p")

def generate_weather_adjustments():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        sys.exit(1)

    df = pd.read_csv(INPUT_FILE)
    if df.empty:
        print("❌ Input file is empty.")
        sys.exit(1)

    print(f"🔍 Loaded {len(df)} rows from {INPUT_FILE}")

    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    rows = []
    for _, row in df.iterrows():
        try:
            home_team = row['home_team']
            game_time = row['game_time']
            venue = row['venue']
            city = row['city']
            state = row['state']
            tz = row['timezone']
            is_dome = str(row['is_dome']).lower()
            lat = row['latitude']
            lon = row['longitude']

            try:
                local_time = convert_to_localtime(game_time, tz)
            except Exception as e:
                local_time = f"Time Error: {str(e)}"

            if is_dome in ['true', 'closed', 'dome']:
                weather = {
                    'temperature': 'N/A',
                    'wind_speed': 'N/A',
                    'wind_direction': 'N/A',
                    'humidity': 'N/A',
                    'precipitation': 'N/A',
                    'notes': 'Roof closed'
                }
            else:
                data = get_weather(lat, lon)
                if data:
                    weather = {
                        'temperature': data.get('main', {}).get('temp', 'Missing'),
                        'wind_speed': data.get('wind', {}).get('speed', 'Missing'),
                        'wind_direction': data.get('wind', {}).get('deg', 'Missing'),
                        'humidity': data.get('main', {}).get('humidity', 'Missing'),
                        'precipitation': data.get('rain', {}).get('1h', 0),
                        'notes': 'Roof open'
                    }
                else:
                    weather = {
                        'temperature': 'Error',
                        'wind_speed': 'Error',
                        'wind_direction': 'Error',
                        'humidity': 'Error',
                        'precipitation': 'Error',
                        'notes': 'API Failed'
                    }

            rows.append({
                'home_team': home_team,
                'stadium': venue,
                'city': city,
                'state': state,
                'local_game_time': local_time,
                **weather
            })

        except Exception as err:
            print(f"⚠️ Row skipped due to error: {err}")

    print(f"✅ Processed {len(rows)} rows")
    pd.DataFrame(rows).to_csv(OUTPUT_FILE, index=False)
    print(f"📁 Weather data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_weather_adjustments()
