import csv
import requests
import pandas as pd
from datetime import datetime
import pytz

API_KEY = "b55200ce76260b2adb442b2f17b896c0"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
TODAYS_PITCHERS_FILE = "data/daily/todays_pitchers.csv"
OUTPUT_FILE = "data/weather_adjustments.csv"

def get_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial"
    response = requests.get(url)
    return response.json()

def convert_to_localtime(game_time_str, tz_str):
    utc = pytz.utc
    local_tz = pytz.timezone(tz_str)
    game_time = datetime.strptime(game_time_str, "%H:%M")
    now = datetime.now().date()
    game_datetime_utc = utc.localize(datetime.combine(now, game_time.time()))
    return game_datetime_utc.astimezone(local_tz).strftime("%I:%M %p")

def generate_weather_adjustments():
    stadium_df = pd.read_csv(STADIUM_FILE)
    pitchers_df = pd.read_csv(TODAYS_PITCHERS_FILE)

    merged_df = pd.merge(
        pitchers_df[['home_team', 'game_time']],
        stadium_df,
        how='inner',
        left_on='home_team',
        right_on='home_team'
    )

    rows = []
    for _, row in merged_df.iterrows():
        stadium = row['venue']
        city = row['city']
        state = row['state']
        lat = row['latitude']
        lon = row['longitude']
        timezone = row['timezone']
        roof = str(row.get('is_dome', False))
        game_time = row['game_time']

        try:
            local_time = convert_to_localtime(game_time, timezone)
        except Exception as e:
            local_time = f"Time Error: {str(e)}"

        if roof.lower() in ['true', 'closed', 'dome']:
            weather = {
                'temperature': 'N/A',
                'wind_speed': 'N/A',
                'wind_direction': 'N/A',
                'humidity': 'N/A',
                'precipitation': 'N/A',
                'notes': 'Roof closed'
            }
        else:
            try:
                data = get_weather(lat, lon)
                weather = {
                    'temperature': data['main']['temp'],
                    'wind_speed': data['wind']['speed'],
                    'wind_direction': data['wind']['deg'],
                    'humidity': data['main']['humidity'],
                    'precipitation': data.get('rain', {}).get('1h', 0),
                    'notes': 'Roof open'
                }
            except Exception as e:
                weather = {
                    'temperature': 'Error',
                    'wind_speed': 'Error',
                    'wind_direction': 'Error',
                    'humidity': 'Error',
                    'precipitation': 'Error',
                    'notes': str(e)
                }

        rows.append({
            'stadium': stadium,
            'city': city,
            'state': state,
            'local_game_time': local_time,
            **weather
        })

    pd.DataFrame(rows).to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    generate_weather_adjustments()
