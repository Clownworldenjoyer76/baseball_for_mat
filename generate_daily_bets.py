import requests
import pandas as pd
import json
from datetime import datetime

# === CONFIG ===
PARK_FACTORS_PATH = "data/Data/park_factors_full_verified.csv"
OUTPUT_PATH = "daily_bets.json"
TODAY = datetime.today().strftime("%Y-%m-%d")
MLB_API_URL = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={TODAY}"
API_KEY = "b55200ce76260b2adb442b2f17b896c0"  # OpenWeatherMap

# === STADIUM METADATA ===
STADIUMS = {
    "Angel Stadium": {"lat": 33.8003, "lon": -117.8827, "indoor": False},
    "Daikin Park": {"lat": 29.7572, "lon": -95.3553, "indoor": True},
    "Rogers Centre": {"lat": 43.6414, "lon": -79.3894, "indoor": True},
    "Truist Park": {"lat": 33.8908, "lon": -84.4678, "indoor": False},
    "American Family Field": {"lat": 43.0280, "lon": -87.9712, "indoor": True},
    "Busch Stadium": {"lat": 38.6226, "lon": -90.1928, "indoor": False},
    "Wrigley Field": {"lat": 41.9484, "lon": -87.6553, "indoor": False},
    "Chase Field": {"lat": 33.4455, "lon": -112.0667, "indoor": True},
    "Dodger Stadium": {"lat": 34.0739, "lon": -118.2400, "indoor": False},
    "Oracle Park": {"lat": 37.7786, "lon": -122.3893, "indoor": False},
    "Nationals Park": {"lat": 38.8730, "lon": -77.0074, "indoor": False},
    "loanDepot park": {"lat": 25.7781, "lon": -80.2195, "indoor": True},
    "Kauffman Stadium": {"lat": 39.0517, "lon": -94.4803, "indoor": False},
    "Great American Ball Park": {"lat": 39.0975, "lon": -84.5071, "indoor": False},
    "Globe Life Field": {"lat": 32.7513, "lon": -97.0821, "indoor": True},
    "Fenway Park": {"lat": 42.3467, "lon": -71.0972, "indoor": False},
    "Coors Field": {"lat": 39.7561, "lon": -104.9942, "indoor": False},
    "Comerica Park": {"lat": 42.3390, "lon": -83.0485, "indoor": False},
    "Citizens Bank Park": {"lat": 39.9061, "lon": -75.1665, "indoor": False},
    "Citi Field": {"lat": 40.7571, "lon": -73.8458, "indoor": False},
    "Target Field": {"lat": 44.9817, "lon": -93.2783, "indoor": False},
    "PNC Park": {"lat": 40.4469, "lon": -80.0057, "indoor": False},
    "Oriole Park at Camden Yards": {"lat": 39.2839, "lon": -76.6217, "indoor": False},
    "Petco Park": {"lat": 32.7073, "lon": -117.1573, "indoor": False},
    "Progressive Field": {"lat": 41.4962, "lon": -81.6852, "indoor": False},
    "Rate Field": {"lat": 41.8309, "lon": -87.6339, "indoor": False},
    "T-Mobile Park": {"lat": 47.5914, "lon": -122.3325, "indoor": True},
    "Tropicana Field": {"lat": 27.7683, "lon": -82.6534, "indoor": True},
    "Yankee Stadium": {"lat": 40.8296, "lon": -73.9262, "indoor": False}
}

# === LOAD PARK FACTORS ===
park_factors = pd.read_csv(PARK_FACTORS_PATH)

def get_park_factors(home_team):
    row = park_factors[park_factors["Team"].str.lower() == home_team.lower()]
    return row.iloc[0].to_dict() if not row.empty else {}

def get_weather(venue):
    if venue not in STADIUMS:
        return {"note": "Weather unavailable — venue not recognized"}

    meta = STADIUMS[venue]
    if meta["indoor"]:
        return {"note": "Indoor stadium — weather not applicable"}

    lat, lon = meta["lat"], meta["lon"]
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial"
    try:
        resp = requests.get(url)
        w = resp.json()
        return {
            "temp_f": w["main"]["temp"],
            "wind_speed_mph": w["wind"]["speed"],
            "wind_dir_deg": w["wind"]["deg"],
            "condition": w["weather"][0]["description"],
            "note": "Outdoor game — weather may affect ball flight"
        }
    except:
        return {"note": "Weather fetch failed"}

# === FETCH TODAY'S GAMES ===
resp = requests.get(MLB_API_URL)
data = resp.json()

games_out = []
for date_data in data.get("dates", []):
    for game in date_data.get("games", []):
        home_team = game["teams"]["home"]["team"]["name"]
        away_team = game["teams"]["away"]["team"]["name"]
        matchup = f"{home_team} vs {away_team}"

        park_data = get_park_factors(home_team)
        venue = park_data.get("Venue", "")
        weather = get_weather(venue)

        games_out.append({
            "matchup": matchup,
            "parlay": {
                "moneyline_or_spread": None,
                "total": None,
                "player_props": []
            },
            "park_factors": {
                "venue": venue,
                "HR": park_data.get("HR"),
                "H": park_data.get("H"),
                "HardHit": park_data.get("HardHit"),
                "OBP": park_data.get("OBP"),
                "wOBACon": park_data.get("wOBACon"),
                "xwOBACon": park_data.get("xwOBACon")
            },
            "weather": weather
        })

# === OUTPUT ===
out = {
    "date": TODAY,
    "games": games_out,
    "top_hr_hitters": [],
    "top_hit_props": []
}

with open(OUTPUT_PATH, "w") as f:
    json.dump(out, f, indent=2)

print(f"Generated {OUTPUT_PATH} with {len(games_out)} games.")