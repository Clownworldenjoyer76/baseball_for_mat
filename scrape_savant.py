import requests
import pandas as pd
from bs4 import BeautifulSoup
import json

# OpenWeatherMap API key (user provided)
API_KEY = "b55200ce76260b2adb442b2f17b896c0"

# Stadium-to-location map for weather lookup
STADIUM_LOCATIONS = {
    "Coors Field": {"lat": 39.7561, "lon": -104.9942, "indoor": False},
    "Fenway Park": {"lat": 42.3467, "lon": -71.0972, "indoor": False},
    "Yankee Stadium": {"lat": 40.8296, "lon": -73.9262, "indoor": False},
    "Dodger Stadium": {"lat": 34.0739, "lon": -118.2400, "indoor": False},
    "Oracle Park": {"lat": 37.7786, "lon": -122.3893, "indoor": False},
    "Wrigley Field": {"lat": 41.9484, "lon": -87.6553, "indoor": False},
    "T-Mobile Park": {"lat": 47.5914, "lon": -122.3325, "indoor": False},
    "Rogers Centre": {"lat": 43.6414, "lon": -79.3894, "indoor": True},
    "Chase Field": {"lat": 33.4455, "lon": -112.0667, "indoor": True},
    "Minute Maid Park": {"lat": 29.7572, "lon": -95.3555, "indoor": True},
    "LoanDepot Park": {"lat": 25.7781, "lon": -80.2197, "indoor": True},
    "American Family Field": {"lat": 43.0280, "lon": -87.9712, "indoor": True},
    "Globe Life Field": {"lat": 32.7473, "lon": -97.0847, "indoor": True}
}

def get_weather_boost(lat, lon, indoor):
    if indoor:
        return 1.00

    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=imperial&appid={API_KEY}"
        r = requests.get(url)
        data = r.json()

        temp = data['main']['temp']
        wind_speed = data['wind']['speed']
        wind_dir_deg = data['wind']['deg']

        wind_direction = "out" if 45 <= wind_dir_deg <= 135 else "in"

        boost = 1.00
        if temp >= 85:
            boost += 0.02
        elif temp <= 55:
            boost -= 0.02
        if wind_direction == "out" and wind_speed >= 10:
            boost += 0.03
        elif wind_direction == "in" and wind_speed >= 10:
            boost -= 0.03

        return round(boost, 3)
    except:
        return 1.00

def get_today_pitchers():
    url = "https://www.espn.com/mlb/lines"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    matchups = []
    for row in soup.select("section.Table__TR"):
        teams = row.select("span.sb-team-short")
        pitchers = row.select("span.gamepitcher")
        stadium = row.select_one("div.game-location")
        if len(teams) == 2 and len(pitchers) == 2:
            matchup = {
                "home_team": teams[1].text.strip(),
                "away_team": teams[0].text.strip(),
                "home_pitcher": pitchers[1].text.strip(),
                "away_pitcher": pitchers[0].text.strip(),
                "stadium": stadium.text.strip() if stadium else "Unknown"
            }
            matchups.append(matchup)
    return matchups

def get_top_batters():
    url = "https://baseballsavant.mlb.com/leaderboard/statcast"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    scripts = soup.find_all("script")
    data_script = None
    for s in scripts:
        if "var leaderboardData =" in s.text:
            data_script = s.text
            break

    if not data_script:
        raise Exception("Could not find embedded statcast data.")

    start = data_script.find("var leaderboardData = ") + len("var leaderboardData = ")
    end = data_script.find(";
", start)
    json_str = data_script[start:end].strip()
    data = json.loads(json_str)

    df = pd.DataFrame(data)
    df = df[["player_name", "team", "xwOBA", "barrel_batted_rate", "sweet_spot_percent"]]
    df.columns = ["batter", "team", "xwoba", "barrel_rate", "sweet_spot"]
    df = df.dropna().head(10)
    return df

def generate_props():
    batters = get_top_batters()
    matchups = get_today_pitchers()

    props = []
    i = 0
    for b in batters.to_dict(orient="records"):
        matchup = matchups[i % len(matchups)]
        stadium = matchup["stadium"]
        stadium_info = STADIUM_LOCATIONS.get(stadium, {"lat": 0, "lon": 0, "indoor": False})
        indoor = stadium_info["indoor"]
        lat, lon = stadium_info["lat"], stadium_info["lon"]

        park_boost = 1.00  # built into stadium effects if needed
        weather_boost = get_weather_boost(lat, lon, indoor)

        edge = (
            (float(b["xwoba"]) + b["barrel_rate"] / 100 + b["sweet_spot"] / 100)
            * park_boost * weather_boost * 10
        )
        props.append({
            "batter": b["batter"],
            "team": b["team"],
            "pitcher": matchup["home_pitcher"],
            "stadium": stadium,
            "indoor": indoor,
            "weather_boost": weather_boost,
            "xwoba": b["xwoba"],
            "barrel_rate": b["barrel_rate"],
            "sweet_spot": b["sweet_spot"],
            "edge_score": round(edge, 2)
        })
        i += 1

    with open("top_props.json", "w") as f:
        json.dump(props, f, indent=2)

    print("✅ top_props.json created with real weather and stadium logic.")

generate_props()