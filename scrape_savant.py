import requests
import pandas as pd
from bs4 import BeautifulSoup
import json

PARK_FACTORS = {
    "Coors Field": 1.08,
    "Fenway Park": 1.04,
    "Yankee Stadium": 1.03,
    "Dodger Stadium": 1.00,
    "Tropicana Field": 0.95,
    "Oracle Park": 0.92,
    "Guaranteed Rate Field": 1.01,
    "Wrigley Field": 1.02,
    "T-Mobile Park": 0.96
}

# Weather modifier based on wind speed and direction
def get_weather_boost(temp, wind_speed, wind_direction):
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
        park_boost = PARK_FACTORS.get(stadium, 1.00)

        # Simulated weather data
        temp = 87 if "Coors" in stadium else 72
        wind_speed = 12 if "Wrigley" in stadium else 5
        wind_direction = "out" if "Coors" in stadium else "neutral"

        weather_boost = get_weather_boost(temp, wind_speed, wind_direction)

        edge = (
            (float(b["xwoba"]) + b["barrel_rate"] / 100 + b["sweet_spot"] / 100)
            * park_boost * weather_boost * 10
        )
        props.append({
            "batter": b["batter"],
            "team": b["team"],
            "pitcher": matchup["home_pitcher"],
            "stadium": stadium,
            "park_boost": park_boost,
            "weather_boost": weather_boost,
            "xwoba": b["xwoba"],
            "barrel_rate": b["barrel_rate"],
            "sweet_spot": b["sweet_spot"],
            "edge_score": round(edge, 2)
        })
        i += 1

    with open("top_props.json", "w") as f:
        json.dump(props, f, indent=2)

    print("✅ top_props.json created with park and weather-adjusted edge scores.")

generate_props()