import requests
import pandas as pd
from bs4 import BeautifulSoup
import json

# STEP 1: Get today's games and pitchers
def get_today_pitchers():
    url = "https://www.espn.com/mlb/lines"  # ESPN shows daily matchups and probables
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    matchups = []

    for row in soup.select("section.Table__TR"):
        teams = row.select("span.sb-team-short")
        pitchers = row.select("span.gamepitcher")
        if len(teams) == 2 and len(pitchers) == 2:
            matchup = {
                "home_team": teams[1].text.strip(),
                "away_team": teams[0].text.strip(),
                "home_pitcher": pitchers[1].text.strip(),
                "away_pitcher": pitchers[0].text.strip()
            }
            matchups.append(matchup)
    return matchups

# STEP 2: Scrape Baseball Savant hitter leaderboard
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

# STEP 3: Add fake park and weather boosts (to be replaced by real values)
def apply_environmental_adjustments(df):
    df["park_boost"] = 1.02  # Simulate Coors or hitter-friendly park
    df["weather_boost"] = 1.03  # Simulate good hitting weather
    df["edge_score"] = (
        (df["xwoba"].astype(float) + df["barrel_rate"] / 100 + df["sweet_spot"] / 100)
        * df["park_boost"] * df["weather_boost"] * 10
    ).round(2)
    return df

# STEP 4: Build final JSON
def generate_props():
    batters = get_top_batters()
    batters = apply_environmental_adjustments(batters)

    # Assign placeholder pitchers for now
    matchups = get_today_pitchers()
    pitchers = [m["home_pitcher"] for m in matchups] + [m["away_pitcher"] for m in matchups]
    batters["pitcher"] = (pitchers * 10)[:len(batters)]

    props = batters[["batter", "team", "pitcher", "xwoba", "barrel_rate", "sweet_spot", "edge_score"]].to_dict(orient="records")
    with open("top_props.json", "w") as f:
        json.dump(props, f, indent=2)

    print("✅ top_props.json created with real batter data and simulated matchup + weather boost.")

# Run the script
generate_props()