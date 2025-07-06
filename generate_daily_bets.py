import requests
import pandas as pd
import json
from datetime import datetime

# === CONFIG ===
PARK_FACTORS_PATH = "data/Data/park_factors_full_verified.csv"
OUTPUT_PATH = "daily_bets.json"
TODAY = datetime.today().strftime("%Y-%m-%d")
MLB_API_URL = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={TODAY}"

# === LOAD PARK FACTORS ===
park_factors = pd.read_csv(PARK_FACTORS_PATH)

def get_park_factors(home_team):
    row = park_factors[park_factors["Team"].str.lower() == home_team.lower()]
    return row.iloc[0].to_dict() if not row.empty else {}

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

        games_out.append({
            "matchup": matchup,
            "parlay": {
                "moneyline_or_spread": None,
                "total": None,
                "player_props": []
            },
            "park_factors": {
                "venue": park_data.get("Venue"),
                "HR": park_data.get("HR"),
                "H": park_data.get("H"),
                "HardHit": park_data.get("HardHit"),
                "OBP": park_data.get("OBP"),
                "wOBACon": park_data.get("wOBACon"),
                "xwOBACon": park_data.get("xwOBACon")
            }
        })

# === GENERATE FILE ===
out = {
    "date": TODAY,
    "games": games_out,
    "top_hr_hitters": [],
    "top_hit_props": []
}

with open(OUTPUT_PATH, "w") as f:
    json.dump(out, f, indent=2)

print(f"Generated {OUTPUT_PATH} with {len(games_out)} games.")