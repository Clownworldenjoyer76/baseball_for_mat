import json
import csv
from datetime import date
from pathlib import Path
from data.team_name_map import TEAM_NICKNAME_MAP

# Load park factor data
def load_park_factors(filepath):
    park_factors = {}
    with open(filepath, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            team = row["Team"].strip()
            park_factors[team] = {
                "venue": row.get("Venue", ""),
                "HR": try_parse_float(row.get("HR")),
                "H": try_parse_float(row.get("H")),
                "HardHit": try_parse_float(row.get("HardHit")),
                "OBP": try_parse_float(row.get("OBP")),
                "wOBACon": try_parse_float(row.get("wOBACon")),
                "xwOBACon": try_parse_float(row.get("xwOBACon")),
            }
    return park_factors

def try_parse_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

# Dummy game list — replace with scraped or live data
def get_games():
    return [
        "Atlanta Braves vs Baltimore Orioles",
        "Washington Nationals vs Boston Red Sox",
        "Philadelphia Phillies vs Cincinnati Reds",
        "Toronto Blue Jays vs Los Angeles Angels",
        "Cleveland Guardians vs Detroit Tigers",
        "New York Mets vs New York Yankees",
        "Miami Marlins vs Milwaukee Brewers",
        "Minnesota Twins vs Tampa Bay Rays",
        "Colorado Rockies vs Chicago White Sox",
        "Arizona Diamondbacks vs Kansas City Royals",
        "Los Angeles Dodgers vs Houston Astros",
        "Seattle Mariners vs Pittsburgh Pirates",
        "Chicago Cubs vs St. Louis Cardinals",
        "San Diego Padres vs Texas Rangers",
        "Athletics vs San Francisco Giants"
    ]

def main():
    park_data = load_park_factors("data/Data/park_factors_full_verified.csv")
    output = {"date": str(date.today()), "games": []}

    for matchup in get_games():
        try:
            away_team, home_team = matchup.split(" vs ")
            away_team, home_team = away_team.strip(), home_team.strip()
        except ValueError:
            print(f"⚠️ Bad matchup format: {matchup}")
            continue

        nickname = TEAM_NICKNAME_MAP.get(home_team, "")
        venue_info = park_data.get(nickname, None)

        if venue_info is None:
            print(f"⚠️ No park data for: {home_team} (nickname: {nickname})")

        output["games"].append({
            "matchup": matchup,
            "parlay": {
                "moneyline_or_spread": None,
                "total": None,
                "player_props": []
            },
            "park_factors": venue_info if venue_info else {
                "venue": "",
                "HR": None,
                "H": None,
                "HardHit": None,
                "OBP": None,
                "wOBACon": None,
                "xwOBACon": None
            },
            "weather": {
                "note": "Weather unavailable — venue not recognized"
            }
        })

    with open("daily_bets.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

if __name__ == "__main__":
    main()
