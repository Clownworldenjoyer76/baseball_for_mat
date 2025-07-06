import requests
import json
from datetime import datetime

# === CONFIG ===
TODAY = datetime.today().strftime("%Y-%m-%d")
SCHEDULE_URL = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={TODAY}&expand=schedule.linescore,schedule.teams,schedule.game.content"
BOX_URL_TEMPLATE = "https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"

# === FETCH GAME SCHEDULE ===
schedule_resp = requests.get(SCHEDULE_URL)
schedule_data = schedule_resp.json()

games_out = []

for date_data in schedule_data.get("dates", []):
    for game in date_data.get("games", []):
        gamePk = game.get("gamePk")
        game_time = game.get("gameDate")
        venue = game.get("venue", {}).get("name")
        home_team = game["teams"]["home"]["team"]["name"]
        away_team = game["teams"]["away"]["team"]["name"]

        home_pitcher = game["teams"]["home"].get("probablePitcher", {}).get("fullName", "TBD")
        away_pitcher = game["teams"]["away"].get("probablePitcher", {}).get("fullName", "TBD")

        # Fetch boxscore to get lineups (if available)
        try:
            box_url = BOX_URL_TEMPLATE.format(gamePk=gamePk)
            box_resp = requests.get(box_url)
            box_data = box_resp.json()

            home_players = box_data["teams"]["home"]["players"]
            away_players = box_data["teams"]["away"]["players"]

            def extract_lineup(players):
                lineup = []
                for pid, pdata in players.items():
                    if "battingOrder" in pdata:
                        lineup.append((pdata["battingOrder"], pdata["person"]["fullName"]))
                lineup = sorted(lineup, key=lambda x: int(x[0]))
                return [x[1] for x in lineup]

            home_lineup = extract_lineup(home_players)
            away_lineup = extract_lineup(away_players)

        except Exception:
            home_lineup = []
            away_lineup = []

        games_out.append({
            "matchup": f"{away_team} @ {home_team}",
            "venue": venue,
            "game_time_utc": game_time,
            "home_pitcher": home_pitcher,
            "away_pitcher": away_pitcher,
            "home_lineup": home_lineup,
            "away_lineup": away_lineup
        })

# === SAVE TO FILE ===
with open("game_data.json", "w") as f:
    json.dump({"date": TODAY, "games": games_out}, f, indent=2)

print(f"Saved game_data.json with {len(games_out)} games.")