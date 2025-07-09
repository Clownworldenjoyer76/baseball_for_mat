import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

def scrape_espn_starting_pitchers():
    url = "https://www.espn.com/mlb/schedule"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    games = []

    schedule_tables = soup.find_all("table", class_="schedule")

    for table in schedule_tables:
        rows = table.find_all("tr")[1:]  # Skip header
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 3:
                continue

            matchup = cols[0].text.strip().split(" at ")
            if len(matchup) != 2:
                continue

            away_team = matchup[0]
            home_team = matchup[1]
            pitcher_info = cols[2].text.strip()

            away_pitcher = ""
            home_pitcher = ""

            if "vs." in pitcher_info:
                pitchers = pitcher_info.split("vs.")
                if len(pitchers) == 2:
                    away_pitcher = pitchers[0].strip()
                    home_pitcher = pitchers[1].strip()

            games.append({
                "date": datetime.today().strftime("%Y-%m-%d"),
                "away_team": away_team,
                "home_team": home_team,
                "away_pitcher": away_pitcher,
                "home_pitcher": home_pitcher
            })

    df = pd.DataFrame(games)
    os.makedirs("data/daily", exist_ok=True)
    df.to_csv("data/daily/todays_pitchers.csv", index=False)
    print("✅ Saved to data/daily/todays_pitchers.csv")

if __name__ == "__main__":
    scrape_espn_starting_pitchers()
