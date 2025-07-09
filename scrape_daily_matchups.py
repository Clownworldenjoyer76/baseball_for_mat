import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

url = "https://www.espn.com/mlb/schedule"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"
}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
except requests.RequestException as e:
    print(f"❌ Failed to fetch ESPN schedule page: {e}")
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")

# Containers for final data
games = []

schedule_tables = soup.find_all("table")
for table in schedule_tables:
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) >= 3:
            matchup = cols[0].text.strip()
            time = cols[1].text.strip()
            pitchers = cols[2].text.strip().split(" vs ")

            if len(pitchers) == 2:
                away_team, home_team = matchup.split(" at ")
                away_pitcher = pitchers[0].strip()
                home_pitcher = pitchers[1].strip()

                games.append({
                    "away_team": away_team,
                    "home_team": home_team,
                    "away_pitcher": away_pitcher,
                    "home_pitcher": home_pitcher,
                    "game_time": time
                })

# Output directory
os.makedirs("data/daily", exist_ok=True)
output_path = "data/daily/todays_pitchers.csv"
df = pd.DataFrame(games)
df.to_csv(output_path, index=False)
print(f"✅ Scraped {len(df)} games and saved to {output_path}")