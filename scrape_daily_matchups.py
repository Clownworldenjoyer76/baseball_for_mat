import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os

# Set output path
os.makedirs("data/daily", exist_ok=True)
output_path = "data/daily/matchups_today.csv"

# ESPN schedule URL
url = "https://www.espn.com/mlb/schedule"

# Send request
response = requests.get(url)
if response.status_code != 200:
    print(f"❌ Failed to fetch ESPN schedule page (Status: {response.status_code})")
    exit(1)

soup = BeautifulSoup(response.text, "html.parser")

# Parse the schedule
games = soup.select(".schedule.has-team-logos tbody tr")
if not games:
    print("❌ No games found. Structure may have changed.")
    exit(1)

matchups = []
for row in games:
    cols = row.find_all("td")
    if len(cols) < 3:
        continue
    teams = cols[0].get_text(strip=True).split(" vs. ")
    if len(teams) != 2:
        continue
    away_team, home_team = teams
    pitchers = cols[1].get_text(strip=True).split(" vs ")
    if len(pitchers) != 2:
        continue
    away_pitcher, home_pitcher = pitchers

    matchups.append({
        "date": datetime.today().strftime("%Y-%m-%d"),
        "away_team": away_team,
        "home_team": home_team,
        "away_pitcher": away_pitcher,
        "home_pitcher": home_pitcher,
    })

# Save to CSV
df = pd.DataFrame(matchups)
df.to_csv(output_path, index=False)
print(f"✅ Saved {len(df)} matchups to {output_path}")
