import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

url = "https://www.baseball-reference.com/previews/"

response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"Failed to fetch data from Baseball-Reference. Status code: {response.status_code}")

soup = BeautifulSoup(response.content, "html.parser")

# Locate all links to today's games
game_links = soup.select("table#teams tbody tr td a[href*='/previews/']")

games_data = []
for link in game_links:
    href = link['href']
    full_link = f"https://www.baseball-reference.com{href}"
    game_resp = requests.get(full_link)
    if game_resp.status_code != 200:
        continue

    game_soup = BeautifulSoup(game_resp.content, "html.parser")

    try:
        header = game_soup.select_one("h1").text.strip()
        teams = header.split(" at ")
        away_team = teams[0].strip()
        home_team = teams[1].strip()

        pitchers = game_soup.select("div#meta div p")
        starters = [p.text for p in pitchers if "Starting Pitchers" in p.text]
        if starters:
            raw_text = starters[0]
            if ":" in raw_text:
                raw_text = raw_text.split(":", 1)[1]
            raw_text = raw_text.replace(" (L)", "").replace(" (R)", "")
            names = raw_text.split("vs.")
            away_pitcher = names[0].strip()
            home_pitcher = names[1].strip()
        else:
            away_pitcher = ""
            home_pitcher = ""

        games_data.append({
            "away_team": away_team,
            "home_team": home_team,
            "away_pitcher": away_pitcher,
            "home_pitcher": home_pitcher
        })
    except:
        continue

os.makedirs("data/daily", exist_ok=True)
df = pd.DataFrame(games_data)
df.to_csv("data/daily/todays_pitchers.csv", index=False)
print("✅ Scraped and saved to data/daily/todays_pitchers.csv")