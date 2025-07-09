import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

URL = "https://www.baseball-reference.com/previews/"
OUTFILE = "data/daily/todays_pitchers.csv"

def extract_pitchers():
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, "html.parser")

    games = soup.select("div.game_summary")
    data = []

    for game in games:
        teams = game.select("table.teams td a")
        if len(teams) != 2:
            continue  # Skip malformed entries
        away_team = teams[0].text.strip()
        home_team = teams[1].text.strip()

        # Pitchers are in the <td> next to "Probable Pitchers"
        probable = game.find(string="Probable Pitchers:")
        if probable:
            pitchers_td = probable.find_next("td")
            if pitchers_td:
                pitchers = [p.strip() for p in pitchers_td.text.split("vs.")]
                if len(pitchers) == 2:
                    away_pitcher = pitchers[0]
                    home_pitcher = pitchers[1]
                else:
                    away_pitcher = home_pitcher = ""
            else:
                away_pitcher = home_pitcher = ""
        else:
            away_pitcher = home_pitcher = ""

        data.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "away_team": away_team,
            "home_team": home_team,
            "away_pitcher": away_pitcher,
            "home_pitcher": home_pitcher
        })

    df = pd.DataFrame(data)
    df.to_csv(OUTFILE, index=False)
    print(f"✅ Saved {len(df)} matchups to {OUTFILE}")

if __name__ == "__main__":
    extract_pitchers()
