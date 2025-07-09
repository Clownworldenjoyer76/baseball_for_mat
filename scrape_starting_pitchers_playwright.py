from playwright.sync_api import sync_playwright
import csv
from datetime import datetime

URL = "https://www.baseball-reference.com/previews/"

def fetch_starting_pitchers():
    today = datetime.today()
    date_str = today.strftime("%Y-%m-%d")
    output_path = "data/daily/todays_pitchers.csv"

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(URL, timeout=60000)

        rows = page.query_selector_all("div.section_content > ul > li")
        data = []

        for row in rows:
            text = row.inner_text().strip()
            if " at " in text and "Probable Pitchers:" in text:
                parts = text.split("Probable Pitchers:")
                teams = parts[0].strip()
                pitchers = parts[1].strip()
                away_team, home_team = teams.split(" at ")
                away_pitcher, home_pitcher = pitchers.split(", ")
                data.append([date_str, away_team, home_team, away_pitcher, home_pitcher])

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "away_team", "home_team", "away_pitcher", "home_pitcher"])
            writer.writerows(data)

        browser.close()

if __name__ == "__main__":
    fetch_starting_pitchers()
