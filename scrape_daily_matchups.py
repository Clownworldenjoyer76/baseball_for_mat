import asyncio
from playwright.async_api import async_playwright
import csv
import os

output_path = "data/daily/todays_pitchers.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        url = "https://www.espn.com/mlb/schedule"
        await page.goto(url, timeout=60000)

        games = await page.query_selector_all(".responsive-table-wrap table")

        rows = []
        for table in games:
            rows_in_table = await table.query_selector_all("tbody > tr")
            for row in rows_in_table:
                columns = await row.query_selector_all("td")
                if len(columns) < 3:
                    continue
                matchup_text = await columns[0].inner_text()
                pitchers_text = await columns[2].inner_text()

                if "vs" in matchup_text and "at" not in matchup_text:
                    teams = matchup_text.split(" vs ")
                elif "at" in matchup_text:
                    teams = matchup_text.split(" at ")
                else:
                    continue

                if len(teams) != 2:
                    continue

                away_team = teams[0].strip()
                home_team = teams[1].strip()

                if "vs" in pitchers_text or "at" in pitchers_text:
                    pitchers = (
                        pitchers_text.split(" vs ")
                        if "vs" in pitchers_text
                        else pitchers_text.split(" at ")
                    )
                else:
                    continue

                if len(pitchers) != 2:
                    continue

                away_pitcher = pitchers[0].strip()
                home_pitcher = pitchers[1].strip()

                rows.append([away_team, away_pitcher, home_team, home_pitcher])

        await browser.close()

        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["away_team", "away_pitcher", "home_team", "home_pitcher"])
            writer.writerows(rows)

asyncio.run(scrape())