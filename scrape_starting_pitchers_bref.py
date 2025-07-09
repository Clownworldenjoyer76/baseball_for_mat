import requests
from bs4 import BeautifulSoup
import csv
import datetime

def scrape_starting_pitchers():
    url = "https://www.baseball-reference.com/previews/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch previews page. Status code: {response.status_code}")

    soup = BeautifulSoup(response.content, "html.parser")
    preview_links = soup.select("ul.page_index li a")

    results = []
    for link in preview_links:
        href = link.get("href")
        if not href:
            continue

        game_url = f"https://www.baseball-reference.com{href}"
        game_res = requests.get(game_url, headers=headers)
        if game_res.status_code != 200:
            continue

        game_soup = BeautifulSoup(game_res.content, "html.parser")
        title = game_soup.select_one("h1")
        if not title:
            continue

        teams = title.text.strip().replace(" Preview", "").split(" at ")
        if len(teams) != 2:
            continue

        away_team, home_team = teams
        pitching_section = game_soup.find("div", string=lambda s: s and "Starting Pitchers" in s)
        if not pitching_section:
            continue

        pitcher_lines = pitching_section.find_next("ul")
        if not pitcher_lines:
            continue

        pitchers = [li.text.strip() for li in pitcher_lines.find_all("li")]
        if len(pitchers) != 2:
            continue

        results.append({
            "away_team": away_team,
            "home_team": home_team,
            "away_pitcher": pitchers[0],
            "home_pitcher": pitchers[1]
        })

    # Save to CSV
    today = datetime.date.today()
    out_file = Path("data/daily/todays_pitchers.csv")
    out_file.parent.mkdir(parents=True, exist_ok=True)

    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["away_team", "home_team", "away_pitcher", "home_pitcher"])
        writer.writeheader()
        for row in results:
            writer.writerow(row)

if __name__ == "__main__":
    scrape_starting_pitchers()
