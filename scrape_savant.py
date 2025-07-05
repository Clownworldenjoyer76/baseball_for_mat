import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re

# Baseball Savant Statcast batter leaderboard
url = "https://baseballsavant.mlb.com/leaderboard/statcast"

# Send request
headers = {
    "User-Agent": "Mozilla/5.0"
}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

# Find the embedded JS script that contains the leaderboard data
script_tags = soup.find_all("script")
data_script = None
for s in script_tags:
    if "var leaderboardData =" in s.text:
        data_script = s.text
        break

if not data_script:
    raise Exception("Could not find data in page source.")

# Extract the JSON object from JS
start = data_script.find("var leaderboardData = ") + len("var leaderboardData = ")
end = data_script.find(";
", start)
json_str = data_script[start:end].strip()
data = json.loads(json_str)

# Convert to DataFrame
df = pd.DataFrame(data)
df = df[["player_name", "team", "xwOBA", "barrel_batted_rate", "sweet_spot_percent"]]
df.columns = ["batter", "team", "xwoba", "barrel_rate", "sweet_spot"]
df = df.dropna().head(10)

# Assign fake opposing pitchers (upgrade later)
pitchers = ["Gerrit Cole", "Chris Sale", "Yu Darvish", "Max Fried", "Zack Wheeler",
            "Logan Webb", "Nick Lodolo", "Paul Skenes", "Tylor Megill", "Kyle Freeland"]
df["pitcher"] = pitchers[:len(df)]

# Calculate edge score
df["edge_score"] = ((df["xwoba"].astype(float) + df["barrel_rate"] / 100 + df["sweet_spot"] / 100) * 10).round(2)

# Output to JSON
props = df.to_dict(orient="records")
with open("top_props.json", "w") as f:
    json.dump(props, f, indent=2)

print("✅ Scraped and generated top_props.json successfully.")