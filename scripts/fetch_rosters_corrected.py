import statsapi
import pandas as pd
from pathlib import Path

# Create the output directory if it doesn't exist
Path("data/rosters").mkdir(parents=True, exist_ok=True)

# Get all MLB teams
teams = statsapi.get('teams', {'sportIds': 1})['teams']

# Loop through each team, fetch 40-man roster, and save as CSV
for team in teams:
    tid = team['id']
    name = team['name'].replace(' ', '_').replace('/', '_')
    data = statsapi.get("team_roster", {"teamId": tid, "rosterType": "40Man"})
    df = pd.DataFrame(data['roster'])
    df.to_csv(f'data/rosters/{name}_roster.csv', index=False)

print("Rosters saved.")