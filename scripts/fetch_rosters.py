import statsapi
import pandas as pd
from pathlib import Path

# Create the output folder if it doesn't exist
Path("data/rosters").mkdir(parents=True, exist_ok=True)

# Fetch list of all MLB teams
teams = statsapi.get('teams', {'sportIds': 1})['teams']

# Loop through teams and write each roster to CSV
for team in teams:
    tid = team['id']
    name = team['name'].replace(' ', '_').replace('/', '_')
    data = statsapi.roster_data(team=tid, rosterType='40Man')
    df = pd.DataFrame(data['roster'])
    df.to_csv(f'data/rosters/{name}_roster.csv', index=False)