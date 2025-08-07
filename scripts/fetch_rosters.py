import statsapi
import pandas as pd
from pathlib import Path

Path("data/rosters").mkdir(parents=True, exist_ok=True)

teams = statsapi.get('teams', {'sportIds': 1})['teams']

for team in teams:
    tid = team['id']
    name = team['name'].replace(' ', '_').replace('/', '_')
    data = statsapi.get("team_roster", {"teamId": tid, "rosterType": "40Man"})
    df = pd.DataFrame(data['roster'])
    df.to_csv(f'data/rosters/{name}_roster.csv', index=False)

print("Rosters saved.")
