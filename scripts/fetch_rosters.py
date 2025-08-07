import statsapi
import pandas as pd
from pathlib import Path
from pandas import json_normalize

# Create the output directory
Path("data/rosters").mkdir(parents=True, exist_ok=True)

# Get all MLB teams
teams = statsapi.get('teams', {'sportIds': 1})['teams']

for team in teams:
    tid = team['id']
    name = team['name'].replace(' ', '_').replace('/', '_')

    data = statsapi.get("team_roster", {"teamId": tid, "rosterType": "40Man"})
    raw = data['roster']

    # Flatten nested JSON
    df = json_normalize(raw)

    # Keep relevant columns
    columns_to_keep = [
        'person.id',
        'person.fullName',
        'person.primaryNumber',
        'person.primaryPosition.name',
        'position.abbreviation',
        'person.batSide.code',
        'person.pitchHand.code',
        'person.height',
        'person.weight',
        'person.birthDate',
        'person.proDebutDate',
        'person.status.code'
    ]

    df = df[columns_to_keep]
    df.columns = [col.split('.')[-1] for col in df.columns]  # simplify column names

    df.to_csv(f'data/rosters/{name}_roster.csv', index=False)

print("Rosters saved.")