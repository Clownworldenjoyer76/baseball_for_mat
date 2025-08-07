import statsapi
import pandas as pd
from pathlib import Path
from pandas import json_normalize

Path("data/rosters").mkdir(parents=True, exist_ok=True)

teams = statsapi.get('teams', {'sportIds': 1})['teams']

for team in teams:
    tid = team['id']
    name = team['name'].replace(' ', '_').replace('/', '_')

    data = statsapi.get("team_roster", {"teamId": tid, "rosterType": "40Man"})
    raw = data['roster']

    df = pd.json_normalize(
        raw,
        sep='_',
        meta=[
            ['person', 'id'],
            ['person', 'fullName'],
            ['person', 'primaryNumber'],
            ['person', 'primaryPosition', 'abbreviation'],
            ['person', 'batSide', 'code'],
            ['person', 'pitchHand', 'code'],
            ['person', 'height'],
            ['person', 'weight'],
            ['person', 'birthDate'],
            ['person', 'proDebutDate'],
            ['status', 'code'],
            ['status', 'description']
        ]
    )

    df = df.rename(columns={
        'person_id': 'id',
        'person_fullName': 'name',
        'person_primaryNumber': 'jersey',
        'person_primaryPosition_abbreviation': 'pos',
        'person_batSide_code': 'bat',
        'person_pitchHand_code': 'throw',
        'person_height': 'height',
        'person_weight': 'weight',
        'person_birthDate': 'dob',
        'person_proDebutDate': 'mlb_debut',
        'status_code': 'status_code',
        'status_description': 'status_description'
    })

    # Force content change for Git to detect
    df['timestamp'] = pd.Timestamp.now().isoformat()

    df.to_csv(f"data/rosters/{name}_roster.csv", index=False)

print("Flattened rosters saved.")