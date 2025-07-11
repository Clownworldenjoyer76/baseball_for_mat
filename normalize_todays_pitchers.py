
import pandas as pd
import os
import re
from unidecode import unidecode

def normalize_name(name):
    name = unidecode(name)  # remove accents
    name = re.sub(r'\.', '', name)  # remove periods
    name = re.sub(r'\b(Jr|Sr|II|III|IV)\b', '', name)  # remove suffixes
    name = ' '.join(name.split())  # remove extra spaces
    if ',' not in name:
        parts = name.strip().split()
        if len(parts) >= 2:
            last = parts[-1]
            first = ' '.join(parts[:-1])
            name = f"{last}, {first}"
    return name.strip()

input_path = "data/daily/todays_pitchers.csv"
output_path = "data/daily/todays_pitchers_normalized.csv"

if os.path.exists(input_path):
    df = pd.read_csv(input_path)

    df['away_pitcher'] = df['away_pitcher'].astype(str).apply(normalize_name)
    df['home_pitcher'] = df['home_pitcher'].astype(str).apply(normalize_name)

    df.to_csv(output_path, index=False)
    print(f"✅ Normalized file written to {output_path}")
else:
    print(f"❌ File not found: {input_path}")
