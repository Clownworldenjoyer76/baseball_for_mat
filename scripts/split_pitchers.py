import pandas as pd
from pathlib import Path

df = pd.read_csv("data/raw/todaysgames_normalized.csv")

home_pitchers = df[['home_team', 'pitcher_home']].rename(columns={'home_team': 'team', 'pitcher_home': 'pitcher'})
away_pitchers = df[['away_team', 'pitcher_away']].rename(columns={'away_team': 'team', 'pitcher_away': 'pitcher'})

output_path = Path("data/adjusted")
output_path.mkdir(parents=True, exist_ok=True)

home_pitchers.to_csv(output_path / "pitchers_home.csv", index=False)
away_pitchers.to_csv(output_path / "pitchers_away.csv", index=False)
