#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

WEATHER_INPUT = Path("data/weather_input.csv")
WEATHER_TEAMS_OUT = Path("data/weather_teams.csv")

TEAM_FIX = {
    # squash/no-space variants → proper
    "redsox": "Red Sox",
    "whitesox": "White Sox",
    "bluejays": "Blue Jays",
    "diamondbacks": "Diamondbacks",
    "braves": "Braves",
    "cubs": "Cubs",
    "dodgers": "Dodgers",
    "mariners": "Mariners",
    "marlins": "Marlins",
    "nationals": "Nationals",
    "padres": "Padres",
    "phillies": "Phillies",
    "pirates": "Pirates",
    "rays": "Rays",
    "rockies": "Rockies",
    "tigers": "Tigers",
    "twins": "Twins",
    # common aliases
    "white sox": "White Sox",
    "red sox": "Red Sox",
    "blue jays": "Blue Jays",
}

def normalize_series(s: pd.Series) -> pd.Series:
    base = s.astype(str).str.strip()
    key = base.str.lower().str.replace(" ", "", regex=False)
    fixed = key.map(TEAM_FIX).fillna(base.str.title())
    return fixed

def main():
    df = pd.read_csv(WEATHER_INPUT)
    # normalize early and in-place
    for col in ["home_team", "away_team"]:
        if col in df.columns:
            df[col] = normalize_series(df[col])
    # also write a slim file other scripts rely on
    out = df[["home_team", "away_team"]].copy()
    out.to_csv(WEATHER_TEAMS_OUT, index=False)
    # overwrite input so downstream is clean
    df.to_csv(WEATHER_INPUT, index=False)
    print(f"✅ Normalized weather teams and wrote {WEATHER_TEAMS_OUT}")

if __name__ == "__main__":
    main()
