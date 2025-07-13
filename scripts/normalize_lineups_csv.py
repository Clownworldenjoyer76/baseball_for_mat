from pathlib import Path
import pandas as pd

# === INPUT & OUTPUT FILES ===
input_path = Path("data/raw/lineups.csv")
team_map_path = Path("data/Data/team_name_map.csv")
output_path = Path("data/normalized/normalized_lineups.csv")

# === LOAD FILES ===
lineups = pd.read_csv(input_path)
team_map = pd.read_csv(team_map_path)

# === RENAME COLUMNS ===
lineups.rename(columns={
    "team code": "team",
    "player name": "last_name, first_name"
}, inplace=True)

# === MAP TEAM TO STANDARD ABBREVIATION (MATCHING ON team -> name) ===
name_to_abbr = dict(zip(team_map["name"], team_map["team"]))
lineups["team"] = lineups["team"].map(name_to_abbr)

# === DROP UNUSED COLUMNS ===
drop_cols = [
    "game_date",
    "game_number",
    "mlb id",
    "batting order",
    "confirmed",
    "position",
    "weather"
]
lineups.drop(columns=drop_cols, errors="ignore", inplace=True)

# === SAVE OUTPUT ===
output_path.parent.mkdir(parents=True, exist_ok=True)
lineups.to_csv(output_path, index=False)