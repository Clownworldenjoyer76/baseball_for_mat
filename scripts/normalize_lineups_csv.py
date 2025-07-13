from pathlib import Path
import pandas as pd

# === INPUT & OUTPUT FILES ===
input_path = Path("data/raw/lineups.csv")
team_map_path = Path("data/Data/team_name_map.csv")
output_path = Path("data/normalized/normalized_lineups.csv")

# === VALIDATION ===
for path in [input_path, team_map_path]:
    if not path.exists():
        raise FileNotFoundError(f"❌ Missing file: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"❌ Empty file: {path}")

# === LOAD FILES ===
lineups = pd.read_csv(input_path)
team_map = pd.read_csv(team_map_path)

# === RENAME COLUMNS ===
lineups.rename(columns={
    "team code": "team",
    "player name": "last_name, first_name"
}, inplace=True)

# === NORMALIZE PLAYER NAMES ===
def normalize_name(name):
    parts = name.strip().split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

lineups["last_name, first_name"] = lineups["last_name, first_name"].apply(normalize_name)

# === MAP TEAM TO STANDARD ABBREVIATION ===
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

print(f"✔ Normalized CSV saved to: {output_path}")
