
from pathlib import Path
import pandas as pd

# === FILE PATHS ===
input_paths = {
    "games": Path("data/raw/todaysgames.csv"),
    "lineups": Path("data/raw/lineups.csv"),
    "batters": Path("data/cleaned/batters_normalized_cleaned.csv"),
    "pitchers": Path("data/cleaned/pitchers_normalized_cleaned.csv"),
    "team_map": Path("data/Data/team_name_map.csv"),
    "stadiums": Path("data/Data/stadium_metadata.csv"),
}
output_path = Path("data/daily/games_today.csv")

# === VALIDATION ===
for name, path in input_paths.items():
    if not path.exists():
        raise FileNotFoundError(f"❌ Missing file: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"❌ Empty file: {path}")

# === LOAD FILES ===
games = pd.read_csv(input_paths["games"])
lineups = pd.read_csv(input_paths["lineups"])
batters = pd.read_csv(input_paths["batters"])
pitchers = pd.read_csv(input_paths["pitchers"])
team_map = pd.read_csv(input_paths["team_map"])
stadiums = pd.read_csv(input_paths["stadiums"])

# === TEAM NAME NORMALIZATION ===
name_to_abbr = dict(zip(team_map["name"], team_map["team"]))
games["home_team"] = games["home_team"].map(name_to_abbr)
games["away_team"] = games["away_team"].map(name_to_abbr)
lineups["team"] = lineups["team code"].map(team_name_map)
    
    # === FINAL DATA AGGREGATION ===
output_rows = []
for _, row in games.iterrows():
    home_team = row["home_team"]
    away_team = row["away_team"]

    home_pitcher = pitchers.loc[pitchers["team"] == home_team, "name"].iloc[0]
    away_pitcher = pitchers.loc[pitchers["team"] == away_team, "name"].iloc[0]

    home_lineup = lineups.loc[lineups["team"] == home_team, "player"].tolist()
    away_lineup = lineups.loc[lineups["team"] == away_team, "player"].tolist()

    stadium = stadiums.loc[stadiums["home_team"] == home_team].iloc[0]

    output_rows.append({
        "game_time": stadium["game_time"],
        "home_team": home_team,
        "home_pitcher": home_pitcher,
        "home_lineup": home_lineup,
        "away_team": away_team,
        "away_pitcher": away_pitcher,
        "away_lineup": away_lineup,
        "venue": stadium["venue"],
        "city": stadium["city"],
        "state": stadium["state"],
        "timezone": stadium["timezone"],
        "is_dome": stadium["is_dome"]
    })

# === SAVE OUTPUT ===
output_df = pd.DataFrame(output_rows)
output_path.parent.mkdir(parents=True, exist_ok=True)
output_df.to_csv(output_path, index=False)

# === CONFIRMATION ===
print(f"✔ Loaded {len(output_df)} games")
print(f"✔ Saved to {output_path}")
