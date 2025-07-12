from pathlib import Path
import pandas as pd

# File paths
REQUIRED_FILES = {
    "todaysgames": Path("data/raw/todaysgames.csv"),
    "lineups": Path("data/raw/lineups.csv"),
    "batters": Path("data/cleaned/batters_normalized_cleaned.csv"),
    "pitchers": Path("data/cleaned/pitchers_normalized_cleaned.csv"),
    "team_map": Path("data/Data/team_name_map.csv"),
    "stadium_meta": Path("data/Data/stadium_metadata.csv")
}

# Validate all input files
for label, path in REQUIRED_FILES.items():
    if not path.exists():
        raise FileNotFoundError(f"❌ Required file not found: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"❌ Required file is empty: {path}")

# Load CSVs
games_df = pd.read_csv(REQUIRED_FILES["todaysgames"])
lineups_df = pd.read_csv(REQUIRED_FILES["lineups"])
batters_df = pd.read_csv(REQUIRED_FILES["batters"])
pitchers_df = pd.read_csv(REQUIRED_FILES["pitchers"])
team_map_df = pd.read_csv(REQUIRED_FILES["team_map"])
stadium_df = pd.read_csv(REQUIRED_FILES["stadium_meta"])

# Normalize team names
team_map = dict(zip(team_map_df["name"], team_map_df["team"]))
games_df["home_team"] = games_df["home_team"].map(team_map)
games_df["away_team"] = games_df["away_team"].map(team_map)
lineups_df["team_code"] = lineups_df["team_code"].map(team_map)

# Format names as Last, First
def format_name(name):
    parts = name.strip().split()
    if len(parts) < 2:
        return name
    return f"{parts[-1]}, {' '.join(parts[:-1])}"

batters_df["name"] = batters_df["name"].apply(format_name)
pitchers_df["name"] = pitchers_df["name"].apply(format_name)

# Prepare game rows
output_rows = []
for _, game in games_df.iterrows():
    home = game["home_team"]
    away = game["away_team"]
    game_time = game["game_time"]

    venue_row = stadium_df[stadium_df["home_team"] == home].iloc[0]
    venue = venue_row["venue"]
    city = venue_row["city"]
    state = venue_row["state"]
    timezone = venue_row["timezone"]
    is_dome = venue_row["is_dome"]

    home_pitcher = pitchers_df[pitchers_df["team"] == home]["name"].values[0]
    away_pitcher = pitchers_df[pitchers_df["team"] == away]["name"].values[0]

    home_lineup = lineups_df[lineups_df["team_code"] == home]["player_name"].head(9).tolist()
    away_lineup = lineups_df[lineups_df["team_code"] == away]["player_name"].head(9).tolist()

    row = {
        "game_time": game_time,
        "home_team": home,
        "home_pitcher": home_pitcher,
        "away_team": away,
        "away_pitcher": away_pitcher,
        "venue": venue,
        "city": city,
        "state": state,
        "timezone": timezone,
        "is_dome": is_dome
    }
    for i in range(9):
        row[f"home_lineup_{i+1}"] = home_lineup[i] if i < len(home_lineup) else ""
        row[f"away_lineup_{i+1}"] = away_lineup[i] if i < len(away_lineup) else ""
    output_rows.append(row)

# Save to output file
output_path = Path("data/daily/games_today.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)
pd.DataFrame(output_rows).to_csv(output_path, index=False)
print(f"✔ Loaded {len(output_rows)} games")
print(f"✔ Saved to {output_path}")