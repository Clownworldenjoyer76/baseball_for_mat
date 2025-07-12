
from pathlib import Path
import pandas as pd

# Required file paths
required_files = {
    "games": "data/raw/todaysgames.csv",
    "lineups": "data/raw/lineups.csv",
    "batters": "data/cleaned/batters_normalized_cleaned.csv",
    "pitchers": "data/cleaned/pitchers_normalized_cleaned.csv",
    "team_map": "data/Data/team_name_map.csv",
    "stadium": "data/Data/stadium_metadata.csv"
}

# Validate all files exist and are non-empty
for label, path in required_files.items():
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"❌ Required file not found: {path}")
    if file_path.stat().st_size == 0:
        raise ValueError(f"❌ Required file is empty: {path}")

# Load all files
games_df = pd.read_csv(required_files["games"])
lineups_df = pd.read_csv(required_files["lineups"])
batters_df = pd.read_csv(required_files["batters"])
pitchers_df = pd.read_csv(required_files["pitchers"])
team_map_df = pd.read_csv(required_files["team_map"])
stadium_df = pd.read_csv(required_files["stadium"])

# Build team map
team_map = dict(zip(team_map_df["name"], team_map_df["team"]))

# Normalize team names in games and lineups
games_df["home_team"] = games_df["home_team"].map(team_map)
games_df["away_team"] = games_df["away_team"].map(team_map)
lineups_df["team code"] = lineups_df["team code"].map(team_map)

# Normalize player names
def format_name(name):
    parts = name.split()
    return f"{parts[-1]}, {' '.join(parts[:-1])}" if len(parts) > 1 else name

batters_df["name"] = batters_df["name"].apply(format_name)
pitchers_df["name"] = pitchers_df["name"].apply(format_name)
lineups_df["name"] = lineups_df["name"].apply(format_name)

# Collect output rows
output_rows = []
for _, row in games_df.iterrows():
    home = row["home_team"]
    away = row["away_team"]

    home_pitcher = pitchers_df[pitchers_df["team"] == home]["name"].values[0] if not pitchers_df[pitchers_df["team"] == home].empty else "Unknown"
    away_pitcher = pitchers_df[pitchers_df["team"] == away]["name"].values[0] if not pitchers_df[pitchers_df["team"] == away].empty else "Unknown"

    home_lineup = lineups_df[lineups_df["team code"] == home]["name"].head(9).tolist()
    away_lineup = lineups_df[lineups_df["team code"] == away]["name"].head(9).tolist()

    venue_row = stadium_df[stadium_df["home_team"] == home]
    if venue_row.empty:
        continue

    output_rows.append({
        "game_time": row["game_time"],
        "home_team": home,
        "home_pitcher": home_pitcher,
        "home_lineup": home_lineup,
        "away_team": away,
        "away_pitcher": away_pitcher,
        "away_lineup": away_lineup,
        "venue": venue_row["venue"].values[0],
        "city": venue_row["city"].values[0],
        "state": venue_row["state"].values[0],
        "timezone": venue_row["timezone"].values[0],
        "is_dome": venue_row["is_dome"].values[0],
    })

# Create output DataFrame
games_today_df = pd.DataFrame(output_rows)

# Save to file
output_path = Path("data/daily/games_today.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)
games_today_df.to_csv(output_path, index=False)

print(f"✔ Loaded {len(games_today_df)} games")
print(f"✔ Saved to {output_path}")
