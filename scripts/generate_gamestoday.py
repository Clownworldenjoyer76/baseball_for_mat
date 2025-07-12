from pathlib import Path
import pandas as pd
import sys

# Required input files
required_files = {
    "todaysgames": "data/raw/todaysgames.csv",
    "lineups": "data/raw/lineups.csv",
    "batters": "data/cleaned/batters_normalized_cleaned.csv",
    "pitchers": "data/cleaned/pitchers_normalized_cleaned.csv",
    "team_map": "data/Data/team_name_map.csv",
    "stadiums": "data/Data/stadium_metadata.csv"
}

# Validate files
for name, path in required_files.items():
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"❌ Missing required file: {path}")
    if file_path.stat().st_size == 0:
        raise ValueError(f"❌ File is empty: {path}")

# Load all files
todaysgames = pd.read_csv(required_files["todaysgames"])
lineups = pd.read_csv(required_files["lineups"])
batters = pd.read_csv(required_files["batters"])
pitchers = pd.read_csv(required_files["pitchers"])
team_map = pd.read_csv(required_files["team_map"])
stadiums = pd.read_csv(required_files["stadiums"])

# Normalize team names
team_abbrev_to_name = dict(zip(team_map["name"], team_map["team"]))
todaysgames["home_team"] = todaysgames["home_team"].map(team_abbrev_to_name)
todaysgames["away_team"] = todaysgames["away_team"].map(team_abbrev_to_name)
lineups["team code"] = lineups["team code"].map(team_abbrev_to_name)

# Format player names
for df in [batters, pitchers]:
    if "last_name" in df.columns and "first_name" in df.columns:
        df["formatted_name"] = df["last_name"] + ", " + df["first_name"]
    else:
        raise KeyError("Missing 'last_name' or 'first_name' columns in player file.")

# Merge game and stadium info
merged = pd.merge(
    todaysgames,
    stadiums,
    how="left",
    on="home_team"
)

# Extract lineups
def get_lineup(team_name):
    players = lineups[lineups["team code"] == team_name].head(9)["name"].tolist()
    return players if len(players) == 9 else [None]*9

# Extract pitcher names
def get_pitcher_name(team_name, role):
    if role == "home":
        pitcher_row = pitchers[pitchers["team"] == team_name]
    else:
        pitcher_row = pitchers[pitchers["team"] == team_name]
    return pitcher_row["formatted_name"].values[0] if not pitcher_row.empty else None

# Final output rows
output_rows = []
for _, row in merged.iterrows():
    home = row["home_team"]
    away = row["away_team"]
    game = {
        "game_time": row["game_time"],
        "home_team": home,
        "home_pitcher": get_pitcher_name(home, "home"),
        "away_team": away,
        "away_pitcher": get_pitcher_name(away, "away"),
        "venue": row["venue"],
        "city": row["city"],
        "state": row["state"],
        "timezone": row["timezone"],
        "is_dome": row["is_dome"]
    }

    # Add lineup fields
    home_lineup = get_lineup(home)
    away_lineup = get_lineup(away)
    for i in range(9):
        game[f"home_lineup_{i+1}"] = home_lineup[i]
        game[f"away_lineup_{i+1}"] = away_lineup[i]

    output_rows.append(game)

# Output CSV
output_df = pd.DataFrame(output_rows)
output_path = Path("data/daily/games_today.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)
output_df.to_csv(output_path, index=False)

print(f"✔ Loaded {len(output_df)} games")
print(f"✔ Saved to {output_path}")
