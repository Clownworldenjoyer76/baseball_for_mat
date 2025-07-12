from pathlib import Path
import pandas as pd

# Define required input paths
input_paths = {
    "games": Path("data/raw/todaysgames.csv"),
    "lineups": Path("data/raw/lineups.csv"),
    "batters": Path("data/cleaned/batters_normalized_cleaned.csv"),
    "pitchers": Path("data/cleaned/pitchers_normalized_cleaned.csv"),
    "team_map": Path("data/Data/team_name_map.csv"),
    "stadiums": Path("data/Data/stadium_metadata.csv"),
}

# Validate file existence and non-emptiness
for name, path in input_paths.items():
    if not path.exists():
        raise FileNotFoundError(f"❌ Missing file: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"❌ File is empty: {path}")
    input_paths[name] = df

# Unpack validated data
games_df = input_paths["games"]
lineups_df = input_paths["lineups"]
batters_df = input_paths["batters"]
pitchers_df = input_paths["pitchers"]
team_map_df = input_paths["team_map"]
stadiums_df = input_paths["stadiums"]

# Build team name maps
name_to_abbr = dict(zip(team_map_df["name"], team_map_df.index))
name_to_full = dict(zip(team_map_df.index, team_map_df["team"]))

# Normalize team names in games_df
games_df["home_team"] = games_df["home_team"].map(name_to_abbr)
games_df["away_team"] = games_df["away_team"].map(name_to_abbr)

# Normalize team names in lineups_df
lineups_df["team_code"] = lineups_df["team code"].map(str)

# Normalize player names: "First Last" → "Last, First"
def format_name(name):
    if not isinstance(name, str):
        return ""
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[-1]}, {' '.join(parts[:-1])}"
    return name

batters_df["name"] = batters_df["name"].apply(format_name)
pitchers_df["name"] = pitchers_df["name"].apply(format_name)

# Build output rows
rows = []
for _, game in games_df.iterrows():
    home = game["home_team"]
    away = game["away_team"]

    # Get venue metadata
    venue_info = stadiums_df[
        (stadiums_df["home_team"] == home)
    ].iloc[0].to_dict()

    # Pull lineups
    home_lineup = lineups_df[(lineups_df["team_code"] == home) & (lineups_df["batting order"] <= 9)]["player name"].tolist()
    away_lineup = lineups_df[(lineups_df["team_code"] == away) & (lineups_df["batting order"] <= 9)]["player name"].tolist()

    # Pull starting pitchers
    home_pitcher = pitchers_df[pitchers_df["team"] == home]["name"].iloc[0]
    away_pitcher = pitchers_df[pitchers_df["team"] == away]["name"].iloc[0]

    rows.append({
        "game_time": game["game_time"],
        "home_team": home,
        "home_pitcher": home_pitcher,
        "home_lineup": home_lineup,
        "away_team": away,
        "away_pitcher": away_pitcher,
        "away_lineup": away_lineup,
        "venue": venue_info["stadium"],
        "city": venue_info["city"],
        "state": venue_info["state"],
        "timezone": venue_info["timezone"],
        "is_dome": venue_info["is_dome"],
    })

# Output
output_df = pd.DataFrame(rows)
output_path = Path("data/daily/games_today.csv")
output_path.parent.mkdir(parents=True, exist_ok=True)
output_df.to_csv(output_path, index=False)

# Confirm
print(f"✔ Loaded {len(games_df)} games")
print(f"✔ Saved to {output_path}")