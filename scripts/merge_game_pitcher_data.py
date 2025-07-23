import pandas as pd

# Load the data
batters_away = pd.read_csv("data/adjusted/batters_away_weather_park.csv")
pitchers_home = pd.read_csv("data/adjusted/pitchers_home_weather_park.csv")
pitchers_away = pd.read_csv("data/adjusted/pitchers_away_weather_park.csv")

# Confirm required columns
required_columns = [
    ("batters_away", "last_name, first_name"),
    ("pitchers_home", "last_name, first_name"),
    ("pitchers_home", "home_team"),
    ("pitchers_away", "last_name, first_name"),
    ("pitchers_away", "away_team_park"),
]

for df_name, col in required_columns:
    df = eval(df_name)
    if col not in df.columns:
        raise ValueError(f"Missing column '{col}' in {df_name} file")

# Merge home pitcher data onto batters_away
batters_away = batters_away.merge(
    pitchers_home,
    left_on=["home_team"],
    right_on=["home_team"],
    how="left",
    suffixes=("", "_pitcher_home")
)

# Merge away pitcher data
batters_away = batters_away.merge(
    pitchers_away,
    left_on=["away_team"],
    right_on=["away_team_park"],
    how="left",
    suffixes=("", "_pitcher_away")
)

# Save output
batters_away.to_csv("data/final/matchup_stats.csv", index=False)
print("✅ Saved merged data to data/final/matchup_stats.csv")
