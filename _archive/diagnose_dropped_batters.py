import pandas as pd

today = pd.read_csv("data/cleaned/batters_today.csv")
home = pd.read_csv("data/adjusted/batters_home.csv")
away = pd.read_csv("data/adjusted/batters_away.csv")

combined = pd.concat([home, away])
missing = today[~today["name"].isin(combined["name"])]

print("🔎 Dropped batters:")
print(missing[["name", "team", "home_team", "away_team"]])
print(f"\n❌ Missing count: {len(missing)}")
