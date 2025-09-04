import pandas as pd
from pathlib import Path

# after you’ve formed `z` (the expanded batter rows with team info)…
weather = pd.read_csv("data/weather_input.csv", dtype={"home_team_id":int,"away_team_id":int,"game_id":int})

# (A) if your z already has team_id
if "team_id" in z.columns:
    game_map = pd.concat([
        weather.loc[:,["date","game_id","home_team_id"]].rename(columns={"home_team_id":"team_id"}),
        weather.loc[:,["date","game_id","away_team_id"]].rename(columns={"away_team_id":"team_id"})
    ], ignore_index=True)
    z = z.merge(game_map, on="team_id", how="left")

# (B) if your z only has team names, map to ids once, then merge
else:
    teams_ref = pd.read_csv("data/teams_reference.csv")  # columns: team, team_id
    z = z.merge(teams_ref, on="team", how="left")
    game_map = pd.concat([
        weather.loc[:,["date","game_id","home_team_id"]].rename(columns={"home_team_id":"team_id"}),
        weather.loc[:,["date","game_id","away_team_id"]].rename(columns={"away_team_id":"team_id"})
    ], ignore_index=True)
    z = z.merge(game_map, on="team_id", how="left")

# keep in the final CSV
cols_order = ["player_id","name","team","team_id","game_id","date",  # new
              "proj_pa","proj_avg","proj_iso","proj_hr_rate",
              "weather_factor","adj_woba_weather","adj_woba_park","adj_woba_combined",
              "venue","game_time_et","temperature","wind_speed","humidity"]
z = z[[c for c in cols_order if c in z.columns]]
z.to_csv("data/_projections/batter_props_z_expanded.csv", index=False)
