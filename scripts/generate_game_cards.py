
import pandas as pd
import json
from pathlib import Path

pitcher = pd.read_csv("data/_projections/pitcher_props_projected.csv")
batter = pd.read_csv("data/_projections/batter_props_projected.csv")
weather = pd.read_csv("data/weather_adjustments.csv")

weather["game_key"] = weather["away_team"] + " @ " + weather["home_team"]

# BATTER JOIN
batter_game = pd.merge(batter, weather[["home_team", "away_team", "game_key"]],
                       how="left", left_on="team", right_on="away_team")
batter_game["game_key"] = batter_game["game_key"].fillna(
    pd.merge(batter, weather[["home_team", "away_team", "game_key"]],
             how="left", left_on="team", right_on="home_team")["game_key"]
)
batter_game = batter_game.dropna(subset=["game_key"])
batter_game["z_score_hit"] = (batter_game["total_hits_projection"] - batter_game["total_hits_projection"].mean()) / batter_game["total_hits_projection"].std()
batter_game["z_score_hr"] = (batter_game["avg_hr"] - batter_game["avg_hr"].mean()) / batter_game["avg_hr"].std()

# PITCHER JOIN
pitcher_game = pd.merge(pitcher, weather[["home_team", "away_team", "game_key"]],
                        how="left", left_on="team", right_on="away_team")
pitcher_game["game_key"] = pitcher_game["game_key"].fillna(
    pd.merge(pitcher, weather[["home_team", "away_team", "game_key"]],
             how="left", left_on="team", right_on="home_team")["game_key"]
)
pitcher_game = pitcher_game.dropna(subset=["game_key"])
pitcher_game["z_score_era"] = -(pitcher_game["era"] - pitcher_game["era"].mean()) / pitcher_game["era"].std()

# Flatten
batter_long = batter_game[["name", "game_key", "z_score_hit", "z_score_hr"]].melt(id_vars=["name", "game_key"], var_name="stat", value_name="z")
pitcher_long = pitcher_game[["name", "game_key", "z_score_era"]].melt(id_vars=["name", "game_key"], var_name="stat", value_name="z")
all_props = pd.concat([batter_long, pitcher_long], ignore_index=True)

top_props = all_props.sort_values("z", ascending=False).groupby("game_key").head(5)
weather_data = weather[["game_key", "away_team", "home_team", "temperature"]]
props = pd.merge(top_props, weather_data, on="game_key", how="left")

result = []
for game_key, group in props.groupby("game_key"):
    row = group.iloc[0]
    result.append({
        "game": f"{row['away_team']} @ {row['home_team']}",
        "temperature": round(row["temperature"]),
        "top_props": [
            {
                "player": r["name"],
                "stat": r["stat"].replace("z_score_", ""),
                "z_score": round(r["z"], 2)
            } for _, r in group.iterrows()
        ]
    })

Path("public/game_cards_data.json").write_text(json.dumps(result, indent=2))
