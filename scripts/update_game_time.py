#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

TODAY = Path("data/raw/todaysgames_normalized.csv")
STADIUM = Path("data/Data/stadium_metadata.csv")
DAY = Path("data/manual/park_factors_day.csv")
NIGHT = Path("data/manual/park_factors_night.csv")
ROOF = Path("data/manual/park_factors_roof_closed.csv")

def main():
    if not TODAY.exists():
        print("⚠️ todaysgames_normalized.csv missing")
        return

    games = pd.read_csv(TODAY)
    stadium = pd.read_csv(STADIUM)
    day = pd.read_csv(DAY)
    night = pd.read_csv(NIGHT)
    roof = pd.read_csv(ROOF)

    # Merge stadium metadata
    merged = games.merge(stadium, how="left", left_on="home_team", right_on="team")

    # Assign park factor depending on time of day and roof
    merged["Park Factor"] = None
    for idx, row in merged.iterrows():
        team_id = row.get("team_id")
        time_str = str(row.get("game_time", "")).lower()
        closed_roof = str(row.get("roof", "")).lower() == "closed"

        if closed_roof:
            match = roof[roof["team_id"] == team_id]
        elif "pm" in time_str and not time_str.startswith("12"):
            match = night[night["team_id"] == team_id]
        else:
            match = day[day["team_id"] == team_id]

        if not match.empty:
            merged.at[idx, "Park Factor"] = match["Park Factor"].values[0]

    merged.to_csv(STADIUM, index=False)
    print(f"✅ update_game_time wrote {len(merged)} rows -> {STADIUM}")

if __name__ == "__main__":
    main()
