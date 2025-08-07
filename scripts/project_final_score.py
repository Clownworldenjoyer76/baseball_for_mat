import pandas as pd
from pathlib import Path

# File paths
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
GAMES_TODAY_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    # Load all source files
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    games_today = pd.read_csv(GAMES_TODAY_FILE)

    # --- Data Preparation ---
    game_data = pd.merge(
        weather,
        games_today[["home_team", "away_team", "pitcher_home", "pitcher_away"]],
        on=["home_team", "away_team"],
        how="inner"
    )

    batter_scores = batters.groupby("team")["ultimate_z"].mean().to_dict()
    
    if 'name' not in pitchers.columns or 'mega_z' not in pitchers.columns:
        raise ValueError("PITCHER_FILE must contain 'name' and 'mega_z' columns.")
    pitcher_scores = pitchers.set_index("name")["mega_z"].to_dict()


    # ---vvv--- REPLACE THIS FUNCTION ---vvv---
    def project_score(batter_team, pitcher_name, weather_factor):
        # --- Start Debug Block ---
        # This section will print a message if a name or team is not found.
        batter_val = batter_scores.get(batter_team)
        pitcher_val = pitcher_scores.get(pitcher_name)
        if batter_val is None:
            print(f"DEBUG: Could not find team '{batter_team}' in batter scores.")
        if pitcher_val is None:
            print(f"DEBUG: Could not find pitcher '{pitcher_name}' in pitcher scores.")
        # --- End Debug Block ---

        batter = normalize(batter_scores.get(batter_team, 0))
        pitcher = normalize(pitcher_scores.get(pitcher_name, 0))
        return (batter + pitcher) * weather_factor
    # ---^^^--- REPLACE THIS FUNCTION ---^^^---


    def normalize(val):
        return max(val, 1.0)

    # --- Main Loop ---
    rows = []
    for _, row in game_data.iterrows():
        home_team = row["home_team"]
        away_team = row["away_team"]
        factor = row["weather_factor"]
        home_pitcher = row["pitcher_home"]
        away_pitcher = row["pitcher_away"]

        home_score = project_score(home_team, away_pitcher, factor)
        away_score = project_score(away_team, home_pitcher, factor)

        rows.append({
            "home_team": home_team,
            "away_team": away_team,
            "home_pitcher": home_pitcher,
            "away_pitcher": away_pitcher,
            "home_score": home_score,
            "away_score": away_score,
            "weather_factor": factor,
        })

    # --- Finalization and Output ---
    df = pd.DataFrame(rows)
    if not df.empty:
        current_avg = (df["home_score"] + df["away_score"]).mean()
        if current_avg > 0:
            scale = 9.0 / current_avg
            df["home_score"] = (df["home_score"] * scale).round(2)
            df["away_score"] = (df["away_score"] * scale).round(2)

    df.to_csv(OUTPUT_FILE, index=False)
    print("✅ Final score projections saved:", OUTPUT_FILE)

if __name__ == "__main__":
    main()

