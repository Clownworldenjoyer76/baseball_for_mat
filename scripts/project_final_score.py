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
    # Merge weather data with today's game data to get pitchers and weather factor in one place
    game_data = pd.merge(
        weather,
        games_today[["home_team", "away_team", "pitcher_home", "pitcher_away"]],
        on=["home_team", "away_team"],
        how="inner"
    )

    # --- FIX APPLIED ---
    # Create a dictionary for batter scores using the MEAN (average) of the team's players.
    # This keeps the batter score on the same scale as the pitcher score.
    batter_scores = batters.groupby("team")["ultimate_z"].mean().to_dict()

    # Create a dictionary mapping each pitcher's name to their individual score
    if 'name' not in pitchers.columns or 'mega_z' not in pitchers.columns:
        raise ValueError("PITCHER_FILE must contain 'name' and 'mega_z' columns.")
    pitcher_scores = pitchers.set_index("name")["mega_z"].to_dict()

    # --- Projection Logic ---
    def normalize(val):
        # Floor all values to 1.0 to ensure minimum contribution
        return max(val, 1.0)

    def project_score(batter_team, pitcher_name, weather_factor):
        # Get the team's average batter score
        batter = normalize(batter_scores.get(batter_team, 0))
        # Look up the specific pitcher's score by their name
        pitcher = normalize(pitcher_scores.get(pitcher_name, 0))
        return (batter + pitcher) * weather_factor

    # --- Main Loop ---
    rows = []
    # Loop over the merged dataframe which contains all necessary game info
    for _, row in game_data.iterrows():
        home_team = row["home_team"]
        away_team = row["away_team"]
        factor = row["weather_factor"]
        home_pitcher = row["pitcher_home"]
        away_pitcher = row["pitcher_away"]

        # Home team's score depends on their batters vs. the away team's starting pitcher
        home_score = project_score(home_team, away_pitcher, factor)
        # Away team's score depends on their batters vs. the home team's starting pitcher
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
    # Normalize all scores to average 9 total runs per game
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
