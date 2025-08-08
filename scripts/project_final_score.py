import pandas as pd
from pathlib import Path

# File paths
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
GAMES_TODAY_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    weather = pd.read_csv(WEATHER_FILE)
    games_today = pd.read_csv(GAMES_TODAY_FILE)

    game_data = pd.merge(
        weather,
        games_today[["home_team", "away_team", "pitcher_home", "pitcher_away", "game_time"]],
        on=["home_team", "away_team"],
        how="inner"
    )

    # Extract the date from the 'game_time' column
    game_data['date'] = game_data['game_time'].str.split(' ').str[0]

    batter_scores = batters.groupby("team")["ultimate_z"].mean().to_dict()
    pitcher_scores = pitchers.set_index("name")["mega_z"].to_dict()

    def project_score(batter_team, pitcher_name, weather_factor):
        batter_z = batter_scores.get(batter_team, 0)
        pitcher_z = pitcher_scores.get(pitcher_name, 0)

        # A good pitcher REDUCES the score, so we SUBTRACT their Z-score.
        combined_z = batter_z - pitcher_z

        # Shift the score to be positive. Using 5 as a baseline for Z-scores.
        adjusted_score = max(1.0, combined_z + 5)

        return adjusted_score * weather_factor

    rows = []
    for _, row in game_data.iterrows():
        home_team = row["home_team"]
        away_team = row["away_team"]
        factor = row["weather_factor"]
        home_pitcher = row["pitcher_home"]
        away_pitcher = row["pitcher_away"]
        date = row["date"]

        # Home team's score depends on their batters vs. the AWAY pitcher
        home_score = project_score(home_team, away_pitcher, factor)
        
        # Away team's score depends on their batters vs. the HOME pitcher
        away_score = project_score(away_team, home_pitcher, factor)

        rows.append({
            "date": date,
            "home_team": home_team,
            "away_team": away_team,
            "home_pitcher": home_pitcher,
            "away_pitcher": away_pitcher,
            "home_score": home_score,
            "away_score": away_score,
            "weather_factor": factor,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        current_avg = (df["home_score"] + df["away_score"]).mean()
        if current_avg > 0:
            scale = 9.0 / current_avg
            df["home_score"] = (df["home_score"] * scale).round(2)
            df["away_score"] = (df["away_score"] * scale).round(2)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Final score projections saved: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
