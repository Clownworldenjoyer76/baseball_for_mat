import pandas as pd
from pathlib import Path

# Input files
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    weather = pd.read_csv(WEATHER_FILE)

    # Sum batter and pitcher scores by team
    batter_scores = batters.groupby("team")["ultimate_z"].sum().to_dict()
    pitcher_scores = pitchers.groupby("team")["mega_z"].sum().to_dict()

    # Compute final score: batter score minus opposing pitcher score
    def project_score(batter_team, pitcher_team, weather_factor):
        batter_score = batter_scores.get(batter_team, 0)
        pitcher_score = pitcher_scores.get(pitcher_team, 0)
        raw = batter_score - pitcher_score
        return round(max(raw * weather_factor, 0), 2)

    # Generate per-game score projections
    rows = []
    for _, row in weather.iterrows():
        home = row["home_team"]
        away = row["away_team"]
        factor = row["weather_factor"]

        home_score = project_score(home, away, factor)
        away_score = project_score(away, home, factor)

        rows.append({
            "home_team": home,
            "away_team": away,
            "home_score": home_score,
            "away_score": away_score,
            "weather_factor": factor,
        })

    # Output CSV
    pd.DataFrame(rows).to_csv(OUTPUT_FILE, index=False)
    print("✅ Final score projections saved:", OUTPUT_FILE)

if __name__ == "__main__":
    main()
