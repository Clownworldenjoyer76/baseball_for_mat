import pandas as pd
from pathlib import Path

# Input files
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")

def main():
    # Load data
    batters = pd.read_csv(BATTER_FILE)
    pitchers = pd.read_csv(PITCHER_FILE)
    weather = pd.read_csv(WEATHER_FILE)

    # Aggregate team scores
    batter_team_scores = batters.groupby("team")["ultimate_z"].sum().reset_index(name="batter_score")
    pitcher_team_scores = pitchers.groupby("team")["mega_z"].sum().reset_index(name="pitcher_score")

    # Merge into game-level
    merged = weather[["home_team", "away_team", "weather_factor"]].copy()

    def compute_team_score(team, batter_df, pitcher_df):
        b_score = batter_df.get(team, 0)
        p_score = pitcher_df.get(team, 0)
        return (b_score + p_score) / 2

    # Create dicts for lookup
    batter_dict = dict(zip(batter_team_scores["team"], batter_team_scores["batter_score"]))
    pitcher_dict = dict(zip(pitcher_team_scores["team"], pitcher_team_scores["pitcher_score"]))

    # Compute projected scores
    merged["home_score"] = merged.apply(
        lambda row: round(compute_team_score(row["home_team"], batter_dict, pitcher_dict) * row["weather_factor"], 2),
        axis=1
    )
    merged["away_score"] = merged.apply(
        lambda row: round(compute_team_score(row["away_team"], batter_dict, pitcher_dict) * row["weather_factor"], 2),
        axis=1
    )

    # Final columns
    final = merged[["home_team", "away_team", "home_score", "away_score", "weather_factor"]]
    final.to_csv(OUTPUT_FILE, index=False)
    print("✅ Final score projections saved:", OUTPUT_FILE)

if __name__ == "__main__":
    main()
