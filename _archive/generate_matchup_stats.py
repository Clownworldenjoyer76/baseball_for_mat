
import pandas as pd
from pathlib import Path

def main():
    # Load inputs
    batters = pd.read_csv("data/adjusted/batters_deduped.csv")
    pitchers = pd.read_csv("data/cleaned/pitchers_normalized_cleaned.csv")
    matchups = pd.read_csv("data/daily/todays_pitchers.csv")

    # Standardize names for pitcher lookup
    matchups["home_pitcher"] = matchups["home_pitcher"].str.strip()
    matchups["away_pitcher"] = matchups["away_pitcher"].str.strip()

    # Assign opponent team and pitcher to each batter
    def get_matchup(row):
        team = row["team"]
        game = matchups[(matchups["home_team"] == team) | (matchups["away_team"] == team)]
        if game.empty:
            return pd.Series(["Unknown", "Unknown"])
        game = game.iloc[0]
        is_home = game["home_team"] == team
        opponent = game["away_team"] if is_home else game["home_team"]
        pitcher = game["away_pitcher"] if is_home else game["home_pitcher"]
        return pd.Series([opponent, pitcher])

    batters[["opponent_team", "opponent_pitcher"]] = batters.apply(get_matchup, axis=1)

    # Merge pitcher stats on "opponent_pitcher" matching "last_name, first_name"
    pitchers["last_name, first_name"] = pitchers["last_name, first_name"].str.strip()
    batters["opponent_pitcher"] = batters["opponent_pitcher"].str.strip()
    merged = pd.merge(batters, pitchers, how="left",
                      left_on="opponent_pitcher", right_on="last_name, first_name",
                      suffixes=('', '_opp_pitcher'))

    # Save final matchup file
    output_path = Path("data/matchups")
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / "matchup_stats.csv"
    merged.to_csv(output_file, index=False)

    print(f"✅ Matchup stats saved to {output_file}")
    print(f"🧾 Total rows: {len(merged)}")

if __name__ == "__main__":
    main()
