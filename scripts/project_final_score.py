import pandas as pd
from pathlib import Path

# --- File paths ---
BATTER_FILE = Path("data/_projections/batter_props_z_expanded.csv")
PITCHER_FILE = Path("data/_projections/pitcher_mega_z.csv")
WEATHER_FILE = Path("data/weather_adjustments.csv")
GAMES_TODAY_FILE = Path("data/end_chain/cleaned/games_today_cleaned.csv")
OUTPUT_FILE = Path("data/_projections/final_scores_projected.csv")
# --- NEW: Debug file path ---
DEBUG_FILE = Path("data/_projections/debug_output.txt")

def main():
    # --- NEW: List to hold all our debug messages ---
    debug_log = []

    # Load all source files
    try:
        batters = pd.read_csv(BATTER_FILE)
        pitchers = pd.read_csv(PITCHER_FILE)
        weather = pd.read_csv(WEATHER_FILE)
        games_today = pd.read_csv(GAMES_TODAY_FILE)
        debug_log.append("✅ Successfully loaded all source files.")
    except FileNotFoundError as e:
        debug_log.append(f"❌ ERROR: Could not load file - {e}. Halting script.")
        with open(DEBUG_FILE, "w") as f:
            f.write("\n".join(debug_log))
        print(f"Debug info saved to {DEBUG_FILE}")
        return # Stop execution if a file is missing

    # --- Data Preparation ---
    game_data = pd.merge(
        weather,
        games_today[["home_team", "away_team", "pitcher_home", "pitcher_away"]],
        on=["home_team", "away_team"],
        how="inner"
    )

    batter_scores = batters.groupby("team")["ultimate_z"].mean().to_dict()
    pitcher_scores = pitchers.set_index("name")["mega_z"].to_dict()
    
    debug_log.append(f"Found {len(batter_scores)} teams in batter file.")
    debug_log.append(f"Found {len(pitcher_scores)} pitchers in pitcher file.")
    debug_log.append("-" * 20)


    def normalize(val):
        return max(val, 1.0)

    def project_score(batter_team, pitcher_name, weather_factor):
        batter_val = batter_scores.get(batter_team)
        pitcher_val = pitcher_scores.get(pitcher_name)

        # --- NEW: Add lookup results to the debug log ---
        if batter_val is None:
            debug_log.append(f"Batter lookup FAILED for team: '{batter_team}'")
        if pitcher_val is None:
            debug_log.append(f"Pitcher lookup FAILED for name: '{pitcher_name}'")

        batter = normalize(batter_val or 0)
        pitcher = normalize(pitcher_val or 0)
        return (batter + pitcher) * weather_factor

    # --- Main Loop ---
    rows = []
    if game_data.empty:
        debug_log.append("❌ ERROR: No matching games found between weather and games_today files.")
    else:
        debug_log.append("Processing games...")

    for _, row in game_data.iterrows():
        home_team = row["home_team"]
        away_team = row["away_team"]
        factor = row["weather_factor"]
        home_pitcher = row["pitcher_home"]
        away_pitcher = row["pitcher_away"]

        home_score = project_score(home_team, away_pitcher, factor)
        away_score = project_score(away_team, home_pitcher, factor)

        rows.append({
            "home_team": home_team, "away_team": away_team,
            "home_pitcher": home_pitcher, "away_pitcher": away_pitcher,
            "home_score": home_score, "away_score": away_score,
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
    debug_log.append(f"\n✅ Final score projections saved: {OUTPUT_FILE}")

    # --- NEW: Write all debug messages to the debug file ---
    with open(DEBUG_FILE, "w") as f:
        f.write("\n".join(debug_log))
    print(f"Debug info saved to {DEBUG_FILE}")


if __name__ == "__main__":
    main()
