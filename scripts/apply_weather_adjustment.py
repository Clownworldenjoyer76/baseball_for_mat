import pandas as pd

BATTERS_HOME = "data/adjusted/batters_home.csv"
BATTERS_AWAY = "data/adjusted/batters_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv"

OUTPUT_HOME = "data/adjusted/batters_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/batters_away_weather.csv"
LOG_HOME = "log_weather_home.txt"
LOG_AWAY = "log_weather_away.txt"

def apply_adjustment(df, side, weather_df):
    if "team" not in df.columns:
        raise ValueError(f"Missing 'team' column in batters_{side}.csv")

    # Determine the correct merge key based on 'side' (home/away)
    if side == "home":
        # For home batters, match their 'team' to the 'home_team' in weather_df
        merge_key_right = "home_team"
    elif side == "away":
        # For away batters, match their 'team' to the 'away_team' in weather_df
        merge_key_right = "away_team"
    else:
        raise ValueError("Side must be 'home' or 'away'")

    # Perform the merge.
    # We need to consider that multiple games might exist for a team on a given day,
    # or that the weather_df might contain game-specific venue data.
    # If weather_df is one row per game with 'home_team' and 'away_team', this works.
    merged = df.merge(
        weather_df,
        left_on="team",
        right_on=merge_key_right,
        how="left",
        suffixes=('_batter', '_weather') # Add suffixes to avoid conflicts if column names overlap
    )

    # Check if weather columns were actually merged (i.e., not all NaN)
    # The original check for 'temperature' in merged.columns will always pass if it was in weather_df,
    # even if all values are NaN. We need to check if actual data was merged.
    if merged["temperature"].isnull().all():
        print(f"⚠️ Warning: No weather data merged for batters_{side}. "
              f"Check if '{side}' team names in input match '{merge_key_right}' in weather_adjustments.csv.")
        # If no merge happened, you might want to exit or handle this specifically.
        # For now, we'll continue, but adj_woba_weather won't change.

    # Adjust wOBA based on temperature
    # Ensure 'adj_woba_weather' is initialized even if no temperature data is present
    if "woba" not in merged.columns:
         raise ValueError(f"Missing 'woba' column in batters_{side}.csv for adjustment")

    merged["adj_woba_weather"] = merged["woba"]
    # Only apply adjustment if temperature data is available and not NaN
    merged.loc[merged["temperature"].notna() & (merged["temperature"] >= 85), "adj_woba_weather"] *= 1.03
    merged.loc[merged["temperature"].notna() & (merged["temperature"] <= 50), "adj_woba_weather"] *= 0.97


    # Clean up duplicate columns from the merge if suffixes created them and they're not needed
    # e.g., if 'team_weather' or 'venue_weather' columns were added and are redundant.
    # You'll need to inspect your merged DataFrame columns to know which ones to drop.
    # For example, if 'home_team_weather' and 'away_team_weather' are redundant with 'home_team'/'away_team'
    # coming from the weather_df, you might want to keep only the weather_df versions.
    # This will depend on what columns you want in your final output.

    # Example: If your weather_df has 'venue' and 'location', and you want to keep those from weather_df:
    # After the merge, you will have the original columns from 'df' (e.g. 'team', 'last_name, first_name', 'woba')
    # PLUS all columns from 'weather_df' (e.g. 'venue', 'location', 'temperature', 'wind_speed', etc.)
    # The columns that caused the conflict (if any) will have _batter and _weather suffixes.
    # You might want to drop 'team_weather' if that was created.

    return merged.drop(columns=[col for col in merged.columns if '_batter' in str(col)], errors='ignore')


def write_log(df, path):
    # Ensure 'last_name, first_name' column exists before sorting
    if 'last_name, first_name' not in df.columns:
        print(f"Error: 'last_name, first_name' column not found for log writing in {path}.")
        # Fallback to another identifier or skip log writing
        return

    top5 = df.sort_values("adj_woba_weather", ascending=False).head(5)
    with open(path, "w") as f:
        for _, row in top5.iterrows():
            f.write(f"{row['last_name, first_name']} - {row['team']} - {row['adj_woba_weather']:.3f}\n")

def main():
    print("Loading input files...")
    try:
        batters_home = pd.read_csv(BATTERS_HOME)
        batters_away = pd.read_csv(BATTERS_AWAY)
        weather = pd.read_csv(WEATHER_FILE)
        print("Files loaded successfully.")
    except FileNotFoundError as e:
        print(f"Error loading file: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during file loading: {e}")
        return

    print("Applying weather adjustments for home batters...")
    adjusted_home = apply_adjustment(batters_home, "home", weather)
    print("Applying weather adjustments for away batters...")
    adjusted_away = apply_adjustment(batters_away, "away", weather)

    print("Saving adjusted data...")
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)
    print(f"Adjusted data saved to {OUTPUT_HOME} and {OUTPUT_AWAY}")

    print("Writing adjustment logs...")
    write_log(adjusted_home, LOG_HOME)
    write_log(adjusted_away, LOG_AWAY)
    print(f"Logs written to {LOG_HOME} and {LOG_AWAY}")

if __name__ == "__main__":
    main()

