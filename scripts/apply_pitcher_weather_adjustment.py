import pandas as pd
import subprocess
import os # Import os for path checking

# File paths
PITCHERS_HOME_FILE = "data/adjusted/pitchers_home.csv"
PITCHERS_AWAY_FILE = "data/adjusted/pitchers_away.csv"
WEATHER_FILE = "data/weather_adjustments.csv" # This is likely weather_input.csv if that's what generate_weather_csv creates
OUTPUT_HOME = "data/adjusted/pitchers_home_weather.csv"
OUTPUT_AWAY = "data/adjusted/pitchers_away_weather.csv"
LOG_HOME = "log_pitchers_home_weather.txt"
LOG_AWAY = "log_pitchers_away_weather.txt"

def adjust_temperature(temp):
    if pd.isna(temp):
        return 1.0
    if temp > 85:
        return 1.02
    elif temp < 60:
        return 0.98
    return 1.0

def apply_adjustment(df, team_col, weather_df, side):
    # Ensure team_col is treated consistently (e.g., upper case for merging)
    df[team_col] = df[team_col].str.strip().str.upper()
    weather_df[team_col] = weather_df[team_col].str.strip().str.upper() # Assuming weather_df has a similar team_col

    merged = df.merge(weather_df, left_on=team_col, right_on=team_col, how='left')

    if 'temperature' not in merged.columns:
        print(f"WARNING: 'temperature' column not found in merged dataframe for {side} team. Setting adj_woba_weather to None.")
        merged['adj_woba_weather'] = None
        merged['temperature'] = None
    else:
        merged['adj_woba_weather'] = merged['woba'] * merged['temperature'].apply(adjust_temperature)
    return merged

def log_top5(df, log_path, side):
    print(f"DEBUG: Generating log for top 5 {side.capitalize()} Pitchers to {log_path}")
    # Ensure output directory exists for logs if they are not in root
    Path(log_path).parent.mkdir(parents=True, exist_ok=True) 
    with open(log_path, "w") as f:
        f.write(f"Top 5 {side.capitalize()} Pitchers by adj_woba_weather:\n")
        if "adj_woba_weather" in df.columns:
            top5 = df.sort_values('adj_woba_weather', ascending=False).head(5)
            # Ensure all columns exist before trying to select them
            cols_to_log = ["name", "team", "woba", "temperature", "adj_woba_weather"]
            existing_cols = [col for col in cols_to_log if col in top5.columns]
            f.write(top5[existing_cols].to_string(index=False))
        else:
            f.write("No adjusted wOBA data available.")
    print(f"DEBUG: Log for {side.capitalize()} Pitchers written successfully.")


def git_commit_and_push(): # No longer takes 'files' as argument as it uses 'git add .'
    try:
        print("DEBUG: Starting Git operations for apply_pitcher_weather_adjustment.")
        # Add all newly generated or modified files in the current directory
        print(f"DEBUG: Attempting to git add all modified files (git add .).")
        subprocess.run(["git", "add", "."], check=True, capture_output=True, text=True)
        print("DEBUG: Git add . successful. All changes staged.")

        # Check if there are any staged changes to commit
        status_output = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        if not status_output.strip():
            print("DEBUG: No changes to commit. Workflow files are up to date.")
        else:
            print("DEBUG: Changes detected, attempting to commit.")
            subprocess.run(["git", "commit", "-m", "📝 Apply pitcher weather adjustment and update data files"], check=True, capture_output=True, text=True)
            print("DEBUG: Git commit successful.")
            print("DEBUG: Attempting to git push.")
            subprocess.run(["git", "push"], check=True, capture_output=True, text=True)
            print("✅ Git commit and push complete for pitcher weather adjustments.")

    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed for pitcher weather adjustments:")
        print(f"  Command: {e.cmd}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT: {e.stdout}")
        print(f"  STDERR: {e.stderr}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Git operations for pitcher weather adjustments: {e}")

def main():
    print("DEBUG: Loading input files for weather adjustment.")
    try:
        home_df = pd.read_csv(PITCHERS_HOME_FILE)
        away_df = pd.read_csv(PITCHERS_AWAY_FILE)
        weather_df = pd.read_csv(WEATHER_FILE)
        print(f"DEBUG: Loaded {len(home_df)} home pitchers, {len(away_df)} away pitchers, and {len(weather_df)} weather adjustments.")
    except FileNotFoundError as e:
        print(f"❌ File not found during initial loading: {e}")
        return
    except Exception as e:
        print(f"❌ Error loading input files: {e}")
        return

    print("DEBUG: Applying weather adjustments to home pitchers.")
    adjusted_home = apply_adjustment(home_df, "home_team", weather_df, "home")
    print("DEBUG: Applying weather adjustments to away pitchers.")
    adjusted_away = apply_adjustment(away_df, "away_team", weather_df, "away")

    print(f"DEBUG: Writing adjusted home pitchers to {OUTPUT_HOME}")
    # Ensure output directory exists
    Path(OUTPUT_HOME).parent.mkdir(parents=True, exist_ok=True)
    adjusted_home.to_csv(OUTPUT_HOME, index=False)
    print(f"DEBUG: Writing adjusted away pitchers to {OUTPUT_AWAY}")
    Path(OUTPUT_AWAY).parent.mkdir(parents=True, exist_ok=True)
    adjusted_away.to_csv(OUTPUT_AWAY, index=False)
    print("DEBUG: Adjusted pitcher files written.")

    log_top5(adjusted_home, LOG_HOME, "home")
    log_top5(adjusted_away, LOG_AWAY, "away")

    # Call the modified git_commit_and_push without arguments
    git_commit_and_push()

if __name__ == "__main__":
    main()
