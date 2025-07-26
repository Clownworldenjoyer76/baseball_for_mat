import pandas as pd
from pathlib import Path
import subprocess
import os

# --- File Paths ---
GAMES_FILE = "data/raw/todaysgames_normalized.csv"
STADIUM_FILE = "data/Data/stadium_metadata.csv"
TEAM_MAP_FILE = "data/Data/team_name_master.csv"
OUTPUT_FILE = "data/weather_input.csv"
SUMMARY_FILE = "data/weather_summary.txt"

def generate_weather_csv():
    try:
        games_df = pd.read_csv(GAMES_FILE)
        stadium_df = pd.read_csv(STADIUM_FILE)
        team_map_df = pd.read_csv(TEAM_MAP_FILE)
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return
    except Exception as e:
        print(f"❌ Error reading input files: {e}")
        return

    # Normalize team names
    games_df["home_team_raw"] = games_df["home_team"].str.strip().str.upper()
    games_df["away_team_raw"] = games_df["away_team"].str.strip().str.upper()
    stadium_df["home_team_stadium"] = stadium_df["home_team"].str.strip().str.upper()
    team_map_df["uppercase"] = team_map_df["team_name"].str.strip().str.upper()
    team_map_df = team_map_df.drop_duplicates(subset="uppercase")

    # Drop 'game_time' if present (we'll keep the one from stadium metadata)
    games_df.drop(columns=["game_time"], errors="ignore", inplace=True)

    # --- Merge with Stadium Info ---
    merged = pd.merge(
        games_df,
        stadium_df.drop(columns=["home_team"]),
        left_on="home_team_raw",
        right_on="home_team_stadium",
        how="left"
    )

    if merged.empty:
        print("❌ Merge failed: No matching rows after games + stadium merge.")
        return

    merged.drop(columns=["home_team_stadium"], inplace=True)

    # --- Map home team canonical name ---
    merged = pd.merge(
        merged,
        team_map_df[["uppercase", "team_name"]],
        left_on="home_team_raw",
        right_on="uppercase",
        how="left"
    ).rename(columns={"team_name": "home_team"})

    merged.drop(columns=["home_team_raw", "uppercase"], inplace=True)

    # --- Map away team canonical name ---
    merged = pd.merge(
        merged,
        team_map_df[["uppercase", "team_name"]],
        left_on="away_team_raw",
        right_on="uppercase",
        how="left"
    ).rename(columns={"team_name": "away_team"})

    merged.drop(columns=["away_team_raw", "uppercase"], inplace=True)

    # --- Drop merge artifacts if any exist ---
    for col in ["away_team_x", "away_team_y", "team_name_original", "team_name_mapped"]:
        if col in merged.columns:
            merged.drop(columns=[col], inplace=True)

    # --- Reorder columns for clarity ---
    preferred_order = [
        "home_team", "away_team", "pitcher_home", "pitcher_away",
        "venue", "city", "state", "timezone", "is_dome", "latitude", "longitude",
        "game_time", "time_of_day", "Park Factor"
    ]
    merged = merged[[col for col in preferred_order if col in merged.columns]]

    # --- Validate ---
    if merged.isnull().any().any():
        print("⚠️ Warning: Some values are missing after merge.")
        print(merged[merged.isnull().any(axis=1)])

    if len(merged) != len(games_df):
        print(f"⚠️ Row mismatch: expected {len(games_df)}, got {len(merged)}")
        missing_teams = set(games_df["home_team"].str.upper()) - set(stadium_df["home_team"].str.upper())
        if missing_teams:
            print("🔍 Possible unmatched teams in stadium merge:", sorted(missing_teams))

    # --- Output to CSV ---
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_FILE, index=False)

    summary = (
        f"✅ Weather input file generated\n"
        f"🔢 Rows: {len(merged)}\n"
        f"📁 Output: {OUTPUT_FILE}\n"
        f"📄 Games file: {GAMES_FILE}\n"
        f"🏟️ Stadium file: {STADIUM_FILE}"
    )
    print(summary)
    Path(SUMMARY_FILE).write_text(summary)

    # --- Git Operations ---
    try:
        git_env = os.environ.copy()
        git_env["GIT_AUTHOR_NAME"] = "github-actions"
        git_env["GIT_AUTHOR_EMAIL"] = "github-actions@github.com"

        subprocess.run(["git", "add", "."], check=True, capture_output=True, text=True, env=git_env)

        status_output = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True, env=git_env).stdout
        if not status_output.strip():
            print("✅ No changes to commit.")
        else:
            subprocess.run(["git", "commit", "-m", "🔁 Update data files and weather input/summary"], check=True, capture_output=True, text=True, env=git_env)
            subprocess.run(["git", "push"], check=True, capture_output=True, text=True, env=git_env)
            print("✅ Git commit and push complete.")

    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push failed:")
        print(f"  Command: {e.cmd}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT: {e.stdout}")
        print(f"  STDERR: {e.stderr}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during Git operations: {e}")

if __name__ == "__main__":
    generate_weather_csv()
