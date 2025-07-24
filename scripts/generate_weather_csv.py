import pandas as pd
from pathlib import Path
import subprocess
import os # Import os module

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

    games_df['home_team'] = games_df['home_team'].str.strip().str.upper()
    games_df['away_team'] = games_df['away_team'].str.strip().str.upper()
    stadium_df['home_team'] = stadium_df['home_team'].str.strip().str.upper()
    team_map_df['uppercase'] = team_map_df['team_name'].str.strip().str.upper()
    team_map_df = team_map_df.drop_duplicates(subset='uppercase')

    games_df = games_df.drop(columns=['game_time'], errors='ignore')

    merged = pd.merge(games_df, stadium_df, on='home_team', how='left')
    if merged.empty:
        print("❌ Merge failed: No matching rows after games + stadium merge.")
        return

    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']],
        left_on='home_team',
        right_on='uppercase',
        how='left'
    )
    if 'home_team' in merged.columns and 'uppercase' in merged.columns:
        merged.drop(columns=['home_team', 'uppercase'], inplace=True)
    merged.rename(columns={'team_name': 'home_team'}, inplace=True)

    if 'away_team_x' in merged.columns and 'away_team_y' in merged.columns:
        merged['away_team'] = merged['away_team_x'].combine_first(merged['away_team_y'])
        merged.drop(columns=['away_team_x', 'away_team_y'], inplace=True)
    elif 'away_team' not in merged.columns:
        print("❌ 'away_team' column not found in merged dataframe.")
        return

    merged['away_team'] = merged['away_team'].str.strip().str.upper()
    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']],
        left_on='away_team',
        right_on='uppercase',
        how='left'
    )
    if 'away_team' in merged.columns and 'uppercase' in merged.columns:
        merged.drop(columns=['away_team', 'uppercase'], inplace=True)
    merged.rename(columns={'team_name': 'away_team'}, inplace=True)

    # Ensure output directory exists
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
        # Create a new environment dictionary based on the current environment
        # and explicitly set Git author information for the subprocess calls.
        git_env = os.environ.copy()
        git_env["GIT_AUTHOR_NAME"] = "github-actions"
        git_env["GIT_AUTHOR_EMAIL"] = "github-actions@github.com"

        # Pass the custom environment to each subprocess.run call
        subprocess.run(["git", "add", "."], check=True, capture_output=True, text=True, env=git_env)
        
        status_output = subprocess.run(["git", "status", "--porcelain"], check=True, capture_output=True, text=True, env=git_env).stdout
        if not status_output.strip():
            print("✅ No changes to commit.")
        else:
            subprocess.run(["git", "commit", "-m", "🔁 Update data files and weather input/summary"], check=True, capture_output=True, text=True, env=git_env)
            subprocess.run(["git", "push"], check=True, capture_output=True, text=True, env=git_env) # Also pass env for push
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

