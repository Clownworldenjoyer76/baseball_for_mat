import pandas as pd
from pathlib import Path
import subprocess
import os

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

    # Clean and uppercase relevant team name columns for consistent merging
    games_df['home_team_raw'] = games_df['home_team'].str.strip().str.upper() # Keep original for merge
    games_df['away_team_raw'] = games_df['away_team'].str.strip().str.upper() # Keep original for merge
    stadium_df['home_team_stadium'] = stadium_df['home_team'].str.strip().str.upper() # Use a distinct name for stadium team
    team_map_df['uppercase'] = team_map_df['team_name'].str.strip().str.upper()
    team_map_df = team_map_df.drop_duplicates(subset='uppercase')

    # Drop game_time if it's not needed in the final output and can cause issues with other data sources
    # Re-add it later if it's explicitly needed from games_df in the final output
    games_df = games_df.drop(columns=['game_time'], errors='ignore')

    # --- Merge with Stadium Metadata ---
    # Merge on the cleaned 'home_team_raw' from games_df and 'home_team_stadium' from stadium_df
    # We will get 'home_team_raw' from games_df, and stadium columns.
    # 'home_team_stadium' won't be in the result as it's the merge key.
    merged = pd.merge(
        games_df,
        stadium_df.drop(columns=['home_team']), # Drop original 'home_team' from stadium_df as we used 'home_team_stadium' for merge
        left_on='home_team_raw',
        right_on='home_team_stadium', # Use the cleaned stadium team name for merging
        how='left'
    )
    if merged.empty:
        print("❌ Merge failed: No matching rows after games + stadium merge.")
        return

    # Drop the temporary stadium_team merge key
    merged.drop(columns=['home_team_stadium'], errors='ignore', inplace=True)

    # --- Map Home Team Names ---
    # Merge to get the canonical 'team_name' for home team
    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']], # 'team_name' from team_map_df will be the mapped name
        left_on='home_team_raw', # Merge using the raw uppercased name
        right_on='uppercase',
        how='left',
        suffixes=('_original', '_mapped') # Add suffixes if columns share names, but here we explicitly use 'team_name'
    )

    # Rename the mapped 'team_name' to 'home_team'
    # The 'team_name' from team_map_df becomes 'team_name_mapped' if suffixes are applied,
    # or just 'team_name' if no conflict. Assuming it's just 'team_name' from the mapping.
    # Based on your previous code, team_name is just 'team_name' so this works:
    merged.rename(columns={'team_name': 'home_team'}, inplace=True)
    merged.drop(columns=['home_team_raw', 'uppercase'], inplace=True) # Drop the temporary raw home team and uppercase key

    # --- Map Away Team Names ---
    # Merge to get the canonical 'team_name' for away team
    merged = pd.merge(
        merged,
        team_map_df[['uppercase', 'team_name']], # 'team_name' from team_map_df will be the mapped name
        left_on='away_team_raw', # Merge using the raw uppercased name
        right_on='uppercase',
        how='left',
        suffixes=('_original', '_mapped') # Suffixes for any conflicting columns if they arise
    )

    # Rename the mapped 'team_name' to 'away_team'
    # The 'team_name' from team_map_df becomes 'team_name_mapped' if suffixes are applied.
    # If it still ends up as 'team_name_y', you would rename 'team_name_y' here.
    # However, by being explicit with suffixes, we expect 'team_name_mapped' or just 'team_name'
    merged.rename(columns={'team_name': 'away_team'}, inplace=True) # Assuming mapped team_name is 'team_name'
    # If the above line still results in team_name_y, then use:
    # merged.rename(columns={'team_name_mapped': 'away_team'}, inplace=True) # Or 'team_name_y' if that's what's produced
    merged.drop(columns=['away_team_raw', 'uppercase'], inplace=True) # Drop the temporary raw away team and uppercase key


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
